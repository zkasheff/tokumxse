// kv_record_store.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <algorithm>
#include <climits>
#include <boost/static_assert.hpp>

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary_update.h"
#include "mongo/db/storage/kv/dictionary/kv_record_store.h"
#include "mongo/db/storage/kv/dictionary/kv_size_storer.h"
#include "mongo/db/storage/kv/slice.h"

#include "mongo/platform/endian.h"
#include "mongo/util/log.h"

namespace mongo {

    /**
     * Class to abstract the in-memory vs on-disk format of a key in the
     * record dictionary.
     *
     * In memory, a key is a valid RecordId for which loc.isValid() and
     * !loc.isNull() are true.
     *
     * On disk, a key is a pair of back-to-back integers whose individual
     * 4 bytes values are serialized in big-endian format. This allows the
     * entire structure to be compared with memcmp while still maintaining
     * the property that the filenum field compares more significantly
     * than the offset field. Being able to compare with memcmp is
     * important because for most storage engines, memcmp is the default
     * and fastest comparator.
     */
    class RecordIdKey {
        RecordId _id;
        uint64_t _key;

        // We assume that a RecordId can be represented in a 64 bit integer.
        BOOST_STATIC_ASSERT(sizeof(RecordId) == sizeof(uint64_t));

        // In order to use memcmp to compare keys the same way as they're compared by
        // RecordId::compare(), we need to move negative numbers up into the unsigned space before
        // converting to big-endian.  These convert back and forth.
        //
        // Note: this is intimately tied to the internal representation of RecordIds.
        static uint64_t toUnsigned(int64_t v) {
            return static_cast<uint64_t>(v - LLONG_MIN);
        }
        static int64_t toSigned(uint64_t v) {
            return static_cast<int64_t>(v) + LLONG_MIN;
        }

    public:
        /**
         * Used when we have a RecordId and we want a memcmp-ready Slice to
         * be used as a stored key in the KVDictionary.
         *
         * Algorithm:
         * - Take a RecordId with two integers laid out in native bye order
         *   [a, o]
         * - Construct a 64 bit integer whose high order bits are `a' and
         *   whose low order bits are `o'
         * - Convert that integer to big endian and store it in `_key'
         */
        RecordIdKey(const RecordId &id)
            : _id(id),
              _key(endian::nativeToBig(toUnsigned(id.repr())))
        {}

        /**
         * Used when we have a big-endian key Slice from the KVDictionary
         * and we want to get its RecordId representation.
         *
         * Algorithm (work backwards from the above constructor's
         * algorithm):
         * - Interpret the stored key as a 64 bit integer into `_key',
         *   then convert `_key' to native byte order and store it in `k'.
         * - Create a RecordId(a, o) where `a' is a 32 bit integer
         *   constructed from the high order bits of `_k' and where `o' is
         *   constructed from the low order bits.
         */
        RecordIdKey(const Slice &key)
            : _id(toSigned(endian::bigToNative(key.as<uint64_t>()))),
              _key(key.as<uint64_t>())
        {}

        /**
         * Return an un-owned slice of _key, suitable for use as a key
         * into the KVDictionary that maps record ids to record data.
         */
        Slice key() const {
            return Slice::of(_key);
        }

        /**
         * Return the RecordId representation of a deserialized key Slice
         */
        RecordId id() const {
            return _id;
        }
    };

    KVRecordStore::KVRecordStore( KVDictionary *db,
                                  OperationContext* opCtx,
                                  const StringData& ns,
                                  const StringData& ident,
                                  const CollectionOptions& options,
                                  KVSizeStorer *sizeStorer )
        : RecordStore(ns),
          _db(db),
          _ident(ident.toString()),
          _sizeStorer(sizeStorer)
    {
        invariant(_db != NULL);

        // Get the next id, which is one greater than the greatest stored.
        boost::scoped_ptr<RecordIterator> iter(getIterator(opCtx, RecordId(), CollectionScanParams::BACKWARD));
        if (!iter->isEOF()) {
            const RecordId lastId = iter->curr();
            invariant(lastId.isNormal());
            _nextIdNum.store(lastId.repr() + 1);
        } else {
            // Need to start at 1 so we are within bounds of RecordId::isNormal()
            _nextIdNum.store(1);
        }

        if (_sizeStorer) {
            long long numRecords;
            long long dataSize;
            _sizeStorer->load(_ident, &numRecords, &dataSize);
            _numRecords.store(numRecords);
            _dataSize.store(dataSize);
            _sizeStorer->onCreate(this, _ident, numRecords, dataSize);
        }
    }

    KVRecordStore::~KVRecordStore() {
        if (_sizeStorer) {
            _sizeStorer->onDestroy(_ident, _numRecords.load(), _dataSize.load());
        }
    }

#define invariantKVOK(s, expr) massert(28584, expr, s.isOK())

    long long KVRecordStore::dataSize( OperationContext* txn ) const {
        if (_sizeStorer) {
            return _dataSize.load();
        } else {
            return _db->getStats().dataSize;
        }
    }

    long long KVRecordStore::numRecords( OperationContext* txn ) const {
        if (_sizeStorer) {
            return _numRecords.load();
        } else {
            return _db->getStats().numKeys;
        }
    }

    int64_t KVRecordStore::storageSize( OperationContext* txn,
                                        BSONObjBuilder* extraInfo,
                                        int infoLevel ) const {
        return _db->getStats().storageSize;
    }

    class RollbackSizeChange : public RecoveryUnit::Change {
        KVRecordStore *_rs;
        long long _nrDelta;
        long long _dsDelta;
    public:
        RollbackSizeChange(KVRecordStore *rs, long long nrDelta, long long dsDelta)
            : _rs(rs),
              _nrDelta(nrDelta),
              _dsDelta(dsDelta)
        {}

        void commit() {}

        void rollback() {
            _rs->undoUpdateStats(_nrDelta, _dsDelta);
        }
    };

    void KVRecordStore::undoUpdateStats(long long nrDelta, long long dsDelta) {
        invariant(_sizeStorer);
        _numRecords.subtractAndFetch(nrDelta);
        _dataSize.subtractAndFetch(dsDelta);
    }

    void KVRecordStore::_updateStats(OperationContext *txn, long long nrDelta, long long dsDelta) {
        if (_sizeStorer) {
            _numRecords.addAndFetch(nrDelta);
            _dataSize.addAndFetch(dsDelta);
            txn->recoveryUnit()->registerChange(new RollbackSizeChange(this, nrDelta, dsDelta));
        }
    }

    RecordData KVRecordStore::_getDataFor(const KVDictionary *db, OperationContext* txn, const RecordId& loc) {
        const RecordIdKey key(loc);

        Slice value;
        Status status = db->get(txn, key.key(), value);
        if (!status.isOK()) {
            if (status.code() == ErrorCodes::NoSuchKey) {
                return RecordData(nullptr, 0);
            } else {
                log() << "storage engine get() failed, operation will fail: " << status.toString();
                uasserted(28549, status.toString());
            }
        }

        // Return an owned RecordData that uses the SharedBuffer from `value'
        return RecordData(value.ownedBuf().moveFrom(), value.size());
    }

    RecordData KVRecordStore::dataFor( OperationContext* txn, const RecordId& loc) const {
        RecordData rd;
        bool found = findRecord(txn, loc, &rd);
        // This method is called when we know there must be an associated record for `loc'
        invariant(found);
        return rd;
    }

    bool KVRecordStore::findRecord( OperationContext* txn,
                                    const RecordId& loc, RecordData* out ) const {
        RecordData rd = _getDataFor(_db.get(), txn, loc);
        if (rd.data() == NULL) {
            return false;
        }
        *out = rd;
        return true;
    }

    void KVRecordStore::deleteRecord( OperationContext* txn, const RecordId& id ) {
        const RecordIdKey key(id);

        Slice val;
        Status s = _db->get(txn, key.key(), val);
        invariantKVOK(s, str::stream() << "KVRecordStore: couldn't find record " << id << " for delete: " << s.toString());

        _updateStats(txn, -1, -val.size());

        s = _db->remove( txn, key.key() );
        invariant(s.isOK());
    }

    StatusWith<RecordId> KVRecordStore::insertRecord( OperationContext* txn,
                                                     const char* data,
                                                     int len,
                                                     bool enforceQuota ) {
        const RecordId loc = _nextId();
        const RecordIdKey key(loc);
        const Slice value(data, len);

        DEV {
            // Should never overwrite an existing record.
            Slice v;
            const Status status = _db->get(txn, key.key(), v);
            invariant(status.code() == ErrorCodes::NoSuchKey);
        }

        const Status status = _db->insert(txn, key.key(), value);
        if (!status.isOK()) {
            return StatusWith<RecordId>(status);
        }

        _updateStats(txn, 1, value.size());

        return StatusWith<RecordId>(loc);
    }

    StatusWith<RecordId> KVRecordStore::insertRecord( OperationContext* txn,
                                                     const DocWriter* doc,
                                                     bool enforceQuota ) {
        Slice value(doc->documentSize());
        doc->writeDocument(value.mutableData());
        return insertRecord(txn, value.data(), value.size(), enforceQuota);
    }

    StatusWith<RecordId> KVRecordStore::updateRecord( OperationContext* txn,
                                                     const RecordId& loc,
                                                     const char* data,
                                                     int len,
                                                     bool enforceQuota,
                                                     UpdateMoveNotifier* notifier ) {
        const RecordIdKey key(loc);
        const Slice value(data, len);

        int64_t numRecordsDelta = 0;
        int64_t dataSizeDelta = value.size();

        Slice val;
        Status status = _db->get(txn, key.key(), val);
        if (status.code() == ErrorCodes::NoSuchKey) {
            numRecordsDelta += 1;
        } else if (status.isOK()) {
            dataSizeDelta -= val.size();
        } else {
            return StatusWith<RecordId>(status);
        }

        // An update with a complete new image (data, len) is implemented as an overwrite insert.
        status = _db->insert(txn, key.key(), value);
        if (!status.isOK()) {
            return StatusWith<RecordId>(status);
        }

        _updateStats(txn, numRecordsDelta, dataSizeDelta);

        return StatusWith<RecordId>(loc);
    }

    Status KVRecordStore::updateWithDamages( OperationContext* txn,
                                             const RecordId& loc,
                                             const RecordData& oldRec,
                                             const char* damageSource,
                                             const mutablebson::DamageVector& damages ) {
        const RecordIdKey key(loc);

        const Slice oldValue(oldRec.data(), oldRec.size());
        const KVUpdateWithDamagesMessage message(damageSource, damages);

        // updateWithDamages can't change the number or size of records, so we don't need to update
        // stats.

        return _db->update(txn, key.key(), oldValue, message);
    }

    RecordIterator* KVRecordStore::getIterator( OperationContext* txn,
                                                const RecordId& start,
                                                const CollectionScanParams::Direction& dir
                                              ) const {
        return new KVRecordIterator(_db.get(), txn, start, dir);
    }

    std::vector<RecordIterator *> KVRecordStore::getManyIterators( OperationContext* txn ) const {
        std::vector<RecordIterator *> iterators;
        iterators.push_back(getIterator(txn));
        return iterators;
    }

    Status KVRecordStore::truncate( OperationContext* txn ) {
        // This is not a very performant implementation of truncate.
        //
        // At the time of this writing, it is only used by 'emptycapped', a test-only command.
        for (boost::scoped_ptr<RecordIterator> iter( getIterator( txn ) );
             !iter->isEOF(); ) {
            RecordId loc = iter->getNext();
            deleteRecord( txn, loc );
        }

        return Status::OK();
    }

    Status KVRecordStore::compact( OperationContext* txn,
                                   RecordStoreCompactAdaptor* adaptor,
                                   const CompactOptions* options,
                                   CompactStats* stats ) {
        return _db->compact( txn );
    }

    Status KVRecordStore::validate( OperationContext* txn,
                                    bool full,
                                    bool scanData,
                                    ValidateAdaptor* adaptor,
                                    ValidateResults* results,
                                    BSONObjBuilder* output ) const {
        bool invalidObject = false;
        size_t numRecords = 0;
        for (boost::scoped_ptr<RecordIterator> iter( getIterator( txn ) );
             !iter->isEOF(); ) {
            numRecords++;
            if (scanData) {
                RecordData data = dataFor( txn, iter->curr() );
                size_t dataSize;
                if (full) {
                    const Status status = adaptor->validate( data, &dataSize );
                    if (!status.isOK()) {
                        results->valid = false;
                        if ( invalidObject ) {
                            results->errors.push_back("invalid object detected (see logs)");
                        }
                        invalidObject = true;
                        log() << "Invalid object detected in " << _ns << ": " << status.reason();
                    }
                }
            }
            iter->getNext();
        }
        output->appendNumber("nrecords", numRecords);

        return Status::OK();
    }

    void KVRecordStore::appendCustomStats( OperationContext* txn,
                                           BSONObjBuilder* result,
                                           double scale ) const {
        _db->appendCustomStats(txn, result, scale);
    }

    Status KVRecordStore::touch( OperationContext* txn, BSONObjBuilder* output ) const {
        Timer t;
        for (boost::scoped_ptr<RecordIterator> iter( getIterator( txn ) );
             !iter->isEOF(); iter->getNext()) {
            // no-op, data is brought into memory just by iterating over it
        }

        if (output) {
            output->append("numRanges", 1);
            output->append("millis", t.millis());
        }
        return Status::OK();
    }

    Status KVRecordStore::setCustomOption( OperationContext* txn,
                                           const BSONElement& option,
                                           BSONObjBuilder* info ) {
        return _db->setCustomOption( txn, option, info );
    }

    RecordId KVRecordStore::_nextId() {
        return RecordId(_nextIdNum.fetchAndAdd(1));
    }

    // ---------------------------------------------------------------------- //

    void KVRecordStore::KVRecordIterator::_setCursor(const RecordId loc) {
        // We should no cursor at this point, either because we're getting newly
        // constructed or because we're recovering from saved state (and so
        // the old cursor needed to be dropped).
        invariant(!_cursor);
        _cursor.reset();
        _savedLoc = RecordId();
        _savedVal = Slice();

        // A new iterator with no start position will be either min() or max()
        invariant(loc.isNormal() || loc == RecordId::min() || loc == RecordId::max());
        const RecordIdKey key(loc);
        _cursor.reset(_db->getCursor(_txn, key.key(), _dir));
    }

    KVRecordStore::KVRecordIterator::KVRecordIterator(KVDictionary *db, OperationContext *txn,
                                                      const RecordId &start,
                                                      const CollectionScanParams::Direction &dir) :
        _db(db), _dir(dir), _savedLoc(), _savedVal(), _txn(txn), _cursor() {
        if (start.isNull()) {
            // A null RecordId means the beginning for a forward cursor,
            // and the end for a reverse cursor.
            _setCursor(_dir == CollectionScanParams::FORWARD ? RecordId::min() : RecordId::max());
        } else {
            _setCursor(start);
        }
    }

    bool KVRecordStore::KVRecordIterator::isEOF() {
        return !_cursor || !_cursor->ok();
    }

    RecordId KVRecordStore::KVRecordIterator::curr() {
        if (isEOF()) {
            return RecordId();
        }

        const RecordIdKey key(_cursor->currKey());
        return key.id();
    }

    void KVRecordStore::KVRecordIterator::_saveLocAndVal() {
        if (!isEOF()) {
            _savedLoc = curr();
            _savedVal = _cursor->currVal().owned();
        } else {
            _savedLoc = RecordId();
            _savedVal = Slice();
        }
    }

    RecordId KVRecordStore::KVRecordIterator::getNext() {
        if (isEOF()) {
            return RecordId();
        }

        // We need valid copies of _savedLoc / _savedVal since we are
        // about to advance the underlying cursor.
        _saveLocAndVal();
        _cursor->advance(_txn);
        return _savedLoc;
    }

    void KVRecordStore::KVRecordIterator::invalidate(const RecordId& loc) {
        // this only gets called to invalidate potentially buffered
        // `loc' results between saveState() and restoreState(). since
        // we dropped our cursor and have no buffered rows, we do nothing.
    }

    void KVRecordStore::KVRecordIterator::saveState() {
        // we need to drop the current cursor because it was created with
        // an operation context that the caller intends to close after
        // this function finishes (and before restoreState() is called,
        // which will give us a new operation context)
        _saveLocAndVal();
        _cursor.reset();
        _txn = NULL;
    }

    bool KVRecordStore::KVRecordIterator::restoreState(OperationContext* txn) {
        invariant(!_txn && !_cursor);
        _txn = txn;
        if (!_savedLoc.isNull()) {
            _setCursor(_savedLoc);
        } else {
            // We had saved state when the cursor was at EOF, so the savedLoc
            // was null - therefore we must restoreState to EOF as well. 
            //
            // Assert that this is indeed the case.
            invariant(isEOF());
        }

        // `true' means the collection still exists, which is always the case
        // because this cursor would have been deleted by higher layers if
        // the collection were to indeed be dropped.
        return true;
    }

    RecordData KVRecordStore::KVRecordIterator::dataFor(const RecordId& loc) const {
        invariant(_txn);

        // Kind-of tricky:
        //
        // We save the last loc and val that we were pointing to before a call
        // to getNext(). We know that our caller intends to call dataFor() on
        // each loc read this way, so if the given loc is equal to the last 
        // loc, then we can return the last value read, which we own and now
        // pass to the caller with a shared pointer.
        if (!_savedLoc.isNull() && _savedLoc == loc) {
            Slice val = _savedVal;
            invariant(val.mutableData());
            return RecordData(val.ownedBuf().moveFrom(), val.size());
        } else {
            // .. otherwise something strange happened and the caller actually
            // wants some other data entirely. we should probably never execute
            // this code that often because it is slow to descend the dictionary
            // for every value we want to read..
            return _getDataFor(_db, _txn, loc);
        }
    }

} // namespace mongo
