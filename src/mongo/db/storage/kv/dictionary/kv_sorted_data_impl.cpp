// kv_sorted_data_impl.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#include <boost/scoped_ptr.hpp>

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary.h"
#include "mongo/db/storage/kv/dictionary/kv_sorted_data_impl.h"
#include "mongo/db/storage/kv/slice.h"
#include "mongo/platform/endian.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    namespace {

        const int kTempKeyMaxSize = 1024; // this goes away with SERVER-3372

        Status checkKeySize(const BSONObj &key) {
            if (key.objsize() >= kTempKeyMaxSize) {
                StringBuilder sb;
                sb << "KVSortedDataImpl::insert(): key too large to index, failing "
                   << key.objsize() << ' ' << key;
                return Status(ErrorCodes::KeyTooLong, sb.str());
            }
            return Status::OK();
        }

        bool hasFieldNames(const BSONObj& obj) {
            BSONForEach(e, obj) {
                if (e.fieldName()[0])
                    return true;
            }
            return false;
        }

        BSONObj stripFieldNames(const BSONObj& query) {
            if (!hasFieldNames(query))
                return query;

            BSONObjBuilder bb;
            BSONForEach(e, query) {
                bb.appendAs(e, StringData());
            }
            return bb.obj();
        }

        /**
         * Creates an error code message out of a key
         */
        string dupKeyError(const BSONObj& key) {
            StringBuilder sb;
            sb << "E11000 duplicate key error ";
            // TODO figure out how to include index name without dangerous casts
            sb << "dup key: " << key.toString();
            return sb.str();
        }

    }  // namespace

    KVSortedDataImpl::KVSortedDataImpl(KVDictionary* db,
                                       OperationContext* opCtx,
                                       const IndexDescriptor* desc)
        : _db(db),
          _ordering(Ordering::make(desc ? desc->keyPattern() : BSONObj()))
    {
        invariant(_db);
    }

    Status KVSortedDataBuilderImpl::addKey(const BSONObj& key, const RecordId& loc) {
        return _impl->insert(_txn, key, loc, _dupsAllowed);
    }

    SortedDataBuilderInterface* KVSortedDataImpl::getBulkBuilder(OperationContext* txn,
                                                                 bool dupsAllowed) {
      return new KVSortedDataBuilderImpl(this, txn, dupsAllowed);
    }

    BSONObj KVSortedDataImpl::extractKey(const Slice &s, const Ordering &ordering) {
        return KeyString::toBson(s.data(), s.size(), ordering);
    }

    RecordId KVSortedDataImpl::extractRecordId(const Slice &s) {
        return KeyString::decodeRecordIdEndingAt(s.data() + s.size() - 1);
    }

    Status KVSortedDataImpl::insert(OperationContext* txn,
                                    const BSONObj& key,
                                    const RecordId& loc,
                                    bool dupsAllowed) {
        invariant(loc.isNormal());
        dassert(!hasFieldNames(key));

        Status s = checkKeySize(key);
        if (!s.isOK()) {
            return s;
        }

        try {
            if (!dupsAllowed) {
                s = (_db->supportsDupKeyCheck()
                     ? _db->dupKeyCheck(txn,
                                        Slice::of(KeyString::make(key, _ordering, RecordId::min())),
                                        Slice::of(KeyString::make(key, _ordering, RecordId::max())),
                                        loc)
                     : dupKeyCheck(txn, key, loc));
                if (s == ErrorCodes::DuplicateKey) {
                    // Adjust the message to include the key.
                    return Status(ErrorCodes::DuplicateKey, dupKeyError(key));
                }
                if (!s.isOK()) {
                    return s;
                }
            }

            s = _db->insert(txn, Slice::of(KeyString::make(key, _ordering, loc)), Slice());
        } catch (WriteConflictException) {
            if (!dupsAllowed) {
                // If we see a WriteConflictException on a unique index, according to
                // https://jira.mongodb.org/browse/SERVER-16337 we should consider it a duplicate
                // key even if this means reporting false positives.
                return Status(ErrorCodes::DuplicateKey, dupKeyError(key));
            }
            throw;
        }

        return s;
    }

    void KVSortedDataImpl::unindex(OperationContext* txn,
                                   const BSONObj& key,
                                   const RecordId& loc,
                                   bool dupsAllowed) {
        invariant(loc.isNormal());
        dassert(!hasFieldNames(key));
        _db->remove(txn, Slice::of(KeyString::make(key, _ordering, loc)));
    }

    Status KVSortedDataImpl::dupKeyCheck(OperationContext* txn,
                                         const BSONObj& key,
                                         const RecordId& loc) {
        boost::scoped_ptr<SortedDataInterface::Cursor> cursor(newCursor(txn, 1));
        cursor->locate(key, RecordId());

        if (cursor->isEOF() || cursor->getKey() != key) {
            return Status::OK();
        } else if (cursor->getRecordId() == loc) {
            return Status::OK();
        } else {
            return Status(ErrorCodes::DuplicateKey, dupKeyError(key));
        }
    }

    void KVSortedDataImpl::fullValidate(OperationContext* txn, bool full, long long* numKeysOut,
                                        BSONObjBuilder* output) const {
        if (numKeysOut) {
            *numKeysOut = 0;
            for (boost::scoped_ptr<KVDictionary::Cursor> cursor(_db->getCursor(txn));
                 cursor->ok(); cursor->advance(txn)) {
                ++(*numKeysOut);
            }
        }
    }

    bool KVSortedDataImpl::isEmpty( OperationContext* txn ) {
        boost::scoped_ptr<KVDictionary::Cursor> cursor(_db->getCursor(txn));
        return !cursor->ok();
    }

    Status KVSortedDataImpl::touch(OperationContext* txn) const {
        // fullValidate iterates over every key, which brings things into memory
        long long numKeys;
        fullValidate(txn, true, &numKeys, NULL);
        return Status::OK();
    }

    long long KVSortedDataImpl::numEntries(OperationContext* txn) const {
        long long numKeys = 0;
        fullValidate(txn, true, &numKeys, NULL);
        return numKeys;
    }

    Status KVSortedDataImpl::initAsEmpty(OperationContext* txn) {
        // no-op
        return Status::OK();
    }

    long long KVSortedDataImpl::getSpaceUsedBytes( OperationContext* txn ) const {
        KVDictionary::Stats stats = _db->getStats();
        return stats.storageSize;
    }

    bool KVSortedDataImpl::appendCustomStats(OperationContext* txn, BSONObjBuilder* output, double scale) const {
        return _db->appendCustomStats(txn, output, scale);
    }

    // ---------------------------------------------------------------------- //

    class KVSortedDataInterfaceCursor : public SortedDataInterface::Cursor {
        KVDictionary *_db;
        const int _dir;
        OperationContext *_txn;
        const Ordering &_ordering;

        mutable boost::scoped_ptr<KVDictionary::Cursor> _cursor;
        BSONObj _savedKey;
        RecordId _savedLoc;
        mutable bool _initialized;

        void _initialize() const {
            if (_initialized) {
                return;
            }
            _initialized = true;
            if (_cursor) {
                return;
            }
            _cursor.reset(_db->getCursor(_txn, _dir));
        }

        bool _locate(const BSONObj &key, const RecordId &loc) {
            _cursor.reset(_db->getCursor(_txn, Slice::of(KeyString::make(key, _ordering, loc)), _dir));
            return !isEOF() && loc == getRecordId() && key == getKey();
        }

    public:
        KVSortedDataInterfaceCursor(KVDictionary *db, OperationContext *txn, int direction, const Ordering &ordering)
            : _db(db),
              _dir(direction),
              _txn(txn),
              _ordering(ordering),
              _cursor(),
              _savedKey(),
              _savedLoc(),
              _initialized(false)
        {}

        virtual ~KVSortedDataInterfaceCursor() {}

        int getDirection() const {
            return _dir;
        }

        bool isEOF() const {
            _initialize();
            return !_cursor || !_cursor->ok();
        }

        bool pointsToSamePlaceAs(const Cursor& other) const {
            return getRecordId() == other.getRecordId() && getKey() == other.getKey();
        }

        void aboutToDeleteBucket(const RecordId& bucket) { }

        bool locate(const BSONObj& key, const RecordId& loc) {
            return _locate(stripFieldNames(key), loc);
        }

        void advanceTo(const BSONObj &keyBegin,
                       int keyBeginLen,
                       bool afterKey,
                       const vector<const BSONElement*>& keyEnd,
                       const vector<bool>& keyEndInclusive) {
            // make a key representing the location to which we want to advance.
            BSONObj key = IndexEntryComparison::makeQueryObject(
                                                                keyBegin,
                                                                keyBeginLen,
                                                                afterKey,
                                                                keyEnd,
                                                                keyEndInclusive,
                                                                getDirection() );
            _locate(key, _dir > 0 ? RecordId::min() : RecordId::max());
        }

        void customLocate(const BSONObj& keyBegin,
                          int keyBeginLen,
                          bool afterVersion,
                          const vector<const BSONElement*>& keyEnd,
                          const vector<bool>& keyEndInclusive) {
            // The rocks engine has this to say:
            // XXX I think these do the same thing????
            advanceTo( keyBegin, keyBeginLen, afterVersion, keyEnd, keyEndInclusive );
        }

        BSONObj getKey() const {
            _initialize();
            if (isEOF()) {
                return BSONObj();
            }
            return KVSortedDataImpl::extractKey(_cursor->currKey(), _ordering);
        }

        RecordId getRecordId() const {
            _initialize();
            if (isEOF()) {
                return RecordId();
            }
            return KVSortedDataImpl::extractRecordId(_cursor->currKey());
        }

        void advance() {
            _initialize();
            if (!isEOF()) {
                _cursor->advance(_txn);
            }
        }

        void savePosition() {
            _initialize();
            _savedKey = getKey().getOwned();
            _savedLoc = getRecordId();
            _cursor.reset();
            _txn = NULL;
        }

        void restorePosition(OperationContext* txn) {
            invariant(!_txn && !_cursor);
            _txn = txn;
            _initialized = true;
            if (!_savedKey.isEmpty() && !_savedLoc.isNull()) {
                _locate(_savedKey, _savedLoc);
            } else {
                invariant(_savedKey.isEmpty() && _savedLoc.isNull());
                invariant(isEOF()); // this is the whole point!
            }
        }
    };

    SortedDataInterface::Cursor* KVSortedDataImpl::newCursor(OperationContext* txn,
                                                             int direction) const {
        return new KVSortedDataInterfaceCursor(_db.get(), txn, direction, _ordering);
    }

} // namespace mongo
