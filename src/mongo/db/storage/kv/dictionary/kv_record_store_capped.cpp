// kv_record_store_capped.cpp

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

#include <boost/scoped_ptr.hpp>

#include "mongo/base/checked_cast.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/kv/dictionary/kv_record_store_capped.h"
#include "mongo/db/storage/kv/dictionary/visible_id_tracker.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

namespace mongo {

    KVRecordStoreCapped::KVRecordStoreCapped( KVDictionary *db,
                                              OperationContext* opCtx,
                                              StringData ns,
                                              StringData ident,
                                              const CollectionOptions& options,
                                              KVSizeStorer *sizeStorer,
                                              bool engineSupportsDocLocking)
        : KVRecordStore(db, opCtx, ns, ident, options, sizeStorer),
          _cappedMaxSize(options.cappedSize ? options.cappedSize : 4096 ),
          _cappedMaxSizeSlack(std::min(_cappedMaxSize/10, int64_t(64<<20))),
          _cappedMaxDocs(options.cappedMaxDocs ? options.cappedMaxDocs : -1),
          _lastDeletedId(RecordId::min()),
          _cappedDeleteCallback(NULL),
          _engineSupportsDocLocking(engineSupportsDocLocking),
          _isOplog(NamespaceString::oplog(ns)),
          _idTracker(_engineSupportsDocLocking
                     ? (_isOplog
                        ? static_cast<VisibleIdTracker *>(new OplogIdTracker(_nextIdNum.load()))
                        : static_cast<VisibleIdTracker *>(new CappedIdTracker(_nextIdNum.load())))
                     : static_cast<VisibleIdTracker *>(new NoopIdTracker()))
    {}

    bool KVRecordStoreCapped::needsDelete(OperationContext* txn) const {
        if (dataSize(txn) >= _cappedMaxSize) {
            // .. too many bytes
            return true;
        }

        if ((_cappedMaxDocs != -1) && (numRecords(txn) > _cappedMaxDocs)) {
            // .. too many documents
            return true;
        }

        // we're ok
        return false;
    }

    class TempRecoveryUnitSwap {
        OperationContext *_txn;
        KVRecoveryUnit *_oldRecoveryUnit;

    public:
        TempRecoveryUnitSwap(OperationContext *txn)
            : _txn(txn),
              _oldRecoveryUnit(checked_cast<KVRecoveryUnit *>(_txn->releaseRecoveryUnit())) {
            _txn->setRecoveryUnit(_oldRecoveryUnit->newRecoveryUnit());
        }
        ~TempRecoveryUnitSwap() {
            boost::scoped_ptr<RecoveryUnit> deleting(_txn->releaseRecoveryUnit());
            _txn->setRecoveryUnit(_oldRecoveryUnit);
        }
    };

    void KVRecordStoreCapped::deleteAsNeeded(OperationContext *txn) {
        if (!needsDelete(txn)) {
            // nothing to do
            return;
        }

        // Only one thread should do deletes at a time, otherwise they'll conflict.
        boost::mutex::scoped_lock lock(_cappedDeleteMutex, boost::defer_lock);
        if (_cappedMaxDocs != -1) {
            lock.lock();
        } else {
            if (!lock.try_lock()) {
                // Someone else is deleting old records. Apply back-pressure if too far behind,
                // otherwise continue.
                if ((dataSize(txn) - _cappedMaxSize) < _cappedMaxSizeSlack)
                    return;

                lock.lock();

                // If we already waited, let someone else do cleanup unless we are significantly
                // over the limit.
                if ((dataSize(txn) - _cappedMaxSize) < (2 * _cappedMaxSizeSlack))
                    return;
            }
        }

        // we do this is a side transaction in case it aborts
        TempRecoveryUnitSwap swap(txn);

        int64_t ds = dataSize(txn);
        int64_t nr = numRecords(txn);
        int64_t sizeOverCap = (ds > _cappedMaxSize) ? ds - _cappedMaxSize : 0;
        int64_t sizeSaved = 0;
        int64_t docsOverCap = (_cappedMaxDocs != -1 && nr > _cappedMaxDocs) ? nr - _cappedMaxDocs : 0;
        int64_t docsRemoved = 0;

        try {
            WriteUnitOfWork wuow(txn);

            // We're going to notify the underlying store that we've
            // deleted this range of ids.  In TokuFT, this will trigger an
            // optimize.
            RecordId firstDeleted, lastDeleted;

            Timer t;

            // Delete documents while we are over-full and the iterator has more.
            //
            // Note that the iterator we get has the _idTracker's logic
            // already built in, so we don't need to worry about deleting
            // records that are not yet committed, including the one we
            // just inserted
            for (boost::scoped_ptr<RecordIterator> iter(getIterator(txn));
                 ((sizeSaved < sizeOverCap || docsRemoved < docsOverCap) &&
                  !iter->isEOF());
                 ) {
                const RecordId oldest = iter->getNext();

                ++docsRemoved;
                sizeSaved += iter->dataFor(oldest).size();

                if (_cappedDeleteCallback) {
                    // need to notify higher layers that a RecordId is about to be deleted
                    uassertStatusOK(_cappedDeleteCallback->aboutToDeleteCapped(txn, oldest, iter->dataFor(oldest)));
                }
                deleteRecord(txn, oldest);

                if (firstDeleted.isNull()) {
                    firstDeleted = oldest;
                }
                dassert(oldest > lastDeleted);
                lastDeleted = oldest;

                // Now, decide whether to keep working, we want to balance
                // staying on top of the deletion workload with the
                // latency of the client that's doing the deletes for
                // everyone.
                if (sizeOverCap >= _cappedMaxSizeSlack) {
                    // If we're over the slack amount, everyone's going to
                    // block on us anyway, so we may as well keep working.
                    continue;
                }
                if (sizeOverCap < (_cappedMaxSizeSlack / 4) && docsRemoved >= 1000) {
                    // If we aren't too much over and we've done a fair
                    // amount of work, take a break.
                    break;
                } else if (docsRemoved % 1000 == 0 && t.seconds() >= 4) {
                    // If we're under the slack amount and we've already
                    // spent a second working on this, return and give
                    // someone else a chance to shoulder that latency.
                    break;
                }
            }

            if (docsRemoved > 0) {
                _db->justDeletedCappedRange(txn, Slice::of(KeyString(firstDeleted)), Slice::of(KeyString(lastDeleted)),
                                            sizeSaved, docsRemoved);
                wuow.commit();
                dassert(lastDeleted > _lastDeletedId);
                _lastDeletedId = lastDeleted;
            }
        } catch (WriteConflictException) {
            log() << "Got conflict truncating capped, ignoring.";
            return;
        }
    }

    StatusWith<RecordId> KVRecordStoreCapped::insertRecord( OperationContext* txn,
                                                           const char* data,
                                                           int len,
                                                           bool enforceQuota ) {
        if (len > _cappedMaxSize) {
            // this single document won't fit
            return StatusWith<RecordId>(ErrorCodes::BadValue,
                                       "object to insert exceeds cappedMaxSize");
        }

        StatusWith<RecordId> id(Status::OK());
        if (_isOplog) {
            id = oploghack::extractKey(data, len);
            if (!id.isOK()) {
                return id;
            }

            Status s = _insertRecord(txn, id.getValue(), Slice(data, len));
            if (!s.isOK()) {
                return StatusWith<RecordId>(s);
            }
        } else {
            // insert using the regular KVRecordStore insert implementation..
            id = KVRecordStore::insertRecord(txn, data, len, enforceQuota);
            if (!id.isOK()) {
                return id;
            }
        }

        _idTracker->addUncommittedId(txn, id.getValue());

        // ..then delete old data as needed
        deleteAsNeeded(txn);

        return id;
    }

    StatusWith<RecordId> KVRecordStoreCapped::insertRecord( OperationContext* txn,
                                                           const DocWriter* doc,
                                                           bool enforceQuota ) {
        // We need to override every insertRecord overload, otherwise the compiler gets mad.
        Slice value(doc->documentSize());
        doc->writeDocument(value.mutableData());
        return insertRecord(txn, value.data(), value.size(), enforceQuota);
    }

    void KVRecordStoreCapped::appendCustomStats( OperationContext* txn,
                                                 BSONObjBuilder* result,
                                                 double scale ) const {
        result->append("capped", true);
        result->appendIntOrLL("max", _cappedMaxDocs);
        result->appendIntOrLL("maxSize", _cappedMaxSize / scale);
        KVRecordStore::appendCustomStats(txn, result, scale);
    }

    void KVRecordStoreCapped::temp_cappedTruncateAfter(OperationContext* txn,
                                                       RecordId end,
                                                       bool inclusive) {
        WriteUnitOfWork wu( txn );
        // Not very efficient, but it should only be used by tests.
        for (boost::scoped_ptr<RecordIterator> iter(KVRecordStore::getIterator(txn, end)); !iter->isEOF(); ) {
            RecordId loc = iter->getNext();
            if (!inclusive && loc == end) {
                continue;
            }
            deleteRecord(txn, loc);
        }
        wu.commit();
    }

    boost::optional<RecordId> KVRecordStoreCapped::oplogStartHack(OperationContext* txn,
                                                                  const RecordId& startingPosition) const {
        if (!_isOplog) {
            return boost::none;
        }

        RecordId lowestInvisible = _idTracker->lowestInvisible();
        for (boost::scoped_ptr<RecordIterator> iter(getIterator(txn, startingPosition, CollectionScanParams::BACKWARD));
             !iter->isEOF(); iter->getNext()) {
            if (iter->curr() <= startingPosition && iter->curr() < lowestInvisible) {
                return iter->curr();
            }
        }
        return boost::none;
    }

    Status KVRecordStoreCapped::oplogDiskLocRegister(OperationContext* txn,
                                                     const OpTime& opTime) {
        if (!_engineSupportsDocLocking) {
            return Status::OK();
        }

        StatusWith<RecordId> loc = oploghack::keyForOptime( opTime );
        if ( !loc.isOK() )
            return loc.getStatus();

        _idTracker->addUncommittedId(txn, loc.getValue());
        return Status::OK();
    }

    RecordIterator* KVRecordStoreCapped::getIterator(OperationContext* txn,
                                                     const RecordId& start,
                                                     const CollectionScanParams::Direction& dir) const {
        const RecordId &realStart = ((dir == CollectionScanParams::FORWARD &&
                                      (start.isNull() || start == RecordId::min()))
                                     ? _lastDeletedId
                                     : start);
        if (_engineSupportsDocLocking && dir == CollectionScanParams::FORWARD) {
            KVRecoveryUnit *ru = checked_cast<KVRecoveryUnit *>(txn->recoveryUnit());
            // Must set this before we call KVRecordStore::getIterator because that will create a
            // snapshot.
            _idTracker->setRecoveryUnitRestriction(ru);

            std::auto_ptr<RecordIterator> iter(KVRecordStore::getIterator(txn, realStart, dir));

            KVRecordIterator *kvIter = checked_cast<KVRecordIterator *>(iter.get());
            _idTracker->setIteratorRestriction(ru, kvIter);

            return iter.release();
        } else {
            return KVRecordStore::getIterator(txn, realStart, dir);
        }
    }

} // namespace mongo
