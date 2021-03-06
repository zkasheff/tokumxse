// kv_record_store_capped.h

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

#pragma once

#include <boost/scoped_ptr.hpp>

#include "mongo/db/storage/kv/dictionary/kv_record_store.h"
#include "mongo/db/storage/kv/dictionary/visible_id_tracker.h"

namespace mongo {

    class KVSizeStorer;

    // Like a KVRecordStore, but size is capped and inserts
    // may truncate off old records from the beginning.
    class KVRecordStoreCapped : public KVRecordStore {
    public:
        // KVRecordStore takes ownership of db
        KVRecordStoreCapped( KVDictionary *db,
                             OperationContext* opCtx,
                             StringData ns,
                             StringData ident,
                             const CollectionOptions& options,
                             KVSizeStorer *sizeStorer,
                             bool engineSupportsDocLocking);

        virtual ~KVRecordStoreCapped() { }

        virtual StatusWith<RecordId> insertRecord( OperationContext* txn,
                                                  const char* data,
                                                  int len,
                                                  bool enforceQuota );

        virtual StatusWith<RecordId> insertRecord( OperationContext* txn,
                                                  const DocWriter* doc,
                                                  bool enforceQuota );

        virtual RecordIterator* getIterator( OperationContext* txn,
                                             const RecordId& start = RecordId(),
                                             const CollectionScanParams::Direction& dir =
                                             CollectionScanParams::FORWARD ) const;

        virtual void appendCustomStats( OperationContext* txn,
                                        BSONObjBuilder* result,
                                        double scale ) const;

        // KVRecordStore is not capped, KVRecordStoreCapped is capped
        virtual bool isCapped() const { return true; }

        virtual void temp_cappedTruncateAfter(OperationContext* txn,
                                              RecordId end,
                                              bool inclusive);

        virtual void setCappedDeleteCallback(CappedDocumentDeleteCallback* cb) {
            _cappedDeleteCallback = cb;
        }

        virtual bool cappedMaxDocs() const { return _cappedMaxDocs; }

        virtual bool cappedMaxSize() const { return _cappedMaxSize; }

        virtual boost::optional<RecordId> oplogStartHack(OperationContext* txn,
                                                         const RecordId& startingPosition) const;

        virtual Status oplogDiskLocRegister(OperationContext* txn,
                                            const OpTime& opTime);

    private:
        bool needsDelete(OperationContext *txn) const;

        void deleteAsNeeded(OperationContext *txn);

        const int64_t _cappedMaxSize;
        const int64_t _cappedMaxSizeSlack; // when to start applying backpressure
        const int64_t _cappedMaxDocs;
        RecordId _lastDeletedId;
        CappedDocumentDeleteCallback* _cappedDeleteCallback;
        boost::mutex _cappedDeleteMutex;

        const bool _engineSupportsDocLocking;
        const bool _isOplog;
        boost::scoped_ptr<VisibleIdTracker> _idTracker;
    };

} // namespace mongo
