// visible_id_tracker.h

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

#include <set>

#include <boost/thread/mutex.hpp>

#include "mongo/db/operation_context.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/kv/dictionary/kv_record_store.h"
#include "mongo/db/storage/recovery_unit.h"

namespace mongo {

    class VisibleIdTracker {
    public:
        virtual ~VisibleIdTracker() {}

        virtual bool canReadId(const RecordId &id) const = 0;

        virtual void addUncommittedId(OperationContext *opCtx, const RecordId &id) = 0;

        virtual RecordId lowestInvisible() const = 0;

        virtual void setIteratorRestriction(KVRecordStore::KVRecordIterator *iter) const = 0;
    };

    class CappedIdTracker : public VisibleIdTracker {
    protected:
        mutable boost::mutex _mutex;
        std::set<RecordId> _uncommittedIds;
        RecordId _highest;

    public:
        CappedIdTracker()
            : _highest(RecordId::min())
        {}

        virtual ~CappedIdTracker() {}

        virtual bool canReadId(const RecordId &id) const {
            return id < lowestInvisible();
        }

        virtual void addUncommittedId(OperationContext *opCtx, const RecordId &id) {
            opCtx->recoveryUnit()->registerChange(new UncommittedIdChange(this, id));

            boost::mutex::scoped_lock lk(_mutex);
            _uncommittedIds.insert(id);
            if (id > _highest) {
                _highest = id;
            }
        }

        virtual RecordId lowestInvisible() const {
            boost::mutex::scoped_lock lk(_mutex);
            RecordId li = _highest;
            li.inc(1);
            return (_uncommittedIds.empty()
                    //? RecordId(_highest.repr() + 1)  // eww
                    ? li
                    : *_uncommittedIds.begin());
        }

        virtual void setIteratorRestriction(KVRecordStore::KVRecordIterator *iter) const {
            iter->setIdTracker(this);
        }

    protected:
        class UncommittedIdChange : public RecoveryUnit::Change {
            CappedIdTracker *_tracker;
            RecordId _id;

        public:
            UncommittedIdChange(CappedIdTracker *tracker, RecordId id)
                : _tracker(tracker),
                  _id(id)
            {}

            virtual void commit() {
                _tracker->markIdVisible(_id);
            }
            virtual void rollback() {
                _tracker->markIdVisible(_id);
            }
        };

        void markIdVisible(const RecordId &id) {
            boost::mutex::scoped_lock lk(_mutex);
            _uncommittedIds.erase(id);
        }

        friend class UncommittedIdChange;
    };

    class OplogIdTracker : public CappedIdTracker {
    public:
        void setIteratorRestriction(KVRecordStore::KVRecordIterator *iter) const {
            CappedIdTracker::setIteratorRestriction(iter);
            iter->setLowestInvisible(lowestInvisible());
        }
    };

}
