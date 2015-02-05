// tokuft_capped_delete_range_optimizer.h

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

#pragma once

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include <ftcxx/db.hpp>

#include "mongo/db/record_id.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/kv/slice.h"

namespace mongo {

    /**
     * Capped collections delete from the back in batches (see KVRecordStoreCapped::deleteAsNeeded),
     * and then notify the KVDictionary that a batch has been deleted and can be optimized.
     * TokuFTCappedDeleteRangeOptimizer manages, for a specific record store, a background thread
     * which will optimize old ranges of deleted data in the background.  We apply backpressure when
     * the optimizer thread gets too far behind the deleted data.
     */
    class TokuFTCappedDeleteRangeOptimizer {
    public:
        TokuFTCappedDeleteRangeOptimizer(const ftcxx::DB &db);

        ~TokuFTCappedDeleteRangeOptimizer();

        /**
         * Notifies the thread that new data has been deleted beyond max, and so everything before
         * max is eligible for optimization.  Also notes the size and number of documents deleted in
         * the current batch (which will be eligible for optimization later).
         *
         * On restart, we forget about whatever deletes were not yet optimized, but since we always
         * optimize from negative infinity, those things will get optimized in the first pass
         * anyway.
         */
        void updateMaxDeleted(const RecordId &max, int64_t sizeSaved, int64_t docsRemoved);

        void run();

    private:
        static const KeyString kNegativeInfinity;
        int _magic;

        const ftcxx::DB &_db;
        RecordId _max;

        // The most recently deleted range is not optimizable.  Once we see more deletes, we
        // consider that amount optimizable again.
        int64_t _unoptimizableSize;
        int64_t _optimizableSize;

        bool _running;
        bool _terminated;

        boost::thread _thread;

        boost::mutex _mutex;
        boost::condition_variable _updateCond;
        boost::condition_variable _backpressureCond;
    };

}
