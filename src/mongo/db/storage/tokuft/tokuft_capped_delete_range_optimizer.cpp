// tokuft_capped_delete_range_optimizer.cpp

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

#include "mongo/db/storage/tokuft/tokuft_capped_delete_range_optimizer.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary.h"
#include "mongo/db/storage/tokuft/tokuft_errors.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

namespace mongo {

    const KeyString TokuFTCappedDeleteRangeOptimizer::kNegativeInfinity(RecordId::min());

    namespace {

        int MAGIC = 321123;

    }

    TokuFTCappedDeleteRangeOptimizer::TokuFTCappedDeleteRangeOptimizer(const ftcxx::DB &db)
        : _magic(MAGIC),
          _db(db),
          _max(),
          _unoptimizableSize(0),
          _optimizableSize(0),
          _running(true),
          _terminated(false),
          _thread(boost::bind(&TokuFTCappedDeleteRangeOptimizer::run, this))
    {}

    TokuFTCappedDeleteRangeOptimizer::~TokuFTCappedDeleteRangeOptimizer() {
        {
            boost::mutex::scoped_lock lk(_mutex);
            _running = false;
            _updateCond.notify_one();
        }

        {
            boost::mutex::scoped_lock lk(_mutex);
            while (!_terminated) {
                _updateCond.wait(lk);
            }
            _thread.join();
        }
        _magic = 11111;
    }

    namespace {

        class CappedDeleteRangeOptimizeCallback {
            Timer _timer;
            int _lastWarnedAboutTime;
            static const size_t kLoopsWarningLimit = 100;
            size_t _loops;

        public:
            CappedDeleteRangeOptimizeCallback()
                : _lastWarnedAboutTime(0),
                  _loops(0)
            {}

            ~CappedDeleteRangeOptimizeCallback() {
                if (_loops >= kLoopsWarningLimit) {
                    warning() << "TokuFT: Capped deleter optimized " << _lastLoopsWarning
                              << " nodes in one shot, may be falling behind.";
                }
            }

            int operator()(float progress, size_t loops) {
                _loops = loops;
                int secs = _timer.seconds();
                if (secs > _lastWarnedAboutTime) {
                    _lastWarnedAboutTime = secs;
                    if (secs >= 10) {
                        severe() << "TokuFT: Capped deleter has been optimizing for " << secs
                                 << " seconds, may be seriously falling behind.";
                    } else  {
                        warning() << "TokuFT: Capped deleter has been optimizing for " << secs
                                  << " seconds, may be falling behind.";
                    }
                }
                return 0;
            }
        };

    }

    void TokuFTCappedDeleteRangeOptimizer::run() {
        int64_t sizeOptimizing = 0;
        while (_running) {
            RecordId max;
            {
                boost::mutex::scoped_lock lk(_mutex);

                _optimizableSize -= sizeOptimizing;
                _backpressureCond.notify_one();

                while (_max.isNull() && _running) {
                    dassert(_optimizableSize == 0);
                    _updateCond.wait(lk);
                }
                if (!_running) {
                    break;
                }

                max = _max;
                _max = RecordId();
                sizeOptimizing = _optimizableSize;
            }

            const int r = _db.hot_optimize(slice2ftslice(Slice::of(kNegativeInfinity)), slice2ftslice(Slice::of(KeyString(max))),
                                           CappedDeleteRangeOptimizeCallback());
            Status s = statusFromTokuFTError(r);
            if (!s.isOK()) {
                log() << "TokuFT: Capped deleter got error from hot optimize operation " << s;
            }
        }

        {
            boost::mutex::scoped_lock lk(_mutex);
            _terminated = true;
            _updateCond.notify_one();
        }
    }

    void TokuFTCappedDeleteRangeOptimizer::updateMaxDeleted(const RecordId &max, int64_t sizeSaved, int64_t docsRemoved) {
        boost::mutex::scoped_lock lk(_mutex);

        // Now that we've deleted things higher than max, we'll assume anything that was deleted
        // earlier (unoptimizableSize) is now optimizable, and the new deletes are unoptimizable.
        _optimizableSize += _unoptimizableSize;
        _unoptimizableSize = sizeSaved;
        _max = max;
        _updateCond.notify_one();

        static const int64_t lowWatermark = 32<<20;
        static const int64_t highWatermark = lowWatermark * 4;
        if (_optimizableSize > highWatermark) {
            // This will wait for the optimize thread to catch up.  It should actually go to zero
            // rather than just below lowWatermark, but we use hysteresis because it's the right
            // thing if the implementation changes.
            //
            // Since this is done while holding the cappedDeleteMutex, it will apply backpressure
            // gradually, once other threads insert enough to get them to start waiting behind that
            // mutex.
            while (_optimizableSize > lowWatermark) {
                log() << "TokuFT: Capped delete optimizer is " << (_optimizableSize>>20)
                      << "MB behind, waiting for it to catch up somewhat.";

                _backpressureCond.wait(lk);
            }
        }
    }

}
