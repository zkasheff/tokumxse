// tokuft_engine_server_status.cpp

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

#include <stack>

#include <boost/scoped_array.hpp>
#include <boost/static_assert.hpp>

#include <db.h>
#include <toku_time.h>
#include <partitioned_counter.h>

#include <ftcxx/db_env.hpp>

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/storage/tokuft/tokuft_engine.h"
#include "mongo/db/storage/tokuft/tokuft_engine_global_accessor.h"
#include "mongo/db/storage/tokuft/tokuft_global_options.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    class FractalTreeEngineStatus {
    public:
        class Value {
            std::string _string;
            union {
                fs_redzone_state _fsState;
                double _double;
                uint64_t _uint64;
                time_t _time;
            };
            enum Type {
                Unknown = 0,
                FilesystemState,
                String,
                UnixTime,
                TokuTime,
                UInt64,
                Double,
            };
            Type _type;

        public:
            Value() : _type(Unknown) {}

            explicit Value(const TOKU_ENGINE_STATUS_ROW_S& row) {
                if (row.type == FS_STATE) {
                    _type = FilesystemState;
                    _fsState = (fs_redzone_state) row.value.num;
                } else if (row.type == UINT64) {
                    _type = UInt64;
                    _uint64 = row.value.num;
                } else if (row.type == PARCOUNT) {
                    _type = UInt64;
                    _uint64 = read_partitioned_counter(row.value.parcount);
                } else if (row.type == UNIXTIME) {
                    _type = UnixTime;
                    _time = row.value.num;
                } else if (row.type == TOKUTIME) {
                    _type = TokuTime;
                    _double = tokutime_to_seconds(row.value.num);
                } else if (row.type == DOUBLE) {
                    _type = Double;
                    _double = row.value.dnum;
                } else if (row.type == CHARSTR) {
                    _type = String;
                    _string = row.value.str;
                } else {
                    _type = Unknown;
                }
            }

            static Value panic(uint64_t val) {
                Value v;
                v._type = UInt64;
                v._uint64 = val;
                return v;
            }

            static Value panicString(const std::string& s) {
                Value v;
                v._type = String;
                v._string = s;
                return v;
            }

            void append(BSONObjBuilder& builder, const StringData& name, int scale = 1) const {
                if (_type == FilesystemState) {
                    BSONObjBuilder fsBuilder(builder.subobjStart(name));
                    if (_fsState == FS_GREEN) {
                        fsBuilder.append("state", "green");
                        fsBuilder.append("msg", "");
                    } else if (_fsState == FS_YELLOW) {
                        fsBuilder.append("state", "yellow");
                        fsBuilder.append("msg",
                                         str::stream() << "Filesystem space is low: less than "
                                         << (2 * tokuftGlobalOptions.engineOptions.fsRedzone)
                                         << "% remaining.");
                    } else if (_fsState == FS_RED) {
                        fsBuilder.append("state", "red");
                        fsBuilder.append("msg",
                                         str::stream() << "Filesystem space is critical: less than "
                                         << tokuftGlobalOptions.engineOptions.fsRedzone
                                         << "% remaining.  Engine is read-only until space is freed.");
                    } else if (_fsState == FS_BLOCKED) {
                        fsBuilder.append("state", "blocked");
                        fsBuilder.append("msg", "Filesystem is completely full.");
                    } else {
                        fsBuilder.append("state", "unknown");
                        fsBuilder.append("msg", str::stream() << "Code: " << (int) _fsState);
                    }
                    fsBuilder.doneFast();
                } else if (_type == String) {
                    builder.append(name, _string);
                } else if (_type == UnixTime) {
                    builder.appendTimeT(name, _time);
                } else if (_type == TokuTime) {
                    builder.appendNumber(name, _double);
                } else if (_type == UInt64) {
                    builder.appendNumber(name, static_cast<double>(_uint64) / scale);
                } else if (_type == Double) {
                    builder.append(name, _double / scale);
                } else if (_type == Unknown) {
                    builder.append(name, "unknown");
                } else {
                    builder.append(name, "invalid");
                }
            }

            uint64_t getInteger() const {
                massert(28598, "TokuFT: wrong engine status type for getInteger", _type == UInt64);
                return _uint64;
            }

            double getDuration() const {
                if (_type == TokuTime) {
                    return _double;
                } else if (_type == UnixTime) {
                    return static_cast<double>(_time);
                }
                msgasserted(28591, "TokuFT: wrong engine status type for getDouble");
            }
        };

    private:
        typedef std::map<std::string, Value> MapType;
        MapType _map;

    public:
        FractalTreeEngineStatus(ftcxx::DBEnv& env) {
            uint64_t max_rows = env.get_engine_status_num_rows();

            boost::scoped_array<TOKU_ENGINE_STATUS_ROW_S> rows(new TOKU_ENGINE_STATUS_ROW_S[max_rows]);
            uint64_t num_rows;
            uint64_t panic;
            std::string panic_string;
            env.get_engine_status(rows.get(), max_rows, num_rows, panic, panic_string, TOKU_ENGINE_STATUS);

            _map["PANIC"] = Value::panic(panic);
            _map["PANIC_STRING"] = Value::panicString(panic_string);
            for (uint64_t i = 0; i < num_rows; ++i) {
                _map[rows[i].keyname] = Value(rows[i]);
            }
        }

        const Value& operator[](const std::string& key) const {
            MapType::const_iterator it = _map.find(key);
            if (it == _map.end()) {
                static const Value unknown;
                return unknown;
            }
            return it->second;
        }
    };

    class NestedBuilder : boost::noncopyable {
    public:
        class Stack : public std::stack<BSONObjBuilder *> {
            BSONObjBuilder _bottom;
        public:
            Stack() {
                push(&_bottom);
            }
            BSONObjBuilder &b() { return *top(); }
            operator BSONObjBuilder&() { return b(); }
        };

        NestedBuilder(Stack &stack, const StringData &name)
            : _stack(stack), _b(_stack.b().subobjStart(name))
        {
            _stack.push(&_b);
        }

        ~NestedBuilder() {
            _stack.pop();
            _b.doneFast();
        }

    private:
        Stack &_stack;
        BSONObjBuilder _b;
    };

    class TokuFTServerStatusSection : public ServerStatusSection {
    public:
        TokuFTServerStatusSection() : ServerStatusSection("tokuft") {}
        virtual bool includeByDefault() const { return true; }

        BSONObj generateSection(OperationContext *opCtx, const BSONElement &configElement) const {
            if (!globalStorageEngineIsTokuFT()) {
                return BSONObj();
            }

            int scale = 1;
            if (configElement.isABSONObj()) {
                BSONObj o = configElement.Obj();
                BSONElement scaleElt = o["scale"];
                if (scaleElt.ok()) {
                    scale = scaleElt.safeNumberLong();
                }
                uassert(28599, "scale must be positive", scale > 0);
            }

            NestedBuilder::Stack result;

            FractalTreeEngineStatus status(tokuftGlobalEnv());

            {
                NestedBuilder _n1(result, "fsync");
                status["FS_FSYNC_COUNT"].append(result, "count");
                status["FS_FSYNC_TIME"].append(result, "time");
            }
            {
                NestedBuilder _n1(result, "log");
                status["LOGGER_NUM_WRITES"].append(result, "count");
                status["LOGGER_TOKUTIME_WRITES"].append(result, "time");
                status["LOGGER_BYTES_WRITTEN"].append(result, "bytes", scale);
            }
            {
                NestedBuilder _n1(result, "cachetable");
                {
                    NestedBuilder _n2(result, "size");
                    status["CT_SIZE_CURRENT"].append(result, "current", scale);
                    status["CT_SIZE_WRITING"].append(result, "writing", scale);
                    status["CT_SIZE_LIMIT"].append(result, "limit", scale);
                }
                {
                    NestedBuilder _n2(result, "miss");
                    uint64_t fullMisses = status["CT_MISS"].getInteger();
                    // unfortunately, this is a uint64 when it's actually a tokutime...
                    double fullMisstime = tokutime_to_seconds(status["CT_MISSTIME"].getInteger());
                    uint64_t partialMisses = 0;
                    double partialMisstime = 0.0;
                    const char *partialMissKeys[] = {"FT_NUM_BASEMENTS_FETCHED_NORMAL",
                                                     "FT_NUM_BASEMENTS_FETCHED_AGGRESSIVE",
                                                     "FT_NUM_BASEMENTS_FETCHED_PREFETCH",
                                                     "FT_NUM_BASEMENTS_FETCHED_WRITE",
                                                     "FT_NUM_MSG_BUFFER_FETCHED_NORMAL",
                                                     "FT_NUM_MSG_BUFFER_FETCHED_AGGRESSIVE",
                                                     "FT_NUM_MSG_BUFFER_FETCHED_PREFETCH",
                                                     "FT_NUM_MSG_BUFFER_FETCHED_WRITE"};
                    const char *partialMisstimeKeys[] = {"FT_TOKUTIME_BASEMENTS_FETCHED_NORMAL",
                                                         "FT_TOKUTIME_BASEMENTS_FETCHED_AGGRESSIVE",
                                                         "FT_TOKUTIME_BASEMENTS_FETCHED_PREFETCH",
                                                         "FT_TOKUTIME_BASEMENTS_FETCHED_WRITE",
                                                         "FT_TOKUTIME_MSG_BUFFER_FETCHED_NORMAL",
                                                         "FT_TOKUTIME_MSG_BUFFER_FETCHED_AGGRESSIVE",
                                                         "FT_TOKUTIME_MSG_BUFFER_FETCHED_PREFETCH",
                                                         "FT_TOKUTIME_MSG_BUFFER_FETCHED_WRITE"};
                    BOOST_STATIC_ASSERT((sizeof partialMissKeys) == (sizeof partialMisstimeKeys));
                    for (size_t i = 0; i < (sizeof partialMissKeys) / (sizeof partialMissKeys[0]); ++i) {
                        partialMisses += status[partialMissKeys[i]].getInteger();
                        partialMisstime += status[partialMisstimeKeys[i]].getDuration();
                    }

                    result.b().appendNumber("count", fullMisses + partialMisses);
                    result.b().append("time", fullMisstime + partialMisstime);
                    {
                        NestedBuilder _n3(result, "full");
                        result.b().appendNumber("count", fullMisses);
                        result.b().append("time", fullMisstime);
                    }
                    {
                        NestedBuilder _n3(result, "partial");
                        result.b().appendNumber("count", partialMisses);
                        result.b().append("time", partialMisstime);
                    }
                }
                {
                    NestedBuilder _n2(result, "evictions");
                    {
                        NestedBuilder _n3(result, "partial");
                        {
                            NestedBuilder _n4(result, "nonleaf");
                            {
                                NestedBuilder _n5(result, "clean");
                                status["FT_PARTIAL_EVICTIONS_NONLEAF"].append(result, "count");
                                status["FT_PARTIAL_EVICTIONS_NONLEAF_BYTES"].append(result, "bytes", scale);
                            }
                        }
                        {
                            NestedBuilder _n4(result, "leaf");
                            {
                                NestedBuilder _n5(result, "clean");
                                status["FT_PARTIAL_EVICTIONS_LEAF"].append(result, "count");
                                status["FT_PARTIAL_EVICTIONS_LEAF_BYTES"].append(result, "bytes", scale);
                            }
                        }
                    }
                    {
                        NestedBuilder _n3(result, "full");
                        {
                            NestedBuilder _n4(result, "nonleaf");
                            {
                                NestedBuilder _n5(result, "clean");
                                status["FT_FULL_EVICTIONS_NONLEAF"].append(result, "count");
                                status["FT_FULL_EVICTIONS_NONLEAF_BYTES"].append(result, "bytes", scale);
                            }
                            {
                                NestedBuilder _n5(result, "dirty");
                                status["FT_DISK_FLUSH_NONLEAF"].append(result, "count");
                                status["FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES"].append(result, "bytes", scale);
                                status["FT_DISK_FLUSH_NONLEAF_TOKUTIME"].append(result, "time");
                            }
                        }
                        {
                            NestedBuilder _n4(result, "leaf");
                            {
                                NestedBuilder _n5(result, "clean");
                                status["FT_FULL_EVICTIONS_LEAF"].append(result, "count");
                                status["FT_FULL_EVICTIONS_LEAF_BYTES"].append(result, "bytes", scale);
                            }
                            {
                                NestedBuilder _n5(result, "dirty");
                                status["FT_DISK_FLUSH_LEAF"].append(result, "count");
                                status["FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES"].append(result, "bytes", scale);
                                status["FT_DISK_FLUSH_LEAF_TOKUTIME"].append(result, "time");
                            }
                        }
                    }
                }
            }
            {
                NestedBuilder _n1(result, "checkpoint");
                status["CP_CHECKPOINT_COUNT"].append(result, "count");
                status["CP_TIME_CHECKPOINT_DURATION"].append(result, "time");
                status["CP_TIME_LAST_CHECKPOINT_BEGIN"].append(result, "lastBegin");
                {
                    NestedBuilder _n2(result, "lastComplete");
                    status["CP_TIME_LAST_CHECKPOINT_BEGIN_COMPLETE"].append(result, "begin");
                    status["CP_TIME_LAST_CHECKPOINT_END"].append(result, "end");
                    status["CP_TIME_CHECKPOINT_DURATION_LAST"].append(result, "time");
                }
                {
                    NestedBuilder _n2(result, "begin");
                    status["CP_BEGIN_TIME"].append(result, "time");
                }
                {
                    NestedBuilder _n2(result, "write");
                    {
                        NestedBuilder _n3(result, "nonleaf");
                        status["FT_DISK_FLUSH_NONLEAF_FOR_CHECKPOINT"].append(result, "count");
                        status["FT_DISK_FLUSH_NONLEAF_TOKUTIME_FOR_CHECKPOINT"].append(result, "time");
                        {
                            NestedBuilder _n4(result, "bytes");
                            status["FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT"].append(result, "uncompressed", scale);
                            status["FT_DISK_FLUSH_NONLEAF_BYTES_FOR_CHECKPOINT"].append(result, "compressed", scale);
                        }
                    }
                    {
                        NestedBuilder _n3(result, "leaf");
                        status["FT_DISK_FLUSH_LEAF_FOR_CHECKPOINT"].append(result, "count");
                        status["FT_DISK_FLUSH_LEAF_TOKUTIME_FOR_CHECKPOINT"].append(result, "time");
                        {
                            NestedBuilder _n4(result, "bytes");
                            status["FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT"].append(result, "uncompressed", scale);
                            status["FT_DISK_FLUSH_LEAF_BYTES_FOR_CHECKPOINT"].append(result, "compressed", scale);
                        }
                    }
                }
            }
            {
                NestedBuilder _n1(result, "serializeTime");
                {
                    NestedBuilder _n2(result, "nonleaf");
                    status["FT_NONLEAF_SERIALIZE_TOKUTIME"].append(result, "serialize");
                    status["FT_NONLEAF_COMPRESS_TOKUTIME"].append(result, "compress");
                    status["FT_NONLEAF_DECOMPRESS_TOKUTIME"].append(result, "decompress");
                    status["FT_NONLEAF_DESERIALIZE_TOKUTIME"].append(result, "deserialize");
                }
                {
                    NestedBuilder _n2(result, "leaf");
                    status["FT_LEAF_SERIALIZE_TOKUTIME"].append(result, "serialize");
                    status["FT_LEAF_COMPRESS_TOKUTIME"].append(result, "compress");
                    status["FT_LEAF_DECOMPRESS_TOKUTIME"].append(result, "decompress");
                    status["FT_LEAF_DESERIALIZE_TOKUTIME"].append(result, "deserialize");
                }
            }
            {
                NestedBuilder _n1(result, "locktree");
                {
                    NestedBuilder _n2(result, "size");
                    status["LTM_SIZE_CURRENT"].append(result, "current", scale);
                    status["LTM_SIZE_LIMIT"].append(result, "limit", scale);
                }
            }
            {
                NestedBuilder _n1(result, "compressionRatio");
                status["FT_DISK_FLUSH_LEAF_COMPRESSION_RATIO"].append(result, "leaf");
                status["FT_DISK_FLUSH_NONLEAF_COMPRESSION_RATIO"].append(result, "nonleaf");
                status["FT_DISK_FLUSH_OVERALL_COMPRESSION_RATIO"].append(result, "overall");
            }
            {
                NestedBuilder _n1(result, "alerts");
                status["LTM_LOCK_REQUESTS_PENDING"].append(result, "locktreeRequestsPending");
                status["CP_CHECKPOINT_COUNT_FAIL"].append(result, "checkpointFailures");
                {
                    NestedBuilder _n2(result, "panic");
                    status["PANIC"].append(result, "code");
                    status["PANIC_STRING"].append(result, "msg");
                }
                {
                    NestedBuilder _n2(result, "filesystem");
                    status["FS_ENOSPC_REDZONE_STATE"].append(result, "redzone");
                    status["FS_ENOSPC_THREADS_BLOCKED"].append(result, "currentBlockedThreads");
                }
                {
                    NestedBuilder _n2(result, "longWaitEvents");
                    status["LOGGER_WAIT_BUF_LONG"].append(result, "logBufferWait");
                    {
                        NestedBuilder _n3(result, "fsync");
                        status["FS_LONG_FSYNC_COUNT"].append(result, "count");
                        status["FS_LONG_FSYNC_TIME"].append(result, "time");
                    }
                    {
                        NestedBuilder _n3(result, "cachePressure");
                        status["CT_LONG_WAIT_PRESSURE_COUNT"].append(result, "count");
                        status["CT_LONG_WAIT_PRESSURE_TIME"].append(result, "time");
                    }
                    {
                        NestedBuilder _n3(result, "checkpointBegin");
                        status["CP_LONG_BEGIN_COUNT"].append(result, "count");
                        status["CP_LONG_BEGIN_TIME"].append(result, "time");
                    }
                    {
                        NestedBuilder _n3(result, "locktreeWait");
                        status["LTM_LONG_WAIT_COUNT"].append(result, "count");
                        status["LTM_LONG_WAIT_TIME"].append(result, "time");
                    }
                    {
                        NestedBuilder _n3(result, "locktreeWaitEscalation");
                        status["LTM_LONG_WAIT_ESCALATION_COUNT"].append(result, "count");
                        status["LTM_LONG_WAIT_ESCALATION_TIME"].append(result, "time");
                    }
                }
            }

            return result.b().obj();
        }
    } tokuftServerStatusSection;

}
