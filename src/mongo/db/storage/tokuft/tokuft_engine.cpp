/**
 *    Copyright (C) 2014 MongoDB Inc.
 *    Copyright (C) 2014 Tokutek Inc.
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

#include "mongo/db/operation_context.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary_update.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary.h"
#include "mongo/db/storage/tokuft/tokuft_engine.h"
#include "mongo/db/storage/tokuft/tokuft_global_options.h"
#include "mongo/db/storage/tokuft/tokuft_recovery_unit.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/processinfo.h"

#include <ftcxx/db_env.hpp>
#include <ftcxx/db_env-inl.hpp>
#include <ftcxx/db.hpp>

namespace mongo {
    
    // TODO: Real error handling

    static const int env_mode = 0755;
    static const int env_flags = DB_INIT_LOCK | DB_INIT_MPOOL | DB_INIT_TXN | DB_CREATE |
        DB_PRIVATE | DB_INIT_LOG | DB_RECOVER;

    namespace {

        static int tokuft_bt_compare(const ftcxx::Slice &desc, const ftcxx::Slice &a, const ftcxx::Slice &b) {
            TokuFTDictionary::Comparator cmp(desc);
            return cmp(a, b);
        }

        static int tokuft_update(const ftcxx::Slice &desc,
                                 const ftcxx::Slice &key, const ftcxx::Slice &oldVal,
                                 const ftcxx::Slice &extra, ftcxx::SetvalFunc setval) {
            scoped_ptr<KVUpdateMessage> message(KVUpdateMessage::fromSerialized(ftslice2slice(extra)));
            Slice kvOldVal = ftslice2slice(oldVal);
            Slice kvNewVal;
            Status status = message->apply(kvOldVal, kvNewVal);
            invariant(status.isOK());

            // TODO: KVUpdateMessage should be able to specify that a key should be deleted.
            setval(slice2ftslice(kvNewVal));
            return 0;
        }

        static const char *getIndexName(DB *db) {
            if (db != NULL) {
                return db->get_dname(db);
            } else {
                return "$ydb_internal";
            }
        }

        static void prettyBounds(DB *db, const DBT *leftKey, const DBT *rightKey,
                                 BSONArrayBuilder &bounds, bool isString, bool isInteger) {
            if (leftKey->data == NULL) {
                bounds.append("-infinity");
            } else {
                if (isString) {
                    bounds.append(StringData(static_cast<char *>(leftKey->data), leftKey->size));
                } else if (isInteger) {
                    BSONObjBuilder b(bounds.subobjStart());
                    b.appendNumber("record_id", Slice(static_cast<char *>(leftKey->data), leftKey->size).as<uint64_t>());
                    b.doneFast();
                } else {
                    bounds.append(BSONObj((const char *)leftKey->data));
                }
            }

            if (rightKey->data == NULL) {
                bounds.append("+infinity");
            } else {
                if (isString) {
                    bounds.append(StringData(static_cast<char *>(rightKey->data), rightKey->size));
                } else if (isInteger) {
                    BSONObjBuilder b(bounds.subobjStart());
                    b.appendNumber("record_id", Slice(static_cast<char *>(rightKey->data), rightKey->size).as<uint64_t>());
                    b.doneFast();
                } else {
                    bounds.append(BSONObj((const char *)rightKey->data));
                }
            }
        }


        static int iterateTransactionsCallback(uint64_t txnid, uint64_t clientId,
                                               iterate_row_locks_callback iterateLocks,
                                               void *locksExtra, void *extra) {
            try {
                // We ignore clientId because txnid is sufficient for finding
                // the associated operation in db.currentOp()
                BSONObjBuilder status;
                status.appendNumber("txnid", txnid);
                BSONArrayBuilder locks(status.subarrayStart("rowLocks"));
                {
                    DB *db;
                    DBT leftKey, rightKey;
                    while (iterateLocks(&db, &leftKey, &rightKey, locksExtra) == 0) {
                        if (locks.len() + leftKey.size + rightKey.size > BSONObjMaxUserSize - 1024) {
                            // We're running out of space, better stop here.
                            locks.append("too many results to return");
                            break;
                        }
                        BSONObjBuilder rowLock(locks.subobjStart());
                        StringData indexName(getIndexName(db));
                        bool isMetadata = indexName == "tokuft.metadata";
                        bool isRecordStore = indexName.startsWith("collection");
                        rowLock.append("index", indexName);
                        BSONArrayBuilder bounds(rowLock.subarrayStart("bounds"));
                        prettyBounds(db, &leftKey, &rightKey, bounds, isMetadata, isRecordStore);
                        bounds.doneFast();
                        rowLock.doneFast();
                    }
                    locks.doneFast();
                }
                LOG(2) << "TokuFT: live transaction: " << status.done();
                return 0;
            } catch (const DBException &e) {
                warning() << "TokuFT: caught exception " << e.what() << " (code " << e.getCode() << ") in iterate transactions callback.";
                return -1;
            } catch (const std::exception &e) {
                warning() << "TokuFT: caught exception " << e.what() << " in iterate transactions callback.";
                return -1;
            } catch (...) {
                warning() << "TokuFT: caught unknown exception in iterate transactions callback.";
                return -1;
            }
        }

        static int pendingLockRequestsCallback(DB *db, uint64_t requestingTxnid,
                                               const DBT *leftKey, const DBT *rightKey,
                                               uint64_t blockingTxnid, uint64_t startTime,
                                               void *extra) {
            try {
                BSONObjBuilder status;
                StringData indexName(getIndexName(db));
                bool isMetadata = indexName == "tokuft.metadata";
                bool isRecordStore = indexName.startsWith("collection");
                status.append("index", indexName);
                status.appendNumber("requestingTxnid", requestingTxnid);
                status.appendNumber("blockingTxnid", blockingTxnid);
                status.appendDate("started", startTime);
                {
                    BSONArrayBuilder bounds(status.subarrayStart("bounds"));
                    prettyBounds(db, leftKey, rightKey, bounds, isMetadata, isRecordStore);
                    bounds.doneFast();
                }
                LOG(2) << "TokuFT: pending lock: " << status.done();
                return 0;
            } catch (const DBException &e) {
                warning() << "TokuFT: caught exception " << e.what() << " (code " << e.getCode() << ") in pending lock requests callback.";
                return -1;
            } catch (const std::exception &e) {
                warning() << "TokuFT: caught exception " << e.what() << " in pending lock requests callback.";
                return -1;
            } catch (...) {
                warning() << "TokuFT: caught unknown exception in pending lock requests callback.";
                return -1;
            }
        }

        static void lockNotGrantedCallback(DB *db, uint64_t requestingTxnid,
                                           const DBT *leftKey, const DBT *rightKey,
                                           uint64_t blockingTxnid) {
            try {
                if (!logger::globalLogDomain()->shouldLog(MONGO_LOG_DEFAULT_COMPONENT, LogstreamBuilder::severityCast(1))) {
                    return;
                }

                StringData indexName(getIndexName(db));
                bool isMetadata = indexName == "tokuft.metadata";
                bool isRecordStore = indexName.startsWith("collection");
                if (!isMetadata && !isRecordStore && !indexName.startsWith("index")) {
                    LOG(1) << "TokuFT: lock not granted on internal dictionary \"" << indexName << "\", will not attempt to decode.";
                    return;
                }

                BSONObjBuilder info;
                info.append("index", indexName);
                info.appendNumber("requestingTxnid", requestingTxnid);
                info.appendNumber("blockingTxnid", blockingTxnid);
                BSONArrayBuilder bounds(info.subarrayStart("bounds"));
                prettyBounds(db, leftKey, rightKey, bounds, isMetadata, isRecordStore);
                bounds.doneFast();
                LOG(1) << "TokuFT: lock not granted, details: " << info.done();

                if (!logger::globalLogDomain()->shouldLog(MONGO_LOG_DEFAULT_COMPONENT, LogstreamBuilder::severityCast(2))) {
                    return;
                }

                db->dbenv->iterate_live_transactions(db->dbenv, iterateTransactionsCallback, NULL);
                db->dbenv->iterate_pending_lock_requests(db->dbenv, pendingLockRequestsCallback, NULL);
            } catch (const DBException &e) {
                warning() << "TokuFT: caught exception " << e.what() << " (code " << e.getCode() << ") in lock not granted callback.";
            } catch (const std::exception &e) {
                warning() << "TokuFT: caught exception " << e.what() << " in lock not granted callback.";
            } catch (...) {
                warning() << "TokuFT: caught unknown exception in lock not granted callback.";
            }
        }

    }

    TokuFTEngine::TokuFTEngine(const std::string& path)
        : _env(nullptr),
          _metadataDict(nullptr)
    {
        const TokuFTEngineOptions& engineOptions = tokuftGlobalOptions.engineOptions;

        unsigned long long cacheSize = engineOptions.cacheSize;
        if (!cacheSize) {
            ProcessInfo pi;
            unsigned long long memSizeMB = pi.getMemSizeMB();
            cacheSize = (memSizeMB / 2) << 20;
        }

        uint32_t cacheSizeGB = cacheSize >> 30;
        uint32_t cacheSizeB = cacheSize & ~uint32_t(1<<30);

        // TODO: Lock wait timeout callback, lock killed callback
        // TODO: logdir
        ftcxx::DBEnvBuilder builder = ftcxx::DBEnvBuilder()
                .set_product_name("tokuft")
                .set_cachesize(cacheSizeGB, cacheSizeB)
                .checkpointing_set_period(engineOptions.checkpointPeriod)
                .cleaner_set_iterations(engineOptions.cleanerIterations)
                .cleaner_set_period(engineOptions.cleanerPeriod)
                .set_direct_io(engineOptions.directio)
                .set_fs_redzone(engineOptions.fsRedzone)
                .change_fsync_log_period(engineOptions.journalCommitInterval)
                .set_lock_wait_time_msec(engineOptions.lockTimeout)
                .set_lock_timeout_callback(lockNotGrantedCallback)
                .set_compress_buffers_before_eviction(engineOptions.compressBuffersBeforeEviction)
                .set_cachetable_bucket_mutexes(engineOptions.numCachetableBucketMutexes);

        if (engineOptions.locktreeMaxMemory) {
            builder.set_lock_wait_time_msec(engineOptions.locktreeMaxMemory);
        }

        LOG(1) << "TokuFT: opening environment at " << path;
        _env = builder
               .set_default_bt_compare(&ftcxx::wrapped_comparator<tokuft_bt_compare>)
               .set_update(&ftcxx::wrapped_updater<tokuft_update>)
               .open(path.c_str(), env_flags, env_mode);

        ftcxx::DBTxn txn(_env);
        _metadataDict.reset(new TokuFTDictionary(_env, txn, "tokuft.metadata", KVDictionary::Comparator::useMemcmp(),
                                                 tokuftGlobalOptions.collectionOptions));
        txn.commit();
    }

    TokuFTEngine::~TokuFTEngine() {}

    void TokuFTEngine::cleanShutdownImpl() {
        invariant(_env.env() != NULL);

        LOG(1) << "TokuFT: shutdown";

        _metadataDict.reset();
        _env.close();
    }

    static const ftcxx::DBTxn &_getDBTxn(OperationContext *opCtx) {
        TokuFTRecoveryUnit *ru = dynamic_cast<TokuFTRecoveryUnit *>(opCtx->recoveryUnit());
        invariant(ru != NULL);
        return ru->txn(opCtx);
    }

    RecoveryUnit *TokuFTEngine::newRecoveryUnit() {
        return new TokuFTRecoveryUnit(_env);
    }

    TokuFTDictionaryOptions TokuFTEngine::_createOptions(const BSONObj& options, bool isRecordStore) {
        return (isRecordStore
                ? tokuftGlobalOptions.collectionOptions
                : tokuftGlobalOptions.indexOptions).mergeOptions(options);
    }

    Status TokuFTEngine::createKVDictionary(OperationContext* opCtx,
                                            const StringData& ident,
                                            const KVDictionary::Comparator &cmp,
                                            const BSONObj& options,
                                            bool isRecordStore) {
        WriteUnitOfWork wuow(opCtx);
        TokuFTDictionary dict(_env, _getDBTxn(opCtx), ident, cmp, _createOptions(options, isRecordStore));
        invariant(dict.db().db() != NULL);
        wuow.commit();

        return Status::OK();
    }

    KVDictionary* TokuFTEngine::getKVDictionary(OperationContext* opCtx,
                                                const StringData& ident,
                                                const KVDictionary::Comparator &cmp,
                                                const BSONObj& options,
                                                bool isRecordStore,
                                                bool mayCreate) {
        // TODO: mayCreate
        return new TokuFTDictionary(_env, _getDBTxn(opCtx), ident, cmp, _createOptions(options, isRecordStore));
    }

    Status TokuFTEngine::dropKVDictionary( OperationContext* opCtx,
                                            const StringData& ident ) {
        invariant(ident.size() > 0);

        std::string identStr = ident.toString();
        const int r = _env.env()->dbremove(_env.env(), _getDBTxn(opCtx).txn(), identStr.c_str(), NULL, 0);
        if (r != 0) {
            return Status(ErrorCodes::InternalError,
                          str::stream() << "TokuFTEngine::dropKVDictionary - Not found "
                                        << ident);
        }
        invariant(r == 0);
        return Status::OK();
    }

    bool TokuFTEngine::hasIdent(OperationContext* opCtx, const StringData& ident) const {
        ftcxx::Slice key(ident.size() + 1);
        std::copy(ident.begin(), ident.end(), key.mutable_data());
        key.mutable_data()[ident.size()] = '\0';

        typedef ftcxx::BufferedCursor<TokuFTDictionary::Comparator, ftcxx::DB::NullFilter> DirectoryCursor;
        DirectoryCursor cur(_env.buffered_cursor(_getDBTxn(opCtx),
                                                 TokuFTDictionary::Comparator(KVDictionary::Comparator::useMemcmp()),
                                                 ftcxx::DB::NullFilter()));
        TokuFTDictionary::Comparator cmp(KVDictionary::Comparator::useMemcmp());
        cur.seek(key);
        while (cur.ok()) {
            ftcxx::Slice foundKey;
            ftcxx::Slice foundVal;
            cur.next(foundKey, foundVal);
            int c = cmp(foundKey, key);
            if (c == 0) {
                return true;
            } else if (c > 0) {
                break;
            }
        }
        return false;
    }

    std::vector<std::string> TokuFTEngine::getAllIdents(OperationContext *opCtx) const {
        std::vector<std::string> idents;

        ftcxx::Slice key;
        ftcxx::Slice val;
        typedef ftcxx::BufferedCursor<TokuFTDictionary::Comparator, ftcxx::DB::NullFilter> DirectoryCursor;
        for (DirectoryCursor cur(_env.buffered_cursor(_getDBTxn(opCtx),
                                                      TokuFTDictionary::Comparator(KVDictionary::Comparator::useMemcmp()),
                                                      ftcxx::DB::NullFilter()));
             cur.next(key, val); ) {
            if (key.size() == 0) {
                continue;
            }
            StringData filename(key.data(), key.size());

            // strip off the trailing '\0' in the key
            if (filename[filename.size() - 1] == 0) {
                filename = filename.substr(0, filename.size() - 1);
            }

            if (filename == "tokuft.metadata") {
                continue;
            }

            idents.push_back(filename.toString());
        }

        return idents;
    }

} // namespace mongo
