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

    }

    TokuFTEngine::TokuFTEngine(const std::string &path)
        : _env(nullptr),
          _metadataDict(nullptr)
    {
        ProcessInfo pi;
        unsigned long long memSizeMB = pi.getMemSizeMB();
        size_t cacheSize = (memSizeMB / 2) * (1<<20);
        uint32_t cacheSizeGB = cacheSize >> 30;
        uint32_t cacheSizeB = cacheSize & ~uint32_t(1<<30);

        log() << "TokuFT: opening environment at " << path << std::endl;
        _env = ftcxx::DBEnvBuilder()
               // TODO: Direct I/O
               // TODO: Lock wait timeout callback, lock killed callback
               // TODO: redzone, logdir
               // TODO: cleaner period, cleaner iterations
               .set_cachesize(cacheSizeGB, cacheSizeB)
               .checkpointing_set_period(60)
               .change_fsync_log_period(100)
               .set_lock_wait_time_msec(4000)
               .set_default_bt_compare(&ftcxx::wrapped_comparator<tokuft_bt_compare>)
               .set_update(&ftcxx::wrapped_updater<tokuft_update>)
               .open(path.c_str(), env_flags, env_mode);

        ftcxx::DBTxn txn(_env);
        _metadataDict.reset(new TokuFTDictionary(_env, txn, "tokuft.metadata", KVDictionary::Comparator::useMemcmp()));
        txn.commit();
    }

    TokuFTEngine::~TokuFTEngine() {}

    void TokuFTEngine::cleanShutdownImpl(OperationContext *opCtx) {
        invariant(_env.env() != NULL);

        log() << "TokuFT: shutdown" << std::endl;

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

    Status TokuFTEngine::createKVDictionary( OperationContext* opCtx,
                                              const StringData& ident,
                                              const KVDictionary::Comparator &cmp ) {
        WriteUnitOfWork wuow(opCtx);
        TokuFTDictionary dict(_env, _getDBTxn(opCtx), ident, cmp);
        invariant(dict.db().db() != NULL);
        wuow.commit();

        return Status::OK();
    }

    KVDictionary* TokuFTEngine::getKVDictionary( OperationContext* opCtx,
                                                  const StringData& ident,
                                                  const KVDictionary::Comparator &cmp,
                                                  bool mayCreate ) {
        // TODO: mayCreate
        return new TokuFTDictionary(_env, _getDBTxn(opCtx), ident, cmp);
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

    std::vector<std::string> TokuFTEngine::getAllIdents(OperationContext *opCtx) const {
        std::vector<std::string> idents;

        ftcxx::Slice key;
        ftcxx::Slice val;
        typedef ftcxx::BufferedCursor<TokuFTDictionary::Comparator, ftcxx::DB::NullFilter> DirectoryCursor;
        for (DirectoryCursor cur(_env.buffered_cursor(_getDBTxn(opCtx),
                                                      TokuFTDictionary::Comparator(KVDictionary::Comparator::useMemcmp()),
                                                      ftcxx::DB::NullFilter()));
             cur.ok(); cur.next(key, val)) {
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
