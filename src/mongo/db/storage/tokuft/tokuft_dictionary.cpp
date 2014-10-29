// tokuft_dictionary.cpp

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

#include "mongo/base/error_codes.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/kv/slice.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary.h"
#include "mongo/db/storage/tokuft/tokuft_recovery_unit.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/log.h"

#include <db.h>
#include <ftcxx/cursor.hpp>
#include <ftcxx/db.hpp>
#include <ftcxx/db_env.hpp>
#include <ftcxx/db_txn.hpp>
#include <ftcxx/exceptions.hpp>
#include <ftcxx/slice.hpp>
#include <ftcxx/stats.hpp>

namespace mongo {

    TokuFTDictionary::TokuFTDictionary(const ftcxx::DBEnv &env, const ftcxx::DBTxn &txn, const StringData &ident,
                                       const KVDictionary::Comparator &cmp)
        : _db(ftcxx::DBBuilder()
              // TODO: descriptor, options
              .set_descriptor(slice2ftslice(cmp.serialize()))
              .open(env, txn, ident.toString().c_str(), NULL,
                    DB_BTREE /* legacy flag */, DB_CREATE, 0644))
    {
        // TODO: Verify descriptor contents WRT *options, *desc
    }

    static const ftcxx::DBTxn &_getDBTxn(OperationContext *opCtx) {
        TokuFTRecoveryUnit *ru = dynamic_cast<TokuFTRecoveryUnit *>(opCtx->recoveryUnit());
        invariant(ru != NULL);
        return ru->txn();
    }

    static Status error2status(int r) {
        if (r == 0) {
            return Status::OK();
        }
        ftcxx::ft_exception ftexc(r);
        std::string errmsg = str::stream() << "TokuFT: " << ftexc.what();
        if (r == DB_KEYEXIST) {
            return Status(ErrorCodes::DuplicateKey, errmsg);
        } else if (r == DB_LOCK_DEADLOCK) {
            return Status(ErrorCodes::DeadLock, errmsg);
        } else if (r == DB_LOCK_NOTGRANTED) {
            return Status(ErrorCodes::LockTimeout, errmsg);
        } else if (r == DB_NOTFOUND) {
            return Status(ErrorCodes::NoSuchKey, errmsg);
        } else if (r == TOKUDB_OUT_OF_LOCKS) {
            return Status(ErrorCodes::LockFailed, errmsg);
        } else if (r == TOKUDB_DICTIONARY_TOO_OLD) {
            return Status(ErrorCodes::UnsupportedFormat, errmsg);
        } else if (r == TOKUDB_DICTIONARY_TOO_NEW) {
            return Status(ErrorCodes::UnsupportedFormat, errmsg);
        } else if (r == TOKUDB_MVCC_DICTIONARY_TOO_NEW) {
            return Status(ErrorCodes::NamespaceNotFound, errmsg);
        }

        return Status(ErrorCodes::InternalError,
                      str::stream() << "TokuFT: internal error code "
                                    << ftexc.code() << ": " << ftexc.what());
    }

    Status TokuFTDictionary::get(OperationContext *opCtx, const Slice &key, Slice &value) const {
        class Callback {
            Slice &_v;
        public:
            Callback(Slice &v) : _v(v) {}
            int operator()(const ftcxx::Slice &key, const ftcxx::Slice &val) {
                _v = ftslice2slice(val).owned();
                return 0;
            }
        } cb(value);

        int r = _db.getf_set(_getDBTxn(opCtx), slice2ftslice(key),
                             // TODO: No doc-level locking yet, so never take locks on read.
                             DB_PRELOCKED | DB_PRELOCKED_WRITE,
                             cb);
        return error2status(r);
    }

    Status TokuFTDictionary::insert(OperationContext *opCtx, const Slice &key, const Slice &value) {
        int r = _db.put(_getDBTxn(opCtx), slice2ftslice(key), slice2ftslice(value));
        return error2status(r);
    }

    Status TokuFTDictionary::update(OperationContext *opCtx, const Slice &key, const KVUpdateMessage &message) {
        Slice value = message.serialize();
        int r = _db.update(_getDBTxn(opCtx), slice2ftslice(key), slice2ftslice(value));
        return error2status(r);
    }

    Status TokuFTDictionary::remove(OperationContext *opCtx, const Slice &key) {
        int r = _db.del(_getDBTxn(opCtx), slice2ftslice(key));
        return error2status(r);
    }

    KVDictionary::Cursor *TokuFTDictionary::getCursor(OperationContext *opCtx, const int direction) const {
        return new Cursor(*this, opCtx, direction);
    }

    KVDictionary::Stats TokuFTDictionary::getStats() const {
        KVDictionary::Stats kvStats;
        ftcxx::Stats stats = _db.get_stats();
        kvStats.dataSize = stats.data_size;
        kvStats.storageSize = stats.file_size;
        kvStats.numKeys = stats.num_keys;
        return kvStats;
    }
    
    void TokuFTDictionary::appendCustomStats(OperationContext *opCtx, BSONObjBuilder* result, double scale ) const {
        // TODO: stat64
    }

    Status TokuFTDictionary::setCustomOption(OperationContext *opCtx, const BSONElement& option, BSONObjBuilder* info ) {
        StringData name = option.fieldName();
        if ( name == "usePowerOf2Sizes" ) {
            // we ignore, so just say ok
            return Status::OK();
        }

        // TODO: compression, page sizes, fanout

        return Status( ErrorCodes::InvalidOptions,
                       mongoutils::str::stream()
                       << "unknown custom option to TokuFT: "
                       << name );
    }

    Status TokuFTDictionary::compact(OperationContext *opCtx) {
        return Status::OK();
    }

    TokuFTDictionary::Cursor::Cursor(const TokuFTDictionary &dict, OperationContext *txn, const int direction)
        : _cur(dict.db().buffered_cursor(ftcxx::DBTxn(), // no concurrency yet, _getDBTxn(txn),
                                         dict.comparator(), ftcxx::DB::NullFilter(), 0, (direction == 1)))
    {
        advance();
    }

    TokuFTDictionary::Cursor::Cursor(const TokuFTDictionary &dict, OperationContext *txn, const Slice &leftKey, const Slice &rightKey, const int direction)
        : _cur(dict.db().buffered_cursor(ftcxx::DBTxn(), // no concurrency yet, _getDBTxn(txn),
                                         ftcxx::Slice(leftKey.data(), leftKey.size()), ftcxx::Slice(rightKey.data(), rightKey.size()),
                                         dict.comparator(), ftcxx::DB::NullFilter(),
                                         0, (direction == 1), false, true))
    {
        advance();
    }

    bool TokuFTDictionary::Cursor::ok() const {
        return _ok;
    }

    void TokuFTDictionary::Cursor::seek(const Slice &key) {
        _cur.seek(slice2ftslice(key));
        advance();
    }

    void TokuFTDictionary::Cursor::advance() {
        ftcxx::Slice key, val;
        _ok = _cur.next(key, val);
        if (_ok) {
            _currKey = ftslice2slice(key);
            _currVal = ftslice2slice(val);
        }
    }

    Slice TokuFTDictionary::Cursor::currKey() const {
        invariant(ok());
        return _currKey;
    }

    Slice TokuFTDictionary::Cursor::currVal() const {
        invariant(ok());
        return _currVal;
    }

} // namespace mongo
