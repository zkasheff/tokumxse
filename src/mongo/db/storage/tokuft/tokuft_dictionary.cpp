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

#include <algorithm>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/kv/dictionary/kv_sorted_data_impl.h"
#include "mongo/db/storage/kv/slice.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary_options.h"
#include "mongo/db/storage/tokuft/tokuft_errors.h"
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
                                       const KVDictionary::Encoding &enc, const TokuFTDictionaryOptions& options)
        : _options(options),
          _db(ftcxx::DBBuilder()
              .set_readpagesize(options.readPageSize)
              .set_pagesize(options.pageSize)
              .set_compression_method(options.compressionMethod())
              .set_fanout(options.fanout)
              .set_descriptor(slice2ftslice(enc.serialize()))
              .set_always_memcmp(true)
              .open(env, txn, ident.toString().c_str(), NULL,
                    DB_BTREE /* legacy flag */, DB_CREATE, 0644))
    {
        LOG(1) << "TokuFT: Opening dictionary \"" << ident << "\" with options " << options.toBSON();
    }

    namespace {

        TokuFTRecoveryUnit *_getTokuRU(OperationContext *opCtx) {
            TokuFTRecoveryUnit *ru = dynamic_cast<TokuFTRecoveryUnit *>(opCtx->recoveryUnit());
            invariant(ru != NULL);
            return ru;
        }

        bool _isReplicaSetSecondary(OperationContext *opCtx) {
            return _getTokuRU(opCtx)->isReplicaSetSecondary();
        }

        const ftcxx::DBTxn &_getDBTxn(OperationContext *opCtx) {
            return _getTokuRU(opCtx)->txn(opCtx);
        }

        int _getWriteFlags(OperationContext *opCtx, bool skipPessimisticLocking=false) {
            return (skipPessimisticLocking || _isReplicaSetSecondary(opCtx))
                    ? DB_PRELOCKED_WRITE
                    : 0;
        }

        int _getReadFlags(OperationContext *opCtx) {
            return (_getDBTxn(opCtx).is_read_only() || _isReplicaSetSecondary(opCtx))
                    ? DB_PRELOCKED | DB_PRELOCKED_WRITE
                    : 0;
        }

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

        const ftcxx::DBTxn &txn = _getDBTxn(opCtx);
        int r = _db.getf_set(txn, slice2ftslice(key), _getReadFlags(opCtx), cb);
        return statusFromTokuFTError(r);
    }

    class DupKeyFilter {
        const TokuFTDictionary::Encoding &_enc;
        RecordId _id;

    public:
        DupKeyFilter(const TokuFTDictionary::Encoding &enc, const RecordId &id)
            : _enc(enc),
              _id(id)
        {}

        bool operator()(const ftcxx::Slice &key, const ftcxx::Slice &val) const {
            // We are looking for cases where the RecordId *doesn't* match.  So if they're equal,
            // return false so we don't consider this key.
            return _id != _enc.extractRecordId(key);
        }
    };

    Status TokuFTDictionary::dupKeyCheck(OperationContext *opCtx, const Slice &lookupLeft, const Slice &lookupRight, const RecordId &id) {
        if (_isReplicaSetSecondary(opCtx)) {
            return Status::OK();
        }

        try {
            ftcxx::Slice foundKey;
            ftcxx::Slice foundVal;
            for (ftcxx::BufferedCursor<TokuFTDictionary::Encoding, DupKeyFilter> cur(
                     _db.buffered_cursor(_getDBTxn(opCtx), slice2ftslice(lookupLeft), slice2ftslice(lookupRight),
                                         encoding(), DupKeyFilter(encoding(), id), 0, true, false, true));
                 cur.ok(); cur.next(foundKey, foundVal)) {
                // If we found anything, it must have matched the filter, so it's a duplicate.
                return Status(ErrorCodes::DuplicateKey, "E11000 duplicate key error");
            }
        } catch (ftcxx::ft_exception &e) {
            return statusFromTokuFTException(e);
        }
        return Status::OK();
    }

    Status TokuFTDictionary::insert(OperationContext *opCtx, const Slice &key, const Slice &value, bool skipPessimisticLocking) {
        int r = _db.put(_getDBTxn(opCtx), slice2ftslice(key), slice2ftslice(value), _getWriteFlags(opCtx, skipPessimisticLocking));
        return statusFromTokuFTError(r);
    }

    Status TokuFTDictionary::update(OperationContext *opCtx, const Slice &key, const KVUpdateMessage &message) {
        Slice value = message.serialize();
        int r = _db.update(_getDBTxn(opCtx), slice2ftslice(key), slice2ftslice(value), _getWriteFlags(opCtx));
        return statusFromTokuFTError(r);
    }

    Status TokuFTDictionary::remove(OperationContext *opCtx, const Slice &key) {
        int r = _db.del(_getDBTxn(opCtx), slice2ftslice(key), _getWriteFlags(opCtx));
        return statusFromTokuFTError(r);
    }

    KVDictionary::Cursor *TokuFTDictionary::getCursor(OperationContext *opCtx, const Slice &key, const int direction) const {
        try {
            return new Cursor(*this, opCtx, key, direction);
        } catch (ftcxx::ft_exception &e) {
            // Will throw WriteConflictException if needed, discard status
            statusFromTokuFTException(e);
            // otherwise rethrow
            throw;
        }
    }

    KVDictionary::Cursor *TokuFTDictionary::getCursor(OperationContext *opCtx, const int direction) const {
        try {
            return new Cursor(*this, opCtx, direction);
        } catch (ftcxx::ft_exception &e) {
            // Will throw WriteConflictException if needed, discard status
            statusFromTokuFTException(e);
            // otherwise rethrow
            throw;
        }
    }

    KVDictionary::Stats TokuFTDictionary::getStats() const {
        KVDictionary::Stats kvStats;
        ftcxx::Stats stats = _db.get_stats();
        kvStats.dataSize = stats.data_size;
        kvStats.storageSize = stats.file_size;
        kvStats.numKeys = stats.num_keys;
        return kvStats;
    }

    bool TokuFTDictionary::appendCustomStats(OperationContext *opCtx, BSONObjBuilder* result, double scale ) const {
        BSONObjBuilder b(result->subobjStart("tokuft"));
        KVDictionary::Stats stats = getStats();
        {
            BSONObjBuilder sizeBuilder(b.subobjStart("size"));
            sizeBuilder.appendNumber("uncompressed", static_cast<long long>(stats.dataSize));
            sizeBuilder.appendNumber("compressed", static_cast<long long>(stats.storageSize));
            sizeBuilder.doneFast();
        }
        b.appendNumber("numElements", static_cast<long long>(stats.numKeys));

        b.append("createOptions", _options.toBSON());
        b.doneFast();
        return true;
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

    TokuFTDictionary::Cursor::Cursor(const TokuFTDictionary &dict, OperationContext *txn, const Slice &key, const int direction)
        : _cur(dict.db().buffered_cursor(_getDBTxn(txn), slice2ftslice(key),
                                         dict.encoding(), ftcxx::DB::NullFilter(), 0, (direction == 1))),
          _currKey(), _currVal(), _ok(false)
    {
        advance(txn);
    }

    TokuFTDictionary::Cursor::Cursor(const TokuFTDictionary &dict, OperationContext *txn, const int direction)
        : _cur(dict.db().buffered_cursor(_getDBTxn(txn),
                                         dict.encoding(), ftcxx::DB::NullFilter(), 0, (direction == 1))),
          _currKey(), _currVal(), _ok(false)
    {
        advance(txn);
    }

    bool TokuFTDictionary::Cursor::ok() const {
        return _ok;
    }

    void TokuFTDictionary::Cursor::seek(OperationContext *opCtx, const Slice &key) {
        _cur.set_txn(_getDBTxn(opCtx));
        try {
            _cur.seek(slice2ftslice(key));
        } catch (ftcxx::ft_exception &e) {
            // Will throw WriteConflictException if needed, discard status
            statusFromTokuFTException(e);
            // otherwise rethrow
            throw;
        }
        advance(opCtx);
    }

    void TokuFTDictionary::Cursor::advance(OperationContext *opCtx) {
        _cur.set_txn(_getDBTxn(opCtx));
        ftcxx::Slice key, val;
        try {
            _ok = _cur.next(key, val);
        } catch (ftcxx::ft_exception &e) {
            // Will throw WriteConflictException if needed, discard status
            statusFromTokuFTException(e);
            // otherwise rethrow
            throw;
        }
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
