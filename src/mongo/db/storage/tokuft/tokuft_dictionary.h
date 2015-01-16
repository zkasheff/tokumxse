// tokuft_dictionary.h

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

#include <algorithm>

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/mutable/damage_vector.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary_update.h"
#include "mongo/db/storage/kv/slice.h"
#include "mongo/db/storage/record_data.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary_options.h"

#include <ftcxx/cursor.hpp>
#include <ftcxx/db.hpp>
#include <ftcxx/db_env.hpp>
#include <ftcxx/db_txn.hpp>
#include <ftcxx/slice.hpp>

namespace mongo {

    class BSONElement;
    class BSONObjBuilder;
    class RecordId;
    class IndexDescriptor;
    class OperationContext;
    class TokuFTDictionaryOptions;

    inline Slice ftslice2slice(const ftcxx::Slice &in) {
        return Slice(in.data(), in.size());
    }

    inline ftcxx::Slice slice2ftslice(const Slice &in) {
        return ftcxx::Slice(in.data(), in.size());
    }

    // This dictionary interface implementation powers default RecordStore
    // and SortedDataInterface implementations in src/mongo/db/storage/kv.
    class TokuFTDictionary : public KVDictionary {
    public:
        TokuFTDictionary(const ftcxx::DBEnv &env, const ftcxx::DBTxn &txn, const StringData &ident,
                         const KVDictionary::Encoding &enc, const TokuFTDictionaryOptions& options);

        class Encoding : public KVDictionary::Encoding {
        public:
            Encoding(const KVDictionary::Encoding &enc)
                : KVDictionary::Encoding(enc)
            {}

            Encoding(const ftcxx::Slice &serialized)
                : KVDictionary::Encoding(ftslice2slice(serialized))
            {}

            int operator()(const ftcxx::Slice &a, const ftcxx::Slice &b) const {
                return cmp(a, b);
            }

            static int cmp(const ftcxx::Slice &a, const ftcxx::Slice &b) {
                return KVDictionary::Encoding::cmp(ftslice2slice(a), ftslice2slice(b));
            }

            BSONObj extractKey(const ftcxx::Slice &key, const ftcxx::Slice &val) const {
                return KVDictionary::Encoding::extractKey(ftslice2slice(key), ftslice2slice(val));
            }

            RecordId extractRecordId(const ftcxx::Slice &s) const {
                return KVDictionary::Encoding::extractRecordId(ftslice2slice(s));
            }
        };

        class Cursor : public KVDictionary::Cursor {
        public:
            Cursor(const TokuFTDictionary &dict, OperationContext *txn, const Slice &key, const int direction = 1);

            Cursor(const TokuFTDictionary &dict, OperationContext *txn, const int direction = 1);

            virtual bool ok() const;

            virtual void seek(OperationContext *opCtx, const Slice &key);

            virtual void advance(OperationContext *opCtx);

            virtual Slice currKey() const;

            virtual Slice currVal() const;

        private:
            typedef ftcxx::BufferedCursor<TokuFTDictionary::Encoding, ftcxx::DB::NullFilter> FTCursor;
            FTCursor _cur;
            Slice _currKey;
            Slice _currVal;
            bool _ok;
        };

        virtual Status get(OperationContext *opCtx, const Slice &key, Slice &value, bool skipPessimisticLocking=false) const;

        virtual Status dupKeyCheck(OperationContext *opCtx, const Slice &lookupLeft, const Slice &lookupRight, const RecordId &id);
        virtual bool supportsDupKeyCheck() const {
            return true;
        }

        virtual Status insert(OperationContext *opCtx, const Slice &key, const Slice &value, bool skipPessimisticLocking);

        virtual bool updateSupported() const { return true; }

        virtual Status update(OperationContext *opCtx, const Slice &key, const Slice &oldValue,
                              const KVUpdateMessage &message) {
            return update(opCtx, key, message);
        }

        virtual Status update(OperationContext *opCtx, const Slice &key, const KVUpdateMessage &message);

        virtual Status remove(OperationContext *opCtx, const Slice &key);

        virtual KVDictionary::Cursor *getCursor(OperationContext *opCtx, const Slice &key, const int direction = 1) const;

        virtual KVDictionary::Cursor *getCursor(OperationContext *opCtx, const int direction = 1) const;

        virtual const char *name() const { return "tokuft"; }

        virtual KVDictionary::Stats getStats() const;
    
        virtual bool useExactStats() const { return true; }

        virtual bool appendCustomStats(OperationContext *opCtx, BSONObjBuilder* result, double scale ) const;

        virtual Status setCustomOption(OperationContext *opCtx, const BSONElement& option, BSONObjBuilder* info );

        virtual Status compact(OperationContext *opCtx);

        const ftcxx::DB &db() const { return _db; }

    private:
        Encoding encoding() const {
            return TokuFTDictionary::Encoding(_db.descriptor());
        }

        TokuFTDictionaryOptions _options;
        ftcxx::DB _db;
    };

} // namespace mongo
