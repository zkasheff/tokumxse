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

#pragma once

#include "mongo/db/storage/kv/dictionary/kv_engine_impl.h"

#include <ftcxx/db_env.hpp>

namespace mongo {

    class TokuFTEngine : public KVEngineImpl {
        MONGO_DISALLOW_COPYING(TokuFTEngine);
    public:
        // Opens or creates a storage engine environment at the given path
        TokuFTEngine(const std::string &path);
        virtual ~TokuFTEngine();

        virtual RecoveryUnit* newRecoveryUnit();

        virtual Status createKVDictionary( OperationContext* opCtx,
                                            const StringData& ident,
                                            const KVDictionary::Comparator &cmp );

        virtual KVDictionary* getKVDictionary( OperationContext* opCtx,
                                                const StringData& ident,
                                                const KVDictionary::Comparator &cmp,
                                                bool mayCreate = false );

        virtual Status dropKVDictionary( OperationContext* opCtx,
                                          const StringData& ident );

        virtual int64_t getIdentSize( OperationContext* opCtx,
                              const StringData& ident ) {
            return 1;
        }

        virtual Status repairIdent( OperationContext* opCtx,
                            const StringData& ident ) {
            return Status::OK();
        }

        // TODO: Take a checkpoint? Do we have to? What's the contract?
        //       Maybe doing nothing is sufficient.
        virtual int flushAllFiles( bool sync ) { return 0; }

        virtual bool isDurable() const { return true; }

        /**
         * TokuFT supports row-level ("document-level") locking.
         *
         * We disable it for now until the API is finalized.
         */
        virtual bool supportsDocLocking() const { return true; }

        // ------------------------------------------------------------------ //

        bool persistDictionaryStats() const { return true; }

        KVDictionary* getMetadataDictionary() {
            return _metadataDict.get();
        }

    private:
        ftcxx::DBEnv _env;
        scoped_ptr<KVDictionary> _metadataDict;
    };

} // namespace mongo
