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

#include "mongo/base/init.h"
#include "mongo/db/global_environment_experiment.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary_options.h"
#include "mongo/db/storage/tokuft/tokuft_engine.h"
#include "mongo/db/storage/tokuft/tokuft_global_options.h"
#include "mongo/util/log.h"

namespace mongo {

    class TokuFTStorageEngine : public KVStorageEngine {
        MONGO_DISALLOW_COPYING(TokuFTStorageEngine);

        bool _durable;

    public:
        TokuFTStorageEngine(const std::string &path, bool durable, const KVStorageEngineOptions &options)
            : KVStorageEngine(new TokuFTEngine(path), options),
              _durable(durable)
        {
            if (!_durable) {
                warning() << "TokuFT: Initializing with --nojournal.  Note that this will cause {j: true} writes to fail, but will not actually disable journaling.";
                warning() << "TokuFT: This is only for tests, there is no reason to run with --nojournal in production.";
            }
        }

        // Even though the engine is always durable, we sometimes need to fake that we aren't for tests.  SERVER-15942
        virtual bool isDurable() const { return _durable; }

        virtual ~TokuFTStorageEngine() { }
    };

    class TokuFTFactory : public StorageEngine::Factory {
    public:
        virtual ~TokuFTFactory() { }
        virtual StorageEngine *create(const StorageGlobalParams &params,
                                      const StorageEngineLockFile &lockFile) const {
            if (params.directoryperdb) {
                severe() << "TokuFT: directoryPerDB not yet supported.  This option is incompatible with TokuFT.";
                severe() << "TokuFT: The following server crash is intentional.";
                fassertFailed(28610);
            }
            if (tokuftGlobalOptions.engineOptions.directoryForIndexes) {
                severe() << "TokuFT: directoryForIndexes not yet supported.  This option is incompatible with TokuFT.";
                severe() << "TokuFT: The following server crash is intentional.";
                fassertFailed(28611);
            }

            KVStorageEngineOptions options;
            options.directoryPerDB = params.directoryperdb;
            options.directoryForIndexes = tokuftGlobalOptions.engineOptions.directoryForIndexes;
            options.forRepair = params.repair;
            return new TokuFTStorageEngine(params.dbpath, params.dur, options);
        }
        virtual StringData getCanonicalName() const {
            return "tokuft";
        }
        virtual Status validateCollectionStorageOptions(const BSONObj& options) const {
            return TokuFTDictionaryOptions::validateOptions(options);
        }
        virtual Status validateIndexStorageOptions(const BSONObj& options) const {
            return TokuFTDictionaryOptions::validateOptions(options);
        }
        virtual Status validateMetadata(const StorageEngineMetadata& metadata,
                                        const StorageGlobalParams& params) const {
            Status status = metadata.validateStorageEngineOption(
                "directoryPerDB", params.directoryperdb);
            if (!status.isOK()) {
                return status;
            }

            status = metadata.validateStorageEngineOption(
                "directoryForIndexes", tokuftGlobalOptions.engineOptions.directoryForIndexes);
            if (!status.isOK()) {
                return status;
            }

            return Status::OK();
        }
        virtual BSONObj createMetadataOptions(const StorageGlobalParams& params) const {
            BSONObjBuilder builder;
            builder.appendBool("directoryPerDB", params.directoryperdb);
            builder.appendBool("directoryForIndexes",
                               tokuftGlobalOptions.engineOptions.directoryForIndexes);
            return builder.obj();
        }
    };

    MONGO_INITIALIZER_WITH_PREREQUISITES(TokuFTStorageEngineInit,
                                         ("SetGlobalEnvironment"))
                                         (InitializerContext *context) {
        getGlobalEnvironment()->registerStorageEngine("tokuft", new TokuFTFactory());
        return Status::OK();
    }

} // namespace mongo
