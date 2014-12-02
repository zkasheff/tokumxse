// tokuft_engine_server_parameters.cpp

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

#include "mongo/base/parse_number.h"
#include "mongo/base/status.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/db/global_environment_experiment.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/tokuft/tokuft_engine.h"
#include "mongo/db/storage/tokuft/tokuft_errors.h"
#include "mongo/db/storage/tokuft/tokuft_global_options.h"

namespace mongo {

    namespace {

        static ftcxx::DBEnv& globalEnv() {
            StorageEngine* storageEngine = getGlobalEnvironment()->getGlobalStorageEngine();
            massert(28577, "no storage engine available", storageEngine);
            KVStorageEngine* kvStorageEngine = dynamic_cast<KVStorageEngine*>(storageEngine);
            massert(28578, "storage engine is not a KVStorageEngine", kvStorageEngine);
            KVEngine* kvEngine = kvStorageEngine->getEngine();
            invariant(kvEngine);
            TokuFTEngine* tokuftEngine = dynamic_cast<TokuFTEngine*>(kvEngine);
            massert(28579, "storage engine is not TokuFT", tokuftEngine);
            return tokuftEngine->env();
        }

    }

    /**
     * Specify an integer between 0 and 300 signifying the number of milliseconds (ms)
     * between journal commits.
     */
    class TokuFTEngineJournalCommitIntervalSetting : public ServerParameter {
        Status adjust(uint32_t newVal) const {
            Status s = statusFromTokuFTError(globalEnv().change_fsync_log_period(newVal));
            if (s.isOK()) {
                tokuftGlobalOptions.engineOptions.journalCommitInterval = newVal;
            }
            return s;
        }

    public:
        TokuFTEngineJournalCommitIntervalSetting() :
            ServerParameter(ServerParameterSet::getGlobal(), "tokuftEngineJournalCommitInterval",
                    false, // allowedToChangeAtStartup
                    true // allowedToChangeAtRuntime
                    ) {}

        virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name) {
            b << name << tokuftGlobalOptions.engineOptions.journalCommitInterval;
        }

        virtual Status set(const BSONElement& newValueElement) {
            long long newValue;
            if (!newValueElement.isNumber()) {
                StringBuilder sb;
                sb << "Expected number type for tokuftEngineJournalCommitInterval via setParameter command: "
                   << newValueElement;
                return Status(ErrorCodes::BadValue, sb.str());
            }
            if (newValueElement.type() == NumberDouble &&
                (newValueElement.numberDouble() - newValueElement.numberLong()) > 0) {
                StringBuilder sb;
                sb << "tokuftEngineJournalCommitInterval must be a whole number: "
                   << newValueElement;
                return Status(ErrorCodes::BadValue, sb.str());
            }
            newValue = newValueElement.numberLong();
            if (newValue <= 0 || newValue >= 300) {
                StringBuilder sb;
                sb << "tokuftEngineJournalCommitInterval must be between 0 and 300, but attempted to set to: "
                   << newValue;
                return Status(ErrorCodes::BadValue, sb.str());
            }

            return adjust(static_cast<unsigned>(newValue));
        }

        virtual Status setFromString(const std::string& str) {
            unsigned newValue;
            Status status = parseNumberFromString(str, &newValue);
            if (!status.isOK()) {
                return status;
            }
            if (newValue <= 0 || newValue >= 300) {
                StringBuilder sb;
                sb << "tokuftEngineJournalCommitInterval must be between 0 and 300, but attempted to set to: "
                   << newValue;
                return Status(ErrorCodes::BadValue, sb.str());
            }

            return adjust(static_cast<unsigned>(newValue));
        }
    } tokuftEngineJournalCommitIntervalSetting;

}
