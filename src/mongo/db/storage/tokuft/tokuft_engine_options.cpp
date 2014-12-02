// tokuft_engine_options.cpp

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

#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/db/storage/tokuft/tokuft_engine_options.h"

namespace mongo {

    TokuFTEngineOptions::TokuFTEngineOptions()
        : cacheSize(0),
          checkpointPeriod(60),
          cleanerIterations(5),
          cleanerPeriod(2),
          directio(false),
          fsRedzone(5),
          journalCommitInterval(100),
          lockTimeout(100),
          locktreeMaxMemory(0),  // let this be the ft default, computed from cacheSize
          compressBuffersBeforeEviction(false),
          numCachetableBucketMutexes(1<<20)
    {}

    Status TokuFTEngineOptions::add(moe::OptionSection* options) {
        moe::OptionSection tokuftOptions("TokuFT engine options");

        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.cacheSize",
                "tokuftEngineCacheSize", moe::UnsignedLongLong, "TokuFT engine cache size (bytes)");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.checkpointPeriod",
                "tokuftEngineCheckpointPeriod", moe::Int, "TokuFT engine checkpoint period (s)");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.cleanerIterations",
                "tokuftEngineCleanerIterations", moe::Int, "TokuFT engine cleaner iterations");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.cleanerPeriod",
                "tokuftEngineCleanerPeriod", moe::Int, "TokuFT engine cleaner period (s)");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.directio",
                "tokuftEngineDirectio", moe::Bool, "TokuFT engine use Direct I/O");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.fsRedzone",
                "tokuftEngineFsRedzone", moe::Int, "TokuFT engine filesystem redzone");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.journalCommitInterval",
                "tokuftEngineJournalCommitInterval", moe::Int, "TokuFT engine journal commit interval (ms)");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.lockTimeout",
                "tokuftEngineLockTimeout", moe::Int, "TokuFT engine lock wait timeout (ms)");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.locktreeMaxMemory",
                "tokuftEngineLocktreeMaxMemory", moe::UnsignedLongLong, "TokuFT locktree size (bytes)");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.compressBuffersBeforeEviction",
                "tokuftEngineCompressBuffersBeforeEviction", moe::Bool, "TokuFT engine compress buffers before eviction");
        tokuftOptions.addOptionChaining("storage.tokuft.engineOptions.numCachetableBucketMutexes",
                "tokuftEngineNumCachetableBucketMutexes", moe::Int, "TokuFT engine num cachetable bucket mutexes");

        return options->addSection(tokuftOptions);
    }

    bool TokuFTEngineOptions::handlePreValidation(const moe::Environment& params) {
        return true;
    }

    Status TokuFTEngineOptions::store(const moe::Environment& params,
                                 const std::vector<std::string>& args) {
        if (params.count("storage.tokuft.engineOptions.cacheSize")) {
            cacheSize = params["storage.tokuft.engineOptions.cacheSize"].as<unsigned long long>();
        }
        if (params.count("storage.tokuft.engineOptions.checkpointPeriod")) {
            checkpointPeriod = params["storage.tokuft.engineOptions.checkpointPeriod"].as<int>();
        }
        if (params.count("storage.tokuft.engineOptions.cleanerIterations")) {
            cleanerIterations = params["storage.tokuft.engineOptions.cleanerIterations"].as<int>();
        }
        if (params.count("storage.tokuft.engineOptions.cleanerPeriod")) {
            cleanerPeriod = params["storage.tokuft.engineOptions.cleanerPeriod"].as<int>();
        }
        if (params.count("storage.tokuft.engineOptions.directio")) {
            directio = params["storage.tokuft.engineOptions.directio"].as<bool>();
        }
        if (params.count("storage.tokuft.engineOptions.fsRedzone")) {
            fsRedzone = params["storage.tokuft.engineOptions.fsRedzone"].as<int>();
        }
        if (params.count("storage.tokuft.engineOptions.journalCommitInterval")) {
            journalCommitInterval = params["storage.tokuft.engineOptions.journalCommitInterval"].as<int>();
        }
        if (params.count("storage.tokuft.engineOptions.lockTimeout")) {
            lockTimeout = params["storage.tokuft.engineOptions.lockTimeout"].as<int>();
        }
        if (params.count("storage.tokuft.engineOptions.locktreeMaxMemory")) {
            locktreeMaxMemory = params["storage.tokuft.engineOptions.locktreeMaxMemory"].as<unsigned long long>();
        }
        if (params.count("storage.tokuft.engineOptions.compressBuffersBeforeEviction")) {
            compressBuffersBeforeEviction = params["storage.tokuft.engineOptions.compressBuffersBeforeEviction"].as<bool>();
        }
        if (params.count("storage.tokuft.engineOptions.numCachetableBucketMutexes")) {
            numCachetableBucketMutexes = params["storage.tokuft.engineOptions.numCachetableBucketMutexes"].as<int>();
        }

        return Status::OK();
    }
}
