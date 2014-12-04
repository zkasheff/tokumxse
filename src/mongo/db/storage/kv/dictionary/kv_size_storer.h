// kv_size_storer.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "mongo/base/string_data.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/util/string_map.h"

namespace mongo {

    class KVDictionary;
    class KVRecordStore;
    class RecoveryUnit;

    class KVSizeStorer {
    public:
        KVSizeStorer(KVDictionary *metadataDict, RecoveryUnit *ru);
        ~KVSizeStorer();

        void onCreate(RecordStore *rs, const StringData &ident,
                      long long nr, long long ds);
        void onDestroy(const StringData &ident,
                       long long nr, long long ds);

        void store(RecordStore *rs, const StringData& ident,
                   long long numRecords, long long dataSize);

        void load(const StringData& ident,
                  long long* numRecords, long long* dataSize) const;

        void loadFromDict(OperationContext *opCtx);
        void storeIntoDict(OperationContext *opCtx);

        void saveOnShutdown();

    private:
        void _checkMagic() const;

        struct Entry {
            Entry() : numRecords(0), dataSize(0), dirty(false), rs(NULL) {}
            long long numRecords;
            long long dataSize;
            bool dirty;
            RecordStore *rs;

            BSONObj serialize() const;
            Entry(const BSONObj &serialized);
        };

        void syncThread(RecoveryUnit *ru);

        int _magic;

        KVDictionary *_metadataDict;

        typedef std::map<std::string,Entry> Map;
        Map _entries;
        mutable boost::mutex _entriesMutex;

        volatile bool _syncRunning;
        volatile bool _syncTerminated;
        boost::mutex _syncMutex;
        boost::condition_variable _syncCond;
        boost::thread _syncThread;
    };

}
