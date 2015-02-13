// tokuft_disk_format.h

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

#include "mongo/base/status.h"
#include "mongo/bson/bson_field.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/storage/kv/slice.h"

namespace mongo {

    class OperationContext;
    class KVDictionary;

    class TokuFTDiskFormatVersion {
      public:
        enum VersionID {
            DISK_VERSION_INVALID = 0,
            DISK_VERSION_1 = 1,  // Implicit version before we serialized version numbers
            DISK_VERSION_2 = 2,  // Initial prerelease version, BSON index keys, memcmp-able RecordIds
            DISK_VERSION_3 = 3,  // Use KeyString for index entries, incompatible with earlier versions
            DISK_VERSION_4 = 4,  // KeyString gained compressed format, RecordId also uses compressed format, incompatible with earlier versions
            DISK_VERSION_5 = 5,  // KeyString gained type bits, incompatible with earlier versions
            DISK_VERSION_6 = 6,  // Moved to multiple dictionaries per FT
            DISK_VERSION_NEXT,
            DISK_VERSION_CURRENT = DISK_VERSION_NEXT - 1,
            MIN_SUPPORTED_VERSION = DISK_VERSION_6,
            MAX_SUPPORTED_VERSION = DISK_VERSION_CURRENT,
            FIRST_SERIALIZED_VERSION = DISK_VERSION_2,
        };

      private:
        VersionID _startupVersion;
        VersionID _currentVersion;
        KVDictionary *_metadataDict;

        static const Slice versionInfoKey;
        static const BSONField<int> originalVersionField;
        static const BSONField<int> currentVersionField;
        static const BSONField<BSONArray> historyField;
        static const BSONField<int> upgradedToField;
        static const BSONField<Date_t> upgradedAtField;
        static const BSONField<BSONObj> upgradedByField;
        static const BSONField<std::string> mongodbVersionField;
        static const BSONField<std::string> mongodbGitField;
        static const BSONField<std::string> tokuftGitField;
        static const BSONField<std::string> sysInfoField;

        Status upgradeToVersion(OperationContext *opCtx, VersionID targetVersion);

      public:
        TokuFTDiskFormatVersion(KVDictionary *metadataDict);

        Status initialize(OperationContext *opCtx);

        Status upgradeToCurrent(OperationContext *opCtx);

        Status getInfo(OperationContext *opCtx, BSONObj &b) const;
    };

} // namespace mongo
