// tokuft_disk_format.cpp

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

#include <ios>
#include <sstream>
#include <string>

#include <toku_config.h>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/bson/bson_field.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/storage/kv/slice.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary.h"
#include "mongo/db/storage/tokuft/tokuft_disk_format.h"
#include "mongo/util/log.h"
#include "mongo/util/version.h"

namespace mongo {

    namespace {

        std::string tokuftGitVersion() {
            std::stringstream ss;
            ss << std::hex << static_cast<unsigned long long>(TOKUDB_REVISION);
            return ss.str();
        }

    }

    const Slice TokuFTDiskFormatVersion::versionInfoKey("tokuftDiskFormatVersionInfo");
    const BSONField<int> TokuFTDiskFormatVersion::originalVersionField("originalVersion");
    const BSONField<int> TokuFTDiskFormatVersion::currentVersionField("currentVersion");
    const BSONField<BSONArray> TokuFTDiskFormatVersion::historyField("history");
    const BSONField<int> TokuFTDiskFormatVersion::upgradedToField("upgradedTo");
    const BSONField<Date_t> TokuFTDiskFormatVersion::upgradedAtField("upgradedAt");
    const BSONField<BSONObj> TokuFTDiskFormatVersion::upgradedByField("upgradedBy");
    const BSONField<std::string> TokuFTDiskFormatVersion::mongodbVersionField("mongodbVersion");
    const BSONField<std::string> TokuFTDiskFormatVersion::mongodbGitField("mongodbGitVersion");
    const BSONField<std::string> TokuFTDiskFormatVersion::tokuftGitField("tokuftGitVersion");
    const BSONField<std::string> TokuFTDiskFormatVersion::sysInfoField("sysInfo");

    TokuFTDiskFormatVersion::TokuFTDiskFormatVersion(KVDictionary *metadataDict)
        : _startupVersion(DISK_VERSION_INVALID),
          _currentVersion(DISK_VERSION_INVALID),
          _metadataDict(metadataDict)
    {}

    Status TokuFTDiskFormatVersion::initialize(OperationContext *opCtx) {
        BSONObj oldVersionObj;
        Status s = getInfo(opCtx, oldVersionObj);
        if (s == ErrorCodes::NoSuchKey) {
            BSONArrayBuilder ab;
            ab.append(BSON(upgradedToField(DISK_VERSION_CURRENT) <<
                           upgradedAtField(jsTime()) <<
                           upgradedByField(BSON(mongodbVersionField(versionString) <<
                                                mongodbGitField(gitVersion()) <<
                                                tokuftGitField(tokuftGitVersion()) <<
                                                sysInfoField(sysInfo())))));

            BSONObj versionObj = BSON(currentVersionField(DISK_VERSION_CURRENT) <<
                                      originalVersionField(DISK_VERSION_CURRENT) <<
                                      historyField(ab.arr()));

            s = _metadataDict->insert(opCtx, versionInfoKey, Slice::of(versionObj), false);
            if (!s.isOK()) {
                return s;
            }

            _startupVersion = DISK_VERSION_CURRENT;
        } else if (!s.isOK()) {
            return s;
        } else {
            long long llVersion;
            s = bsonExtractIntegerField(oldVersionObj, currentVersionField(), &llVersion);
            if (!s.isOK()) {
                return s;
            }
            _startupVersion = static_cast<VersionID>(llVersion);
        }
            
        if (_startupVersion < MIN_SUPPORTED_VERSION) {
            warning() << "Found unsupported disk format version: " << static_cast<int>(_startupVersion) << "." << startupWarningsLog;
            warning() << "The minimum supported disk format version by TokuFT is " << static_cast<int>(MIN_SUPPORTED_VERSION) << "." << startupWarningsLog;
            warning() << "Please use an earlier version of TokuFT to dump your data and reload it into this version." << startupWarningsLog;
            return Status(ErrorCodes::UnsupportedFormat, "version on disk too low");
        }

        if (_startupVersion > MAX_SUPPORTED_VERSION) {
            warning() << "Found unsupported disk format version: " << static_cast<int>(_startupVersion) << "." << startupWarningsLog;
            warning() << "The maximum supported disk format version by TokuFT is " << static_cast<int>(MAX_SUPPORTED_VERSION) << "." << startupWarningsLog;
            warning() << "Please upgrade to a later version of TokuFT to use the data on disk." << startupWarningsLog;
            return Status(ErrorCodes::UnsupportedFormat, "version on disk too high");
        }

        _currentVersion = _startupVersion;

        return Status::OK();
    }

    Status TokuFTDiskFormatVersion::upgradeToCurrent(OperationContext *opCtx) {
        Status s = Status::OK();
        if (_currentVersion < DISK_VERSION_CURRENT) {
            log() << "Need to upgrade from disk format version " << static_cast<int>(_currentVersion)
                  << " to " << static_cast<int>(DISK_VERSION_CURRENT) << ".";
        }
        while (_currentVersion < DISK_VERSION_CURRENT && s.isOK()) {
            s = upgradeToVersion(opCtx, static_cast<VersionID>(static_cast<int>(_currentVersion) + 1));
        }
        return s;
    }

    Status TokuFTDiskFormatVersion::upgradeToVersion(OperationContext *opCtx, VersionID targetVersion) {
        if (_currentVersion + 1 != targetVersion) {
            return Status(ErrorCodes::BadValue, "bad version in upgrade");
        }

        log() << "Running upgrade of disk format version " << static_cast<int>(_currentVersion)
              << " to " << static_cast<int>(targetVersion);

        switch (targetVersion) {
            case DISK_VERSION_INVALID:
            case DISK_VERSION_1:
            case DISK_VERSION_NEXT: {
                warning() << "Should not be trying to upgrade to " << static_cast<int>(targetVersion) << startupWarningsLog;
                return Status(ErrorCodes::BadValue, "bad version in upgrade");
            }

            case DISK_VERSION_2: {
                // No-op, just serialize the version below.
                break;
            }

            case DISK_VERSION_3: {
                // We can't upgrade to version 3 from any previous version, since it changes the
                // index key format.
                invariant(false);
                break;
            }

            case DISK_VERSION_4: {
                // We can't upgrade to version 4 from any previous version, since it changes the
                // index key and RecordId format.
                invariant(false);
                break;
            }

            case DISK_VERSION_5: {
                // We can't upgrade to version 5 from any previous version, since it updates the
                // index key format with type bits.
                invariant(false);
                break;
            }

            case DISK_VERSION_6: {
                // We can't upgrade to version 6 from any previous version, since it changes the
                // dictionary-to-file mapping.
                invariant(false);
                break;
            }

        }

        BSONObj oldVersionObj;
        Status s = getInfo(opCtx, oldVersionObj);
        if (!s.isOK()) {
            return s;
        }

        BSONElement historyElt = oldVersionObj[historyField()];
        if (historyElt.type() != mongo::Array) {
            return Status(ErrorCodes::BadValue, "invalid version history field type");
        }
        BSONArrayBuilder ab;
        BSONForEach(e, historyElt.Obj()) {
            ab.append(e);
        }
        ab.append(BSON(upgradedToField(targetVersion) <<
                       upgradedAtField(jsTime()) <<
                       upgradedByField(BSON(mongodbVersionField(versionString) <<
                                            mongodbGitField(gitVersion()) <<
                                            tokuftGitField(tokuftGitVersion()) <<
                                            sysInfoField(sysInfo())))));

        long long originalVersion;
        s = bsonExtractIntegerField(oldVersionObj, originalVersionField(), &originalVersion);
        if (!s.isOK()) {
            return s;
        }
        BSONObj versionObj = BSON(currentVersionField(targetVersion) <<
                          originalVersionField(originalVersion) <<
                          historyField(ab.arr()));

        s = _metadataDict->insert(opCtx, versionInfoKey, Slice::of(versionObj), false);
        if (!s.isOK()) {
            return s;
        }

        _currentVersion = targetVersion;
        return Status::OK();
    }

    Status TokuFTDiskFormatVersion::getInfo(OperationContext *opCtx, BSONObj &b) const {
        Slice val;
        Status s = _metadataDict->get(opCtx, versionInfoKey, val, false);
        if (!s.isOK()) {
            return s;
        }
        b = val.as<BSONObj>().getOwned();
        return Status::OK();
    }

} // namespace mongo
