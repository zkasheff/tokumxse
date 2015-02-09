// tokuft_dictionary_options.cpp

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

#include <cctype>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary_options.h"

namespace mongo {

    TokuFTDictionaryOptions::TokuFTDictionaryOptions(const std::string& objectName)
        : _objectName(objectName),
          pageSize(4 << 20),
          readPageSize(64 << 10),
          compression("zlib"),
          fanout(16)
    {}

    namespace {
        static std::string capitalize(const std::string &str) {
            return str::stream() << (char) toupper(str[0]) << str.substr(1);
        }
    }

    std::string TokuFTDictionaryOptions::optionName(const std::string& opt) const {
        return str::stream() << "storage.tokuft." << _objectName << "Options." << opt;
    }

    std::string TokuFTDictionaryOptions::shortOptionName(const std::string& opt) const {
        return str::stream() << "tokuft" << capitalize(_objectName) << capitalize(opt);
    }

    Status TokuFTDictionaryOptions::add(moe::OptionSection* options) {
        moe::OptionSection tokuftOptions(str::stream() << "TokuFT " << _objectName << " options");

        tokuftOptions.addOptionChaining(optionName("pageSize"),
                shortOptionName("pageSize"), moe::UnsignedLongLong, str::stream() << "TokuFT " << _objectName << " page size");
        tokuftOptions.addOptionChaining(optionName("readPageSize"),
                shortOptionName("readPageSize"), moe::UnsignedLongLong, str::stream() << "TokuFT " << _objectName << " read page size");
        tokuftOptions.addOptionChaining(optionName("compression"),
                shortOptionName("compression"), moe::String, str::stream() << "TokuFT " << _objectName << " compression method (none, zlib, lzma, or quicklz)");
        tokuftOptions.addOptionChaining(optionName("fanout"),
                shortOptionName("fanout"), moe::Int, str::stream() << "TokuFT " << _objectName << " fanout");

        return options->addSection(tokuftOptions);
    }

    bool TokuFTDictionaryOptions::handlePreValidation(const moe::Environment& params) {
        return true;
    }

    Status TokuFTDictionaryOptions::store(const moe::Environment& params,
                                          const std::vector<std::string>& args) {
        if (params.count(optionName("pageSize"))) {
            pageSize = params[optionName("pageSize")].as<unsigned long long>();
            if (pageSize <= 0) {
                StringBuilder sb;
                sb << optionName("pageSize") << " must be > 0, but attempted to set to: "
                   << pageSize;
                return Status(ErrorCodes::BadValue, sb.str());
            }
        }
        if (params.count(optionName("readPageSize"))) {
            readPageSize = params[optionName("readPageSize")].as<unsigned long long>();
            if (readPageSize <= 0) {
                StringBuilder sb;
                sb << optionName("readPageSize") << " must be > 0, but attempted to set to: "
                   << readPageSize;
                return Status(ErrorCodes::BadValue, sb.str());
            }
        }
        if (params.count(optionName("compression"))) {
            compression = params[optionName("compression")].as<std::string>();
            if (compression != "zlib" &&
                compression != "quicklz" &&
                compression != "lzma" &&
                compression != "none") {
                StringBuilder sb;
                sb << optionName("compression") << " must be one of \"zlib\", \"quicklz\", \"lzma\", or \"none\", but attempted to set to: "
                   << compression;
                return Status(ErrorCodes::BadValue, sb.str());
            }
        }
        if (params.count(optionName("fanout"))) {
            fanout = params[optionName("fanout")].as<int>();
            if (fanout <= 0) {
                StringBuilder sb;
                sb << optionName("fanout") << " must be > 0, but attempted to set to: "
                   << fanout;
                return Status(ErrorCodes::BadValue, sb.str());
            }
        }

        return Status::OK();
    }

    BSONObj TokuFTDictionaryOptions::toBSON() const {
        BSONObjBuilder b;
        b.appendNumber("pageSize", static_cast<long long>(pageSize));
        b.appendNumber("readPageSize", static_cast<long long>(readPageSize));
        b.append("compression", compression);
        b.appendNumber("fanout", fanout);
        return b.obj();
    }

    Status TokuFTDictionaryOptions::validateOptions(const BSONObj& options) {
        std::set<std::string> found;
        BSONForEach(elem, options.getObjectField("tokuft")) {
            std::string name(elem.fieldName());
            if (found.find(name) != found.end()) {
                StringBuilder sb;
                sb << "TokuFT: Duplicated dictionary options field \"" << name << "\" in " << options;
                return Status(ErrorCodes::BadValue, sb.str());
            }
            found.insert(name);
            if (name == "pageSize" || name == "readPageSize" || name == "fanout") {
                if (!elem.isNumber()) {
                    StringBuilder sb;
                    sb << "TokuFT: Expected number type for \"" << name << "\" in dictionary options "
                       << options;
                    return Status(ErrorCodes::BadValue, sb.str());
                }
                if (elem.type() == NumberDouble &&
                    (elem.numberDouble() - elem.numberLong()) > 0) {
                    StringBuilder sb;
                    sb << "TokuFT: Dictionary options field \"" << name << "\" must be a whole number in options "
                       << options;
                    return Status(ErrorCodes::BadValue, sb.str());
                }
                long long val = elem.numberLong();
                if (val <= 0) {
                    StringBuilder sb;
                    sb << "TokuFT: Dictionary options field \"" << name << "\" must be positive in options "
                       << options;
                    return Status(ErrorCodes::BadValue, sb.str());
                }
            } else if (name == "compression") {
                if (elem.type() != mongo::String) {
                    StringBuilder sb;
                    sb << "TokuFT: \"compression\" option must be a string in options "
                       << options;
                    return Status(ErrorCodes::BadValue, sb.str());
                }
                StringData val = elem.valueStringData();
                if (val != "zlib" &&
                    val != "quicklz" &&
                    val != "lzma" &&
                    val != "none") {
                    StringBuilder sb;
                    sb << "TokuFT: \"compression\" must be one of \"zlib\", \"quicklz\", \"lzma\", or \"none\", in options "
                       << options;
                    return Status(ErrorCodes::BadValue, sb.str());
                }
            } else {
                StringBuilder sb;
                sb << "TokuFT: Dictionary options contains unknown field \"" << name << "\" in options "
                   << options;
                return Status(ErrorCodes::BadValue, sb.str());
            }
        }
        return Status::OK();
    }

    TokuFTDictionaryOptions TokuFTDictionaryOptions::mergeOptions(const BSONObj& options) const {
        BSONObj tokuftOptions = options.getObjectField("tokuft");
        TokuFTDictionaryOptions merged(*this);
        if (tokuftOptions.hasField("pageSize")) {
            merged.pageSize = tokuftOptions["pageSize"].numberLong();
        }
        if (tokuftOptions.hasField("readPageSize")) {
            merged.readPageSize = tokuftOptions["readPageSize"].numberLong();
        }
        if (tokuftOptions.hasField("compression")) {
            merged.compression = tokuftOptions["compression"].String();
        }
        if (tokuftOptions.hasField("fanout")) {
            merged.fanout = tokuftOptions["fanout"].numberLong();
        }
        LOG(1) << "TokuFT: Merged default options " << toBSON() << " with user options " << tokuftOptions << " to get " << merged.toBSON();
        return merged;
    }

    TOKU_COMPRESSION_METHOD TokuFTDictionaryOptions::compressionMethod() const {
        if (compression == "zlib") {
            return TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD;
        } else if (compression == "quicklz") {
            return TOKU_QUICKLZ_METHOD;
        } else if (compression == "lzma") {
            return TOKU_LZMA_METHOD;
        } else if (compression == "none") {
            return TOKU_NO_COMPRESSION;
        } else {
            invariant(false);
        }
    }

}
