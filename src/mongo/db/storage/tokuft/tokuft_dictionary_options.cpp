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
                shortOptionName("compression"), moe::String, str::stream() << "TokuFT " << _objectName << " compression method (uncompressed, zlib, lzma, or quicklz)");
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
        }
        if (params.count(optionName("readPageSize"))) {
            readPageSize = params[optionName("readPageSize")].as<unsigned long long>();
        }
        if (params.count(optionName("compression"))) {
            compression = params[optionName("compression")].as<std::string>();
        }
        if (params.count(optionName("fanout"))) {
            fanout = params[optionName("fanout")].as<int>();
        }

        return Status::OK();
    }
}
