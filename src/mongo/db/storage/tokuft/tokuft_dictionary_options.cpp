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

#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/storage/tokuft/tokuft_dictionary_options.h"

namespace mongo {

    TokuFTDictionaryOptions::TokuFTDictionaryOptions()
        : pageSize(4 << 20),
          readPageSize(64 << 10),
          compression("zlib"),
          fanout(16)
    {}

    Status TokuFTDictionaryOptions::add(moe::OptionSection* options, const std::string& sectionName, const std::string& shortSectionName, const std::string& objectName) {
        moe::OptionSection tokuftOptions(str::stream() << "TokuFT " << objectName << " options");

        tokuftOptions.addOptionChaining(str::stream() << sectionName << ".pageSize",
                str::stream() << shortSectionName << "PageSize", moe::UnsignedLongLong, str::stream() << "TokuFT " << objectName << " page size");
        tokuftOptions.addOptionChaining(str::stream() << sectionName << ".readPageSize",
                str::stream() << shortSectionName << "ReadPageSize", moe::UnsignedLongLong, str::stream() << "TokuFT " << objectName << " read page size");
        tokuftOptions.addOptionChaining(str::stream() << sectionName << ".compression",
                str::stream() << shortSectionName << "Compression", moe::String, str::stream() << "TokuFT " << objectName << " compression method (uncompressed, zlib, lzma, or quicklz)");
        tokuftOptions.addOptionChaining(str::stream() << sectionName << ".fanout",
                str::stream() << shortSectionName << "Fanout", moe::Int, str::stream() << "TokuFT " << objectName << " fanout");

        return options->addSection(tokuftOptions);
    }

    bool TokuFTDictionaryOptions::handlePreValidation(const moe::Environment& params, const std::string& sectionName) {
        return true;
    }

    Status TokuFTDictionaryOptions::store(const moe::Environment& params,
                                          const std::vector<std::string>& args, const std::string& sectionName) {
        if (params.count(str::stream() << sectionName << ".pageSize")) {
            pageSize = params[str::stream() << sectionName << ".pageSize"].as<unsigned long long>();
        }
        if (params.count(str::stream() << sectionName << ".readPageSize")) {
            readPageSize = params[str::stream() << sectionName << ".readPageSize"].as<unsigned long long>();
        }
        if (params.count(str::stream() << sectionName << ".compression")) {
            compression = params[str::stream() << sectionName << ".compression"].as<std::string>();
        }
        if (params.count(str::stream() << sectionName << ".fanout")) {
            fanout = params[str::stream() << sectionName << ".fanout"].as<int>();
        }

        return Status::OK();
    }
}
