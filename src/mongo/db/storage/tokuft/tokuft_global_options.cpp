// tokuft_global_options.cpp

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
#include "mongo/db/storage/tokuft/tokuft_global_options.h"

namespace mongo {

    Status TokuFTGlobalOptions::add(moe::OptionSection* options) {
        Status s = engineOptions.add(options);
        if (!s.isOK()) {
            return s;
        }
        s = collectionOptions.add(options, "storage.tokuft.collectionOptions", "tokuftCollection", "collection");
        if (!s.isOK()) {
            return s;
        }
        s = indexOptions.add(options, "storage.tokuft.indexOptions", "tokuftIndex", "index");
        if (!s.isOK()) {
            return s;
        }

        return Status::OK();
    }

    bool TokuFTGlobalOptions::handlePreValidation(const moe::Environment& params) {
        return engineOptions.handlePreValidation(params) &&
                collectionOptions.handlePreValidation(params, "storage.tokuft.collectionOptions") &&
                indexOptions.handlePreValidation(params, "storage.tokuft.indexOptions");
    }

    Status TokuFTGlobalOptions::store(const moe::Environment& params,
                                 const std::vector<std::string>& args) {
        Status s = tokuftGlobalOptions.engineOptions.store(params, args);
        if (!s.isOK()) {
            return s;
        }

        s = tokuftGlobalOptions.collectionOptions.store(params, args, "storage.tokuft.collectionOptions");
        if (!s.isOK()) {
            return s;
        }

        s = tokuftGlobalOptions.indexOptions.store(params, args, "storage.tokuft.indexOptions");
        if (!s.isOK()) {
            return s;
        }

        return Status::OK();
    }
}
