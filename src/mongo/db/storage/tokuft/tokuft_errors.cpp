// tokuft_errors.cpp

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

#include "mongo/base/error_codes.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/tokuft/tokuft_errors.h"
#include "mongo/util/mongoutils/str.h"

#include <db.h>
#include <ftcxx/exceptions.hpp>

namespace mongo {

    Status statusFromTokuFTException(const ftcxx::ft_exception &ftexc) {
        int r = ftexc.code();
        std::string errmsg = str::stream() << "TokuFT: " << ftexc.what();
        if (r == DB_KEYEXIST) {
            return Status(ErrorCodes::DuplicateKey, errmsg);
        } else if (r == DB_LOCK_DEADLOCK) {
            throw WriteConflictException();
            //return Status(ErrorCodes::WriteConflict, errmsg);
        } else if (r == DB_LOCK_NOTGRANTED) {
            throw WriteConflictException();
            //return Status(ErrorCodes::LockTimeout, errmsg);
        } else if (r == DB_NOTFOUND) {
            return Status(ErrorCodes::NoSuchKey, errmsg);
        } else if (r == TOKUDB_OUT_OF_LOCKS) {
            throw WriteConflictException();
            //return Status(ErrorCodes::LockFailed, errmsg);
        } else if (r == TOKUDB_DICTIONARY_TOO_OLD) {
            return Status(ErrorCodes::UnsupportedFormat, errmsg);
        } else if (r == TOKUDB_DICTIONARY_TOO_NEW) {
            return Status(ErrorCodes::UnsupportedFormat, errmsg);
        } else if (r == TOKUDB_MVCC_DICTIONARY_TOO_NEW) {
            throw WriteConflictException();
            //return Status(ErrorCodes::NamespaceNotFound, errmsg);
        }

        return Status(ErrorCodes::InternalError,
                      str::stream() << "TokuFT: internal error code "
                      << ftexc.code() << ": " << ftexc.what());
    }

    Status statusFromTokuFTError(int r) {
        if (r == 0) {
            return Status::OK();
        }
        ftcxx::ft_exception ftexc(r);
        return statusFromTokuFTException(ftexc);
    }

}
