/**
 *    Copyright (C) 2014 MongoDB Inc.
 *    Copyright (C) 2014 Tokutek Inc.
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

#pragma once

#include <deque>

#include "mongo/db/storage/recovery_unit.h"

#include <boost/shared_ptr.hpp>
#include <ftcxx/db_env.hpp>
#include <ftcxx/db_txn.hpp>

namespace mongo {

    class OperationContext;
    class TokuFTStorageEngine;

    class TokuFTRecoveryUnit : public RecoveryUnit {
        MONGO_DISALLOW_COPYING(TokuFTRecoveryUnit);
    public:
        TokuFTRecoveryUnit(const ftcxx::DBEnv &env);

        virtual ~TokuFTRecoveryUnit();

        void beginUnitOfWork();

        void commitUnitOfWork();

        void endUnitOfWork();

        bool awaitCommit();

        void commitAndRestart();

        void registerChange(Change* change);

        //
        // The remaining methods probably belong on DurRecoveryUnit rather than on the interface.
        //

        void *writingPtr(void *data, size_t len);

    private:
        typedef boost::shared_ptr<Change> ChangePtr;
        typedef std::vector<ChangePtr> Changes;

        const ftcxx::DBEnv &_env;
        ftcxx::DBTxn _txn;

        int _depth;
        Changes _changes;

        static bool _opCtxIsWriting(OperationContext *opCtx);

    public:
        // -- TokuFT Specific

        DB_TXN *db_txn() const {
            return _txn.txn();
        }

        const ftcxx::DBTxn &txn(OperationContext *opCtx);
    };

}  // namespace mongo
