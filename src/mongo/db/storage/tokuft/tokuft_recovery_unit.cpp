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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/storage/tokuft/tokuft_recovery_unit.h"
#include "mongo/util/log.h"

#include <ftcxx/db_env.hpp>
#include <ftcxx/db_txn.hpp>

namespace mongo {

    TokuFTRecoveryUnit::TokuFTRecoveryUnit(const ftcxx::DBEnv &env) :
        // We use depth to track transaction nesting
        _env(env), _txn(), _depth(0) {
    }

    TokuFTRecoveryUnit::~TokuFTRecoveryUnit() {
        invariant(_depth == 0);
        invariant(_changes.size() == 0);
        invariant(_txn.txn() == NULL);
    }

    void TokuFTRecoveryUnit::beginUnitOfWork() {
        if (_depth++ == 0) {
            invariant(_txn.txn() == NULL);
            _txn = ftcxx::DBTxn(_env, DB_SERIALIZABLE);
        }
    }

    void TokuFTRecoveryUnit::commitUnitOfWork() {
        invariant(_depth > 0);

        if (_depth > 1) {
            return;
        }

        for (Changes::iterator it = _changes.begin(), end = _changes.end();
             it != end; ++it) {
            (*it)->commit();
        }
        _changes.clear();

        invariant(_txn.txn() != NULL);
        _txn.commit(DB_TXN_NOSYNC);
        _txn = ftcxx::DBTxn(_env, DB_SERIALIZABLE);
    }

    void TokuFTRecoveryUnit::commitAndRestart() {
        invariant(_depth == 0);
        invariant(_changes.size() == 0);
        invariant(_txn.txn() == NULL);
        // no-op since we have no transaction
    }

    void TokuFTRecoveryUnit::endUnitOfWork() {
        invariant(_depth > 0);

        if (--_depth > 0) {
            return;
        }

        for (Changes::reverse_iterator it = _changes.rbegin(), end = _changes.rend();
             it != end; ++it) {
            (*it)->rollback();
        }
        _changes.clear();

        invariant(_txn.txn() != NULL);
        _txn.abort();
        _txn = ftcxx::DBTxn();
    }

    bool TokuFTRecoveryUnit::awaitCommit() {
        invariant(_env.env() != NULL);

        // The underlying transaction needs to have been committed to
        // the ydb environment, otherwise it's still provisional and
        // cannot be guaranteed durable even after a sync to the log.
        invariant(_txn.txn() == NULL);

        // Once the log is synced, the transaction is fully durable.
        const int r = _env.env()->log_flush(_env.env(), NULL);
        return r == 0;
    }

    void TokuFTRecoveryUnit::registerChange(RecoveryUnit::Change *change) {
        _changes.push_back(ChangePtr(change));
    }

    //
    // The remaining methods probably belong on DurRecoveryUnit rather than on the interface.
    //
    
    void *TokuFTRecoveryUnit::writingPtr(void *data, size_t len) {
        log() << "tokuft-engine: writingPtr does nothing" << std::endl;
        return data;
    }

}  // namespace mongo
