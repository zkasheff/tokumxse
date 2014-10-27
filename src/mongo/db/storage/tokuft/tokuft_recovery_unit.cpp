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

#include "mongo/db/concurrency/lock_mgr_defs.h"
#include "mongo/db/concurrency/lock_state.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/tokuft/tokuft_recovery_unit.h"
#include "mongo/util/log.h"

#include <ftcxx/db_env.hpp>
#include <ftcxx/db_txn.hpp>

namespace mongo {

    TokuFTRecoveryUnit::TokuFTRecoveryUnit(const ftcxx::DBEnv &env) :
        // We use depth to track transaction nesting
        _env(env), _txn(), _depth(0), _willCommit(false)
    {}

    TokuFTRecoveryUnit::~TokuFTRecoveryUnit() {
        // We should have a transaction iff the depth is non-zero
        invariant((_depth > 0) == (_txn.txn() != NULL));

        // Abort the underlying transaction if it is still live.
        if (_txn.txn() != NULL) {
            _finishUnitOfWork(false);
        }
    }

    void TokuFTRecoveryUnit::beginUnitOfWork() {
        if (_depth == 0) {
            if (_txn.txn() == NULL) {
                _txn = ftcxx::DBTxn(_env, DB_SERIALIZABLE);
            }
            _willCommit = false;
        }
        _depth++;
    }

    void TokuFTRecoveryUnit::_finishUnitOfWork(const bool commit) {
        invariant(_txn.txn() != NULL);

        // Don't bother to fsync on commit here since we commit the log
        // in the background on a user-defined period. If the caller wants
        // to force a log sync, they call RecoveryUnit::awaitCommit()
        if (commit) {
            _txn.commit(DB_TXN_NOSYNC);
        } else {
            _txn.abort();
        }

        // Apply and delete changes in forward ordering during
        // commit, and in reverse order during rollback.
        while (!_changes.empty()) {
            RecoveryUnit::Change *ch;
            if (commit) {
                ch = _changes.front();
                _changes.pop_front();
                ch->commit();
            } else {
                ch = _changes.back();
                _changes.pop_back();
                ch->rollback();
            }
            delete ch;
        }
    }

    void TokuFTRecoveryUnit::commitUnitOfWork() {
        invariant(_depth > 0);
        _willCommit = true;
    }

    void TokuFTRecoveryUnit::endUnitOfWork() {
        invariant(_depth > 0);
        if (--_depth == 0) {
            _finishUnitOfWork(_willCommit);
        }
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
        _changes.push_back(change);
    }

    //
    // The remaining methods probably belong on DurRecoveryUnit rather than on the interface.
    //
    
    void *TokuFTRecoveryUnit::writingPtr(void *data, size_t len) {
        log() << "tokuft-engine: writingPtr does nothing" << std::endl;
        return data;
    }

    void TokuFTRecoveryUnit::syncDataAndTruncateJournal() {
        log() << "tokuft-engine: syncDataAndTruncateJournal does nothing" << std::endl;
    }

    bool TokuFTRecoveryUnit::_opCtxIsWriting(OperationContext *opCtx) {
        const Locker *state = opCtx->lockState();
        const LockMode mode =
                (state == NULL
                 // Only for c++ tests, assume tests can do whatever without proper locks.
                 ? MODE_X
                 // We don't have the ns so just check the global resource, generally it should have
                 // the IX or IS lock.
                 : state->getLockMode(ResourceId(RESOURCE_GLOBAL, 1ULL)));
        return mode == MODE_IX || mode == MODE_X;
    }

    const ftcxx::DBTxn &TokuFTRecoveryUnit::txn(OperationContext *opCtx) {
        if (_txn.txn() == NULL) {
            // No txn exists yet, create one on-demand.
            // If locked for write, get a serializable txn, otherwise get a read-only one.
            const int flags = (_opCtxIsWriting(opCtx)
                               ? DB_SERIALIZABLE
                               : (DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY));
            _txn = ftcxx::DBTxn(_env, flags);
        }
        return _txn;
    }

}  // namespace mongo
