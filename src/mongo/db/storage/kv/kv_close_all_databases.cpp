// kv_close_all_databases.cpp

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

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/lock_state.h"
#include "mongo/db/curop.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/kv/kv_close_all_databases.h"
#include "mongo/db/storage/recovery_unit_noop.h"

namespace mongo {

    /**
     * Because we don't get an OperationContext in the shutdown path anymore, we have to convince
     * DatabaseHolder::closeAllDatabases that we do have one and it has a global lock.  Since we're
     * in shutdown, we know this thread does have the lock, but it isn't associated with any
     * OperationContext.  So here are a fake Locker and OperationContext that pretend they do have
     * the lock and don't do much else.
     */
    class LockerImplShutdown : public DefaultLockerImpl {
    public:
        LockerImplShutdown() : DefaultLockerImpl() {}
        virtual ~LockerImplShutdown() {}
        virtual bool isW() const { return true; }
        virtual bool isR() const { return true; }
        virtual bool hasAnyReadLock() const { return true; }

        virtual bool isLocked() const { return true; }
        virtual bool isWriteLocked() const { return true; }
        virtual bool isWriteLocked(StringData ns) const { return true; }

        virtual void assertWriteLocked(StringData ns) const {}
    };

    class OperationContextShutdown : public OperationContext {
    public:
        OperationContextShutdown() {
            _recoveryUnit.reset(new RecoveryUnitNoop());
        }

        virtual ~OperationContextShutdown() { }

        virtual Client* getClient() const {
            invariant(false);
            return NULL;
        }

        virtual CurOp* getCurOp() const {
            invariant(false);
            return NULL;
        }

        virtual RecoveryUnit* recoveryUnit() const {
            return _recoveryUnit.get();
        }

        virtual RecoveryUnit* releaseRecoveryUnit() {
            return _recoveryUnit.release();
        }

        virtual void setRecoveryUnit(RecoveryUnit* unit) {
            _recoveryUnit.reset(unit);
        }

        virtual Locker* lockState() const {
            static LockerImplShutdown lk;
            return &lk;
        }

        virtual ProgressMeter* setMessage(const char * msg,
                                          const std::string &name,
                                          unsigned long long progressMeterTotal,
                                          int secondsBetween) {
            return &_pm;
        }

        virtual void checkForInterrupt() const { }
        virtual Status checkForInterruptNoAssert() const {
            return Status::OK();
        }

        virtual bool isPrimaryFor(StringData ns) {
            return true;
        }

        virtual bool isGod() const {
            return false;
        }

        virtual std::string getNS() const {
            return std::string();
        };

        virtual unsigned int getOpID() const {
            return 0;
        }

    private:
        std::auto_ptr<RecoveryUnit> _recoveryUnit;
        ProgressMeter _pm;
    };

    void closeAllDatabasesWrapper() {
        OperationContextShutdown txn;
        BSONObjBuilder closeResult;
        dbHolder().closeAll(&txn, closeResult, true);
    }

}
