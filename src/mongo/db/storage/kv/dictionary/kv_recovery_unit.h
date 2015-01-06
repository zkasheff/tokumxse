// kv_recovery_unit.h

/**
*    Copyright (C) 2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
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

#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"

namespace mongo {

    /**
     * The KVDictionary layer takes care of capped id management (visible_id_tracker.h), which
     * requires a few hooks in to the RecoveryUnit.  You only need to inherit from this if your
     * engine supports doc locking (if it doesn't, hasSnapshot is sort of nonsensical).
     */
    class KVRecoveryUnit : public RecoveryUnit {

        /**
         * The lowest invisible RecordId for this transaction.  Used by multiple RecordIterators
         * once one is established.
         */
        RecordId _lowestInvisible;

    public:
        ~KVRecoveryUnit() {}

        /**
         * Check whether the recovery unit has a snapshot.  Used by the capped iterator to check
         * whether to set the lowest invisible id.
         */
        virtual bool hasSnapshot() const = 0;

        void setLowestInvisible(const RecordId& id) {
            _lowestInvisible = id;
        }

        const RecordId &getLowestInvisible() const {
            return _lowestInvisible;
        }
    };

} // namespace mongo
