// kv_heap_engine.cpp

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

#include "mongo/db/storage/kv_heap/kv_heap_dictionary.h"
#include "mongo/db/storage/kv_heap/kv_heap_engine.h"
#include "mongo/db/storage/kv_heap/kv_heap_recovery_unit.h"

namespace mongo {

    RecoveryUnit* KVHeapEngine::newRecoveryUnit() {
        return new KVHeapRecoveryUnit();
    }

    Status KVHeapEngine::createKVDictionary(OperationContext* opCtx,
                                            StringData ident,
                                            StringData ns,
                                            const KVDictionary::Encoding &enc,
                                            const BSONObj& options) {
        return Status::OK();
    }

    KVDictionary* KVHeapEngine::getKVDictionary(OperationContext* opCtx,
                                                StringData ident,
                                                StringData ns,
                                                const KVDictionary::Encoding &enc,
                                                const BSONObj& options,
                                                bool mayCreate) {
        boost::mutex::scoped_lock lk(_mapMutex);
        HeapsMap::const_iterator it = _map.find(ident);
        if (it != _map.end()) {
            return it->second;
        }
        std::auto_ptr<KVDictionary> ptr(new KVHeapDictionary(enc));
        _map[ident] = ptr.get();
        return ptr.release();
    }

    Status KVHeapEngine::dropKVDictionary(OperationContext* opCtx,
                                          StringData ident) {
        boost::mutex::scoped_lock lk(_mapMutex);
        _map.erase(ident);
        return Status::OK();
    }

    bool KVHeapEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
        boost::mutex::scoped_lock lk(_mapMutex);
        return _map.find(ident) != _map.end();
    }

    std::vector<std::string> KVHeapEngine::getAllIdents( OperationContext* opCtx ) const {
        std::vector<std::string> idents;
        boost::mutex::scoped_lock lk(_mapMutex);
        for (HeapsMap::const_iterator it = _map.begin(); it != _map.end(); ++it) {
            idents.push_back(it->first);
        }
        return idents;
    }

}
