// kv_engine_impl.cpp

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

#include <boost/thread/mutex.hpp>

#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/kv/dictionary/kv_engine_impl.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary.h"
#include "mongo/db/storage/kv/dictionary/kv_record_store.h"
#include "mongo/db/storage/kv/dictionary/kv_record_store_capped.h"
#include "mongo/db/storage/kv/dictionary/kv_sorted_data_impl.h"

namespace mongo {

    KVSizeStorer *KVEngineImpl::getSizeStorer(OperationContext *opCtx) {
        if (_sizeStorer) {
            return _sizeStorer.get();
        }
        static boost::mutex mutex;
        boost::mutex::scoped_lock lk(mutex);
        if (_sizeStorer) {
            return _sizeStorer.get();
        }
        std::auto_ptr<KVSizeStorer> sizeStorer(new KVSizeStorer(getMetadataDictionary(), newRecoveryUnit()));
        sizeStorer->loadFromDict(opCtx);
        _sizeStorer.reset(sizeStorer.release());
        return _sizeStorer.get();
    }

    /**
     * @param ident Ident is a one time use string. It is used for this instance
     *              and never again.
     */
    Status KVEngineImpl::createRecordStore( OperationContext* opCtx,
                                            StringData ns,
                                            StringData ident,
                                            const CollectionOptions& options ) {
        // Creating a record store is as simple as creating one with the given `ident'
        return createKVDictionary(opCtx, ident, ns, KVDictionary::Encoding::forRecordStore(),
                                  options.storageEngine);
    }

    /**
     * Caller takes ownership
     * Having multiple out for the same ns is a rules violation;
     * Calling on a non-created ident is invalid and may crash.
     */
    RecordStore* KVEngineImpl::getRecordStore( OperationContext* opCtx,
                                               StringData ns,
                                               StringData ident,
                                               const CollectionOptions& options ) {
        std::auto_ptr<KVDictionary> db(getKVDictionary(opCtx, ident, ns, KVDictionary::Encoding::forRecordStore(),
                                                  options.storageEngine));
        std::auto_ptr<KVRecordStore> rs;
        KVSizeStorer *sizeStorer = (persistDictionaryStats()
                                    ? getSizeStorer(opCtx)
                                    : NULL);
        // We separated the implementations of capped / non-capped record stores for readability.
        if (options.capped) {
            rs.reset(new KVRecordStoreCapped(db.release(), opCtx, ns, ident, options, sizeStorer, supportsDocLocking()));
        } else {
            rs.reset(new KVRecordStore(db.release(), opCtx, ns, ident, options, sizeStorer));
        }
        return rs.release();
    }

    Status KVEngineImpl::dropIdent( OperationContext* opCtx,
                                    StringData ident ) {
        return dropKVDictionary(opCtx, ident);
    }

    // --------

    Status KVEngineImpl::createSortedDataInterface(OperationContext* opCtx,
                                                   StringData ident,
                                                   const IndexDescriptor* desc) {
        // Creating a sorted data impl is as simple as creating one with the given `ident'
        const BSONObj keyPattern = desc ? desc->keyPattern() : BSONObj();
        const BSONObj options = desc ? desc->infoObj().getObjectField("storageEngine") : BSONObj();
        return createKVDictionary(opCtx, ident, desc ? desc->parentNS() : NULL, KVDictionary::Encoding::forIndex(Ordering::make(keyPattern)),
                                  options);

    }

    SortedDataInterface* KVEngineImpl::getSortedDataInterface(OperationContext* opCtx,
                                                              StringData ident,
                                                              const IndexDescriptor* desc) {
        const BSONObj keyPattern = desc ? desc->keyPattern() : BSONObj();
        const BSONObj options = desc ? desc->infoObj().getObjectField("storageEngine") : BSONObj();
        std::auto_ptr<KVDictionary> db(getKVDictionary(opCtx, ident, desc ? desc->parentNS() : NULL, KVDictionary::Encoding::forIndex(Ordering::make(keyPattern)),
                                                       options));
        return new KVSortedDataImpl(db.release(), opCtx, desc);
    }

    Status KVEngineImpl::okToRename( OperationContext* opCtx,
                                     StringData fromNS,
                                     StringData toNS,
                                     StringData ident,
                                     const RecordStore* originalRecordStore ) const {
        if (_sizeStorer) {
            _sizeStorer->store(NULL, ident,
                               originalRecordStore->numRecords(opCtx),
                               originalRecordStore->dataSize(opCtx));
            _sizeStorer->storeIntoDict(opCtx);
        }
        return Status::OK();
    }

    void KVEngineImpl::cleanShutdown() {
        if (_sizeStorer) {
            OperationContextNoop opCtx(newRecoveryUnit());
            _sizeStorer->storeIntoDict(&opCtx);
            _sizeStorer.reset();
        }
        cleanShutdownImpl();
    }

}
