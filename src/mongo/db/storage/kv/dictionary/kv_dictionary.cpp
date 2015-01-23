// kv_dictionary.cpp

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

#include "mongo/base/status.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary.h"
#include "mongo/db/storage/kv/dictionary/kv_dictionary_update.h"
#include "mongo/db/storage/kv/dictionary/kv_sorted_data_impl.h"
#include "mongo/db/storage/kv/slice.h"
#include "mongo/platform/endian.h"

namespace mongo {

    namespace {

        Ordering orderingDeserialize(const char *data) {
            const unsigned *big = reinterpret_cast<const unsigned *>(data);
            unsigned native = endian::bigToNative(*big);
            return *reinterpret_cast<Ordering *>(&native);
        }

        void orderingSerialize(const Ordering &o, char *data) {
            const unsigned *native = reinterpret_cast<const unsigned *>(&o);
            unsigned big = endian::nativeToBig(*native);
            const char *p = reinterpret_cast<const char *>(&big);
            std::copy(p, p + (sizeof big), data);
        }

    }

    KVDictionary::Encoding::Encoding()
        : _isRecordStore(false),
          _isIndex(false),
          _ordering(Ordering::make(BSONObj()))
    {}

    KVDictionary::Encoding KVDictionary::Encoding::forRecordStore() {
        return Encoding(true, false, Ordering::make(BSONObj()));
    }

    KVDictionary::Encoding KVDictionary::Encoding::forIndex(const Ordering &o) {
        return Encoding(false, true, o);
    }

    KVDictionary::Encoding::Encoding(const Slice &serialized)
        : _isRecordStore(serialized.size() > 0 && serialized.data()[0] == 0),
          _isIndex(serialized.size() > 0 && serialized.data()[0] == 1),
          _ordering(_isIndex
                    ? orderingDeserialize(serialized.data() + 1)
                    : Ordering::make(BSONObj()))
    {
        // Can't be both a record store and an index.
        dassert(!(_isRecordStore && _isIndex));
        // If not a record store or index, we must be the "empty encoding".
        dassert((_isRecordStore || _isIndex) || serialized.size() == 0);
          
    }

    Slice KVDictionary::Encoding::serialize() const {
        if (_isRecordStore) {
            char c = 0;
            return Slice::of(c).owned();
        } else if (_isIndex) {
            Slice s(1 + sizeof(Ordering));
            s.mutableData()[0] = 1;
            orderingSerialize(_ordering, s.mutableData() + 1);
            return s;
        } else {
            return Slice();
        }
    }

    int KVDictionary::Encoding::cmp(const Slice &a, const Slice &b) {
        const int cmp_len = std::min(a.size(), b.size());
        const int c = memcmp(a.data(), b.data(), cmp_len);
        if (c != 0) {
            return c;
        } else if (a.size() < b.size()) {
            return -1;
        } else if (a.size() > b.size()) { 
            return 1;
        } else {
            return 0;
        }
    }

    BSONObj KVDictionary::Encoding::extractKey(const Slice &key, const Slice &val) const {
        dassert(isIndex());
        return KVSortedDataImpl::extractKey(key, val, _ordering);
    }

    RecordId KVDictionary::Encoding::extractRecordId(const Slice &key) const {
        if (isRecordStore()) {
            BufReader br(key.data(), key.size());
            return KeyString::decodeRecordId(&br);
        } else {
            dassert(isIndex());
            return KVSortedDataImpl::extractRecordId(key);
        }
    }

} // namespace mongo
