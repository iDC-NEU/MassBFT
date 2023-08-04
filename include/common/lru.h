#pragma once
#include <algorithm>
#include <cstdint>
#include <list>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

namespace util {
    class NullMutex {
    public:
        void lock() {}

        void unlock() {}

        bool try_lock() { return true; }
    };

/**
 * error raised when a key not in cache is passed to get()
 */
    class KeyNotFound : public std::invalid_argument {
    public:
        KeyNotFound() : std::invalid_argument("key_not_found") {}
    };

    template<typename K, typename V>
    struct KeyValuePair {
    public:
        K key;
        V value;

        KeyValuePair(K k, V v) : key(std::move(k)), value(std::move(v)) {}
    };

/**
 *	The LRU LRUCache class templated by
 *		Key - key type
 *		Value - value type
 *		MapType - an associative container like std::unordered_map
 *		MutexType - a mutex type derived from the Mutex class (default: NullMutex = no synchronization)
 *
 *	The default NullMutex based template is not thread-safe, however passing
 *  Mutex = std::mutex will make it thread-safe
 */
    template<class Key, class Value, class Mutex = NullMutex,
            class Map = std::unordered_map<Key, typename std::list<KeyValuePair<Key, Value>>::iterator>>
    class LRUCache {
    public:
        typedef std::list<KeyValuePair<Key, Value>> list_type;
        typedef Mutex mutex_type;
        using Guard = std::lock_guard<mutex_type>;

        /**
         * the maxSize is the soft limit of keys and (maxSize + elasticity) is the hard limit
         * the cache is allowed to grow till (maxSize + elasticity) and is pruned back to maxSize keys
         * set maxSize = 0 for an unbounded cache (but in that case, you're better off
         * using a std::unordered_map directly anyway! :)
         */
        explicit LRUCache(size_t maxSize = 64, size_t elasticity = 10)
                : maxSize_(maxSize), elasticity_(elasticity) {}

        virtual ~LRUCache() = default;

        LRUCache(const LRUCache &) = delete;

        LRUCache &operator=(const LRUCache &) = delete;

        void init(size_t maxSize = 64, size_t elasticity = 10) {
            maxSize_ = maxSize;
            elasticity_ = elasticity;
        }

        size_t size() const {
            Guard g(mutex_);
            return cache_.size();
        }

        bool empty() const {
            Guard g(mutex_);
            return cache_.empty();
        }

        void clear() {
            Guard g(mutex_);
            cache_.clear();
            keys_.clear();
        }

        void insert(const Key &k, Value v) {
            Guard g(mutex_);
            const auto iter = cache_.find(k);
            if (iter != cache_.end()) {
                iter->second->value = v;
                keys_.splice(keys_.begin(), keys_, iter->second);
                return;
            }

            keys_.emplace_front(k, std::move(v));
            cache_[k] = keys_.begin();
            prune();
        }

        bool tryGetCopy(const Key &kIn, Value &vOut) {
            Guard g(mutex_);
            Value tmp;
            if (!tryGetRef_nolock(kIn, tmp)) { return false; }
            vOut = tmp;
            return true;
        }

        bool tryGetRef(const Key &kIn, Value &vOut) {
            Guard g(mutex_);
            return tryGetRef_nolock(kIn, vOut);
        }

        /**
         *	Maybe not thread safe!
         *	The const reference returned here is only guaranteed to be valid till the next insert/delete
         *  in multi-threaded apps use getCopy() to be threadsafe
         */
        const Value &getRef(const Key &k) {
            Guard g(mutex_);
            return get_nolock(k);
        }

        /**
         * returns a copy of the stored object (if found) safe to use/recommended in multi-threaded apps
         */
        Value getCopy(const Key &k) {
            Guard g(mutex_);
            return get_nolock(k);
        }

        bool remove(const Key &k) {
            Guard g(mutex_);
            auto iter = cache_.find(k);
            if (iter == cache_.end()) {
                return false;
            }
            keys_.erase(iter->second);
            cache_.erase(iter);
            return true;
        }

        bool contains(const Key &k) const {
            Guard g(mutex_);
            return cache_.find(k) != cache_.end();
        }

        size_t getMaxSize() const { return maxSize_; }

        size_t getElasticity() const { return elasticity_; }

        size_t getMaxAllowedSize() const { return maxSize_ + elasticity_; }

        template<typename F>
        void cwalk(F &f) const {
            Guard g(mutex_);
            std::for_each(keys_.begin(), keys_.end(), f);
        }

    protected:
        const Value &get_nolock(const Key &k) {
            const auto iter = cache_.find(k);
            if (iter == cache_.end()) {
                throw KeyNotFound();
            }
            keys_.splice(keys_.begin(), keys_, iter->second);
            return iter->second->value;
        }

        bool tryGetRef_nolock(const Key &kIn, Value &vOut) {
            const auto iter = cache_.find(kIn);
            if (iter == cache_.end()) {
                return false;
            }
            keys_.splice(keys_.begin(), keys_, iter->second);
            vOut = iter->second->value;
            return true;
        }

        size_t prune() {
            size_t maxAllowed = maxSize_ + elasticity_;
            if (maxSize_ == 0 || cache_.size() < maxAllowed) {
                return 0;
            }
            size_t count = 0;
            while (cache_.size() > maxSize_) {
                cache_.erase(keys_.back().key);
                keys_.pop_back();
                ++count;
            }
            return count;
        }

    private:
        mutable Mutex mutex_;
        Map cache_;
        list_type keys_;
        size_t maxSize_;
        size_t elasticity_;
    };
}