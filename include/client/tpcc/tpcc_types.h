#pragma once

#include "zpp_bits.h"
#include "glog/logging.h"

namespace client::tpcc {
    using Integer = int32_t;
    using Timestamp = uint64_t;
    using Numeric = double;

    template <size_t maxLength>
    class Varchar {
    public:
        Varchar() : length(0) {}

        template<class Container>
        requires requires(const Container& str) { str.begin(); str.end(); str.size(); }
        explicit Varchar(const Container& str)
                : length(str.size()){
            DCHECK(this->size() < maxLength);
            std::copy(str.begin(), str.end(), data.begin());
        }

        [[nodiscard]] auto begin() const noexcept {
            return data.begin();
        }

        [[nodiscard]] auto end() const noexcept {
            return data.begin() + length;
        }

        [[nodiscard]] size_t size() const noexcept {
            return length;
        }

        void append(char x) {
            DCHECK(this->size() < maxLength-1);
            data[length++] = x;
        };

        auto toString() { return std::string(data.data(), length); };

        // concatenates two strings
        template<class Container>
        requires requires(const Container& str) { str.begin(); str.end(); str.size(); }
        Varchar<maxLength> operator||(const Container& rhs) const {
            Varchar<maxLength> ret;
            ret.length = length + rhs.size();
            DCHECK(ret.size() <= maxLength);
            std::copy(this->begin(), this->end(), ret.data.begin());
            std::copy(rhs.begin(), rhs.end(), ret.data.begin()+length);
            return ret;
        }

        template<class Container>
        requires requires(const Container& str) { str.begin(); str.end(); str.size(); }
        bool operator==(const Container& rhs) const {
            if (this->size() != rhs.size()) {
                return false;
            }
            return std::equal(this->begin(), this->end(), rhs.begin());
        }

        bool operator<(const Varchar<maxLength>& rhs) const {
            auto minLen = std::min(this->size(), rhs.size());
            auto ret = std::memcmp(this->data.data(), rhs.data.data(), minLen);
            if (ret != 0) {
                return ret < 0;
            }
            return this->size() < rhs.size();
        }

        char& operator[](size_t idx) noexcept {
            DCHECK(idx < length);
            return this->data[idx];
        }

        char& operator[](size_t idx) const noexcept {
            DCHECK(idx < length);
            return this->data[idx];
        }

        void resize(size_t size) {
            DCHECK(size <= maxLength);
            length = size;
        }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, auto &b) {
            return archive(b.length, b.data);
        }

    private:
        uint32_t length;
        std::array<char, maxLength> data;
    };
}