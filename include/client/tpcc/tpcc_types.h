#pragma once

#include <array>
#include "glog/logging.h"

namespace client::tpcc {
    using Integer = int32_t;
    using Timestamp = uint64_t;
    using Numeric = double;

    inline constexpr static Integer minInteger = std::numeric_limits<int>::min();


    template <size_t maxLength>
    class Varchar {
    public:
        Varchar() : length(0) {}

        template<class Container>
        requires requires(const Container& str) { str.begin(); str.end(); str.size(); }
        explicit Varchar(const Container& str)
                : length(str.size()){
            DCHECK(length < maxLength);
            std::copy(str.begin(), str.end(), data.begin());
        }

        auto begin() const noexcept {
            return data.begin();
        }

        auto end() const noexcept {
            return data.begin() + length;
        }

        auto size() const noexcept {
            return length;
        }

        void append(char x) {
            DCHECK(length < maxLength-1);
            data[length++] = x;
        };

        auto toString() { return std::string(data.data(), length); };

        // concatenates two strings
        template<class Container>
        requires requires(const Container& str) { str.begin(); str.end(); str.size(); }
        Varchar<maxLength> operator||(const Container& rhs) const {
            Varchar<maxLength> ret;
            ret.length = length + rhs.size();
            CHECK(ret.length <= maxLength);
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

    private:
        int16_t length;
        std::array<char, maxLength> data;
    };
}