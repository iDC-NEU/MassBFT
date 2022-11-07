//
// Created by peng on 10/18/22.
//

#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include "random_uint64.h"

namespace ycsb::utils {
    /**
     * YCSB-specific buffer class.  ByteIterators are designed to support
     * efficient field generation, and to allow backend drivers that can stream
     * fields (instead of materializing them in RAM) to do so.
     * <p>
     * YCSB originially used String objects to represent field values.  This led to
     * two performance issues.
     * </p><p>
     * First, it leads to unnecessary conversions between UTF-16 and UTF-8, both
     * during field generation, and when passing data to byte-based backend
     * drivers.
     * </p><p>
     * Second, Java strings are represented internally using UTF-16, and are
     * built by appending to a growable array type (StringBuilder or
     * StringBuffer), then calling a toString() method.  This leads to a 4x memory
     * overhead as field values are being built, which prevented YCSB from
     * driving large object stores.
     * </p>
     * The StringByteIterator class contains a number of convenience methods for
     * backend drivers that convert between Map&lt;String,String&gt; and
     * Map&lt;String,ByteBuffer&gt;.
     *
     */
    class ByteIterator {
    public:
        virtual ~ByteIterator() = default;
        virtual bool hasNext() = 0;
        virtual char nextByte() = 0;

        virtual /** @return byte offset immediately after the last valid byte */
        int nextBuf(std::vector<char>& buf, int bufOff) {
            int sz = bufOff;
            while (sz < (int)buf.size() && hasNext()) {
                buf[sz] = nextByte();
                sz++;
            }
            return sz;
        }
        virtual uint64_t bytesLeft() = 0;
        /** Consumes remaining contents of this object, and returns them as a byte array. */
        virtual std::vector<char> toArray() {
            auto left = bytesLeft();
            std::vector<char>ret;
            ret.resize(left);
            for (char& i : ret) {
                i = nextByte();
            }
            return ret;
        }
        /** Consumes remaining contents of this object, and returns them as a string. */
        virtual std::string toString() {
            auto array = toArray();
            return array.data();
        }
        /** Resets the iterator so that it can be consumed again. Not all
         * implementations support this call.
         * @throws UnsupportedOperationException if the implementation hasn't implemented
         * the method.
         */
        virtual void reset() = 0;

    };
    using ByteIteratorMap = std::unordered_map<std::string, std::unique_ptr<ByteIterator>>;
    /**
     * A ByteIterator that iterates through a string.
     */
    class StringByteIterator : public ByteIterator {
        std::string str;
        int off;
    public:
        /**
         * Put all the entries of one map into the other, converting
         * String values into ByteIterators.
         */
        static void putAllAsByteIterators(ByteIteratorMap& out, const std::unordered_map<std::string, std::string>& in) {
            for (const auto& entry : in) {
                out[entry.first] = std::make_unique<StringByteIterator>(entry.second);
            }
        }
        /**
         * Put all of the entries of one map into the other, converting
         * ByteIterator values into Strings.
         */
        static void putAllAsStrings(std::unordered_map<std::string, std::string>& out, const ByteIteratorMap& in) {
            for (const auto& entry : in) {
                out[entry.first] = entry.second->toString();
            }
        }
        /**
         * Create a copy of a map, converting the values from Strings to
         * StringByteIterators.
         */
        static ByteIteratorMap getByteIteratorMap(const std::unordered_map<std::string, std::string>& m) {
            ByteIteratorMap ret;
            for (const auto& entry : m) {
                ret[entry.first] = std::make_unique<StringByteIterator>(entry.second);
            }
            return ret;
        }

        /**
         * Create a copy of a map, converting the values from
         * StringByteIterators to Strings.
         */
        static std::unordered_map<std::string, std::string> getStringMap(const ByteIteratorMap& m) {
            std::unordered_map<std::string, std::string> ret;
            for (const auto& entry : m) {
                ret[entry.first] = entry.second->toString();
            }
            return ret;
        }

    public:
        explicit StringByteIterator(const std::string& s) {
            this->str = s;
            this->off = 0;
        }
        bool hasNext() override {
            return off < (int)str.size();
        }
        char nextByte() override {
            return str[off++];
        }
        uint64_t bytesLeft() override {
            return str.size() - off;
        }
        void reset() override {
            off = 0;
        }

        std::vector<char> toArray() override {
            std::vector<char> bytes;
            bytes.resize(bytesLeft());
            for (size_t i = 0; i < bytes.size(); i++) {
                bytes[i] = str[off + i];
            }
            off = (int)str.size();
            return bytes;
        }

        /**
         * Specialization of general purpose toString() to avoid unnecessary
         * copies.
         * <p>
         * Creating a new StringByteIterator, then calling toString()
         * yields the original String object, and does not perform any copies
         * or String conversion operations.
         * </p>
         */
        std::string toString() override {
            if (off > 0) {
                return ByteIterator::toString();
            } else {
                return str;
            }
        }
    };
    /**
     *  A ByteIterator that generates a random sequence of bytes.
     */
    class RandomByteIterator :public ByteIterator {
        uint64_t len{}, off{}, bufOff{};
        std::vector<char> buf;
        RandomUINT64 generator;
    public:
        explicit RandomByteIterator(long len): generator(0)  {
            this->len = len;
            this->buf.resize(6);
            this->bufOff = buf.size();
            fillBytes();
            this->off = 0;
        }
        char nextByte() override {
            fillBytes();
            bufOff++;
            return buf[bufOff - 1];
        }
        int nextBuf(std::vector<char>& buffer, int bufOffset) override {
            uint64_t ret;
            if (len - off < buffer.size() - bufOffset) {
                ret = (int) (len - off);
            } else {
                ret = buffer.size() - bufOffset;
            }
            for (uint64_t i = 0; i < ret; i += 6) {
                fillBytesImpl(buffer, (int)i + bufOffset);
            }
            off += ret;
            return (int)ret + bufOffset;
        }

        uint64_t  bytesLeft() override {
            return len - off - bufOff;
        }

        void reset() override {
            off = 0;
        }

        bool hasNext() override {
            return (off + bufOff) < len;
        }

        std::vector<char>  toArray() override {
            auto left = bytesLeft();
            std::vector<char> ret;
            ret.resize(left);
            uint64_t bufOffset = 0;
            while (bufOffset < ret.size()) {
                bufOffset = nextBuf(ret, (int)bufOffset);
            }
            return ret;
        }

    private:
        void fillBytesImpl(std::vector<char>& buffer, int base) {
            auto bytes = generator.nextValue();

            switch (buffer.size() - base) {
                default:
                    buffer[base + 5] = (char) (((bytes >> 25) & 95) + ' ');
                case 5:
                    buffer[base + 4] = (char) (((bytes >> 20) & 63) + ' ');
                case 4:
                    buffer[base + 3] = (char) (((bytes >> 15) & 31) + ' ');
                case 3:
                    buffer[base + 2] = (char) (((bytes >> 10) & 95) + ' ');
                case 2:
                    buffer[base + 1] = (char) (((bytes >> 5) & 63) + ' ');
                case 1:
                    buffer[base + 0] = (char) (((bytes) & 31) + ' ');
                case 0:
                    break;
            }
        }
        void fillBytes() {
            if (bufOff == buf.size()) {
                fillBytesImpl(buf, 0);
                bufOff = 0;
                off += buf.size();
            }
        }
    };
}
