//
// Created by peng on 11/20/22.
//

#pragma once

#include "erasurecode.h"
#include "libgrc.h"
#include "glog/logging.h"
#include <optional>
#include <memory>

namespace util {
    class EncodeResult {
    public:
        explicit EncodeResult(int size) :_size(size) {}

        EncodeResult(const EncodeResult&) = delete;

        virtual ~EncodeResult() = default;

        [[nodiscard]] virtual std::optional<std::string_view> get(int index) const = 0;

        [[nodiscard]] virtual std::optional<std::vector<std::string_view>> getAll() const = 0;

        [[nodiscard]] inline int size() const { return _size; }

    protected:
        virtual bool encode(std::string_view data) = 0;

    private:
        const int _size;

    };

    class DecodeResult {
    public:
        DecodeResult() = default;

        DecodeResult(const DecodeResult&) = delete;

        virtual ~DecodeResult() = default;

        [[nodiscard]] virtual std::optional<std::string_view> getData() const  = 0;

    protected:
        virtual bool decode(const std::vector<std::string_view>& fragmentList) = 0;

    };

    class ErasureCode {
    public:
        ErasureCode(int dataNum, int parityNum) :_dataNum(dataNum),_parityNum(parityNum) {}

        ErasureCode(const ErasureCode&) = delete;

        virtual ~ErasureCode() = default;

        [[nodiscard]] virtual std::unique_ptr<EncodeResult> encode(std::string_view data) const = 0;

        [[nodiscard]] virtual std::unique_ptr<DecodeResult> decode(const std::vector<std::string_view>& fragmentList) const = 0;

    protected:
        const int _dataNum, _parityNum;
    };

    // Call by LibEC
    class LibECEncodeResult : public EncodeResult {
    public:
        LibECEncodeResult(int id, int dataNum, int parityNum)
                :EncodeResult(dataNum+parityNum), _id(id),_dataNum(dataNum), _parityNum(parityNum) { }

        LibECEncodeResult(const LibECEncodeResult&) = delete;

        ~LibECEncodeResult() override {
            auto ret = liberasurecode_encode_cleanup(_id, encodedData, encodedParity);
            if (ret != 0) {
                LOG(WARNING) << "LibECEncodeResult free memory failed!";
            }
        }

        bool encode(std::string_view data) override {
            CHECK(encodedData == nullptr || encodedParity == nullptr);  // only call encode once for each instance
            auto ret = liberasurecode_encode(_id, data.data(), data.size(), &encodedData, &encodedParity, &_fragmentLen);
            if (ret != 0) {
                return false;
            }
            return true;
        }

    protected:
        // WARNING: return a string_view, be careful
        [[nodiscard]] std::optional<std::string_view> get(int index) const override {
            auto partyIndex = index - _dataNum;
            if (partyIndex < 0) {
                return std::string_view(encodedData[index], _fragmentLen);
            }
            if (partyIndex < _parityNum) {
                return std::string_view(encodedParity[partyIndex], _fragmentLen);
            }
            return std::nullopt;
        }

        [[nodiscard]] std::optional<std::vector<std::string_view>> getAll() const override {
            if (encodedData == nullptr || encodedParity == nullptr) {
                return std::nullopt;
            }
            std::vector<std::string_view> fragmentList;
            for(int i=0; i<_dataNum; i++) {
                fragmentList.emplace_back(encodedData[i], _fragmentLen);
            }
            for(int i=0; i<_parityNum; i++) {
                fragmentList.emplace_back(encodedParity[i], _fragmentLen);
            }
            return fragmentList;
        }

    private:
        char **encodedData = nullptr;     /* array of k data buffers */
        char **encodedParity = nullptr;     /* array of m parity buffers */
        uint64_t _fragmentLen{};            /* length, in bytes of the fragments */
        const int _id, _dataNum, _parityNum;
    };

    // Call by LibEC
    class LibECDecodeResult : public DecodeResult {
    public:
        explicit LibECDecodeResult(int id) : _id(id) { }

        LibECDecodeResult(const LibECDecodeResult&) = delete;

        ~LibECDecodeResult() override {
            auto ret = liberasurecode_decode_cleanup(_id, originPayload);
            if (ret != 0) {
                LOG(WARNING) << "LibECDecodeResult free memory failed!";
            }
        }

        bool decode(const std::vector<std::string_view>& fragmentList) override {
            // calculate fragmentLen
            size_t fragmentLen = 0;
            for (const auto& fragment: fragmentList) {
                if (!fragment.empty()) {
                    fragmentLen = fragment.size();
                    break;
                }
            }
            std::vector<const char*> rawFL;
            rawFL.reserve(fragmentList.size());
            for (const auto& fragment: fragmentList) {
                if (fragmentLen != fragment.size()) {
                    continue;   // skip empty string_view
                }
                rawFL.push_back(fragment.data());
            }
            auto ret = liberasurecode_decode(_id, const_cast<char **>(rawFL.data()), (int)rawFL.size(), fragmentLen,
                                             0, &originPayload, &originDataSize);
            if (ret != 0) {
                return false;
            }
            return true;
        }

    protected:
        // WARNING: return a string_view, be careful
        [[nodiscard]] std::optional<std::string_view> getData() const override {
            if (originPayload == nullptr || originDataSize == 0) {
                return std::nullopt;
            }
            return std::string_view(originPayload, originDataSize);
        }

    private:
        const int _id;
        uint64_t originDataSize = 0;            /* data size in bytes ,from fragment hdr */
        char *originPayload = nullptr;            /* buffer to store original payload in */
    };

    // LibEC impl
    class LibErasureCode : public ErasureCode {
    public:
        using BackEndType = ec_backend_id_t;
        LibErasureCode(int dataNum, int parityNum, BackEndType bt=EC_BACKEND_ISA_L_RS_VAND)
                : ErasureCode(dataNum, parityNum) {
            if(!liberasurecode_backend_available(bt)) {
                LOG(WARNING) << "LibErasureCode backend " << bt <<" not available, switch to LIBERASURECODE_RS_VAND.";
                bt = EC_BACKEND_LIBERASURECODE_RS_VAND;
            }
            if(!liberasurecode_backend_available(bt)) {
                CHECK(false) << "No valid backend found";
            }
            ec_args args{};
            args.k = dataNum;
            args.m = parityNum;
            args.hd = parityNum;
            args.ct = ec_checksum_type_t::CHKSUM_NONE;
            id = liberasurecode_instance_create(bt, &args);
            if (id <= 0) {
                CHECK(false) << "Failed to create backend instance";
            }
        }

        // Do not use liberasurecode_instance_destroy to destroy any instance
        ~LibErasureCode() override = default;

        LibErasureCode(const LibErasureCode&) = delete;

        [[nodiscard]] std::unique_ptr<EncodeResult> encode(std::string_view data) const override {
            auto storage = std::make_unique<LibECEncodeResult>(id, _dataNum, _parityNum);
            if(storage->encode(data)) {
                return storage;
            }
            return nullptr;
        }

        [[nodiscard]] std::unique_ptr<DecodeResult> decode(const std::vector<std::string_view>& fragmentList) const override {
            if (fragmentList.empty()) {
                return nullptr;
            }
            auto storage = std::make_unique<LibECDecodeResult>(id);
            if(storage->decode(fragmentList)) {
                return storage;
            }
            return nullptr;
        }

    private:
        int id;
    };

    // Call by GoEC
    class GoEncodeResult : public EncodeResult {
    public:
        GoEncodeResult(int id, int size)
                :EncodeResult(size), _id(id) { }

        GoEncodeResult(const GoEncodeResult&) = delete;

        ~GoEncodeResult() override {
            if (shardsRaw != nullptr) {
                GoSlice shards{};
                shards.data = shardsRaw;
                shards.len = _shardLen;
                shards.cap = _shardLen;
                encodeCleanup(_id, &shards);
                delete shardsRaw;
            }
        }

        bool encode(std::string_view data) override {
            DCHECK(_fragmentLen == 0);  // only call encode once for each instance

            auto ret = ::encodeFirst(_id, (void *)data.data(), static_cast<GoInt>(data.size()), &_fragmentLen, &_shardLen);
            if (ret != 0) {
                return false;
            }
            delete shardsRaw;
            shardsRaw = new char*[_shardLen];
            GoSlice shards{};
            shards.data = shardsRaw;
            shards.len = _shardLen;
            shards.cap = _shardLen;
            ret = ::encodeNext(_id, &shards);
            if (ret != 0) {
                return false;   // TODO: use free insteadof malloc
            }
            return true;
        }

    protected:
        // WARNING: return a string_view, be careful
        [[nodiscard]] std::optional<std::string_view> get(int index) const override {
            if (index < (int)_shardLen) {
                return std::string_view(shardsRaw[index], _fragmentLen);
            }
            return std::nullopt;
        }

        [[nodiscard]] std::optional<std::vector<std::string_view>> getAll() const override {
            if (shardsRaw == nullptr) {
                return std::nullopt;
            }
            std::vector<std::string_view> fragmentList;
            for (int i=0; i<_shardLen; i++) {
                fragmentList.emplace_back(std::string_view(shardsRaw[i], _fragmentLen));
            }
            return fragmentList;
        }

    private:
        char **shardsRaw = nullptr;
        GoInt _fragmentLen{};            /* length, in bytes of the fragments */
        GoInt _shardLen{};
        const int _id;
    };

    // Call by GoEC
    class GoECDecodeResult : public DecodeResult {
    public:
        explicit GoECDecodeResult(int id, int dataNum) : _id(id), _dataNum(dataNum) { }

        GoECDecodeResult(const GoECDecodeResult&) = delete;

        ~GoECDecodeResult() override {
            decodeCleanup(_id, &data);
        }

        bool decode(const std::vector<std::string_view>& fragmentList) override {
            // calculate fragmentLen
            size_t fragmentLen = 0;
            for (const auto& fragment: fragmentList) {
                if (!fragment.empty()) {
                    fragmentLen = fragment.size();
                    break;
                }
            }
            // calculate dataLen
            _dataLen = (int)fragmentLen*_dataNum;
            // setup rawFL
            std::vector<const char*> rawFL;
            rawFL.reserve(fragmentList.size());
            for (const auto& fragment: fragmentList) {
                if (!fragment.empty()) {
                    rawFL.emplace_back(fragment.data());
                } else {
                    rawFL.emplace_back(nullptr);
                }
            }
            auto ret = ::decode(_id, GoSlice{rawFL.data(), static_cast<GoInt>(rawFL.size()), static_cast<GoInt>(rawFL.capacity())}, static_cast<GoInt>(fragmentLen), static_cast<GoInt>(_dataLen), &data);
            if (ret != 0) {
                return false;
            }
            return true;
        }

    protected:
        // WARNING: return a string_view, be careful
        [[nodiscard]] std::optional<std::string_view> getData() const override {
            if (data == nullptr) {
                return std::nullopt;
            }
            return std::string_view(static_cast<char*>(data), _dataLen);
        }

    private:
        const int _id, _dataNum;    // data shard count
        int _dataLen{};
        void* data{};
    };

    class GoErasureCode : public ErasureCode {
    public:
        GoErasureCode(int dataNum, int parityNum): ErasureCode(dataNum, parityNum) {
            id = instanceCreate(dataNum, parityNum);
            if (id < 0) {
                CHECK(false) << "create instance failed.";
            }

        }

        GoErasureCode(const GoErasureCode&) = delete;

        ~GoErasureCode() override {
            if(instanceDestroy(id) != 0) {
                LOG(WARNING) << "Failed to destroy erasure code instance, id: " << id;
            }
        }

        [[nodiscard]] std::unique_ptr<EncodeResult> encode(std::string_view data) const override {
            auto storage = std::make_unique<GoEncodeResult>(id, _dataNum+_parityNum);
            if(storage->encode(data)) {
                return storage;
            }
            return nullptr;
        }

        [[nodiscard]] std::unique_ptr<DecodeResult> decode(const std::vector<std::string_view>& fragmentList) const override {
            if ((int)fragmentList.size() != this->_parityNum+this->_dataNum) {
                return nullptr;
            }
            auto storage = std::make_unique<GoECDecodeResult>(id, _dataNum);
            if(storage->decode(fragmentList)) {
                return storage;
            }
            return nullptr;
        }

    private:
        GoInt id;
    };
}