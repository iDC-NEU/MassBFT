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

        // shardCount, fragmentLen, or nullptr
        [[nodiscard]] virtual std::optional<std::pair<int, int>> encodeWithBuffer(std::string_view data, char* buffer, int size) const = 0;

        virtual bool decodeWithBuffer(const std::vector<std::string_view>& fragmentList, int dataLen, char* buffer, int size) const = 0;

        [[nodiscard]] virtual std::unique_ptr<EncodeResult> encode(std::string_view data) const = 0;

        [[nodiscard]] virtual std::unique_ptr<DecodeResult> decode(const std::vector<std::string_view>& fragmentList, int dataLen) const = 0;

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
        friend class LibErasureCode;

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
        friend class LibErasureCode;

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

        [[nodiscard]] std::unique_ptr<DecodeResult> decode(const std::vector<std::string_view>& fragmentList, int dataLen) const override {
            if (fragmentList.empty()) {
                return nullptr;
            }
            auto storage = std::make_unique<LibECDecodeResult>(id);
            if(storage->decode(fragmentList)) {
                return storage;
            }
            return nullptr;
        }

        std::optional<std::pair<int, int>> encodeWithBuffer(std::string_view data, char* buffer, int size) const override {
            auto storage = LibECEncodeResult(id, _dataNum, _parityNum);
            if(!storage.encode(data)) {
                return std::nullopt;
            }
            auto ret = storage.getAll();
            if (!ret) {
                return std::nullopt;
            }
            if (size < (int)storage._fragmentLen*storage.size()) {
                return std::nullopt;
            }
            for (int i=0; i<(int)ret->size(); i++) {
                std::memcpy(buffer+i*storage._fragmentLen, (*ret)[i].data(), storage._fragmentLen);
            }
            return std::make_pair(storage.size(), storage._fragmentLen);
        }

        bool decodeWithBuffer(const std::vector<std::string_view>& fragmentList, int dataLen, char* buffer, int size) const override {
            if (fragmentList.empty()) {
                return false;
            }
            auto storage = std::make_unique<LibECDecodeResult>(id);
            if(!storage->decode(fragmentList)) {
                return false;
            }
            if ((int)storage->originDataSize > size) {
                return false;
            }
            std::memcpy(buffer, storage->originPayload, dataLen);
            return true;
        }

    private:
        int id;
    };

    // Call by GoEC
    class GoEncodeResult : public EncodeResult {
    public:
        GoEncodeResult(GoInt id, int size)
                :EncodeResult(size), _id(id) { }

        GoEncodeResult(const GoEncodeResult&) = delete;

        ~GoEncodeResult() override {
            if (shardsRaw != nullptr) {
                encodeCleanup(_id, shardsRaw.get(), _shardLen);
            }
        }

        bool encode(std::string_view data, void* buffer, int size) {
            DCHECK(_fragmentLen == 0);  // only call encode once for each instance

            auto ret = ::encodeFirst(_id, (void *)data.data(), static_cast<GoInt>(data.size()), &_fragmentLen, &_shardLen);
            if (ret != 0) {
                return false;
            }
            if (size < _fragmentLen*_shardLen) {
                return false;
            }
            shardsRaw.reset(new char*[_shardLen]);
            for(int i=0; i<_shardLen; i++) {
                shardsRaw.get()[i] = &(static_cast<char*>(buffer)[i*_fragmentLen]);
            }
            ret = ::encodeNext(_id, shardsRaw.get(), _shardLen);
            if (ret != 0) {
                return false;
            }
            return true;
        }

        bool encode(std::string_view data) override {
            DCHECK(_fragmentLen == 0);  // only call encode once for each instance

            auto ret = ::encodeFirst(_id, (void *)data.data(), static_cast<GoInt>(data.size()), &_fragmentLen, &_shardLen);
            if (ret != 0) {
                return false;
            }
            shardsRaw.reset(new char*[_shardLen]);
            shardsView.reset(new char[_shardLen*_fragmentLen]);
            for(int i=0; i<_shardLen; i++) {
                shardsRaw.get()[i] = &(shardsView.get()[i*_fragmentLen]);
            }
            ret = ::encodeNext(_id, shardsRaw.get(), _shardLen);
            if (ret != 0) {
                return false;
            }
            return true;
        }

        friend class GoErasureCode;

    protected:
        // WARNING: return a string_view, be careful
        [[nodiscard]] std::optional<std::string_view> get(int index) const override {
            if (index < (int)_shardLen) {
                return std::string_view(shardsRaw.get()[index], _fragmentLen);
            }
            return std::nullopt;
        }

        [[nodiscard]] std::optional<std::vector<std::string_view>> getAll() const override {
            if (shardsRaw == nullptr) {
                return std::nullopt;
            }
            std::vector<std::string_view> fragmentList;
            for (int i=0; i<_shardLen; i++) {
                fragmentList.emplace_back(std::string_view(shardsRaw.get()[i], _fragmentLen));
            }
            return fragmentList;
        }

    private:
        std::unique_ptr<char *> shardsRaw = nullptr;
        std::unique_ptr<char> shardsView = nullptr;
        GoInt _fragmentLen{};            /* length, in bytes of the fragments */
        GoInt _shardLen{};
        const GoInt _id;
    };

    // Call by GoEC
    class GoECDecodeResult : public DecodeResult {
    public:
        explicit GoECDecodeResult(GoInt id, int dataLen) : _id(id), _dataLen(dataLen) { }

        GoECDecodeResult(const GoECDecodeResult&) = delete;

        ~GoECDecodeResult() override {
            decodeCleanup(_id, data.get());
        }

        bool decode(const std::vector<std::string_view>& fragmentList, void* buffer, int size) const {
            // calculate fragmentLen
            size_t fragmentLen = 0;
            for (const auto& fragment: fragmentList) {
                if (!fragment.empty()) {
                    fragmentLen = fragment.size();
                    break;
                }
            }
            if (size < _dataLen) {
                return false;
            }
            // setup rawFL
            std::vector<const char*> rawFL;
            rawFL.resize(fragmentList.size());
            for (auto i=0; i<(int)fragmentList.size(); i++) {
                if (!fragmentList[i].empty()) {
                    rawFL[i] = fragmentList[i].data();
                }
            }
            auto ret = ::decode(_id, rawFL.data(), static_cast<GoInt>(rawFL.size()), static_cast<GoInt>(fragmentLen), static_cast<GoInt>(_dataLen), static_cast<char*>(buffer));
            if (ret != 0) {
                return false;
            }
            return true;
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
            // setup rawFL
            std::vector<const char*> rawFL;
            rawFL.resize(fragmentList.size());
            for (auto i=0; i<(int)fragmentList.size(); i++) {
                if (!fragmentList[i].empty()) {
                    rawFL[i] = fragmentList[i].data();
                }
            }
            data.reset(new char[_dataLen]);
            auto ret = ::decode(_id, rawFL.data(), static_cast<GoInt>(rawFL.size()), static_cast<GoInt>(fragmentLen), static_cast<GoInt>(_dataLen), data.get());
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
            return std::string_view(data.get(), _dataLen);
        }

    private:
        const GoInt _id{};    // data shard count
        const int _dataLen{};
        std::unique_ptr<char> data{};
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

        [[nodiscard]] std::unique_ptr<DecodeResult> decode(const std::vector<std::string_view>& fragmentList, int dataLen) const override {
            if ((int)fragmentList.size() != this->_parityNum+this->_dataNum) {
                return nullptr;
            }
            auto storage = std::make_unique<GoECDecodeResult>(id, dataLen);
            if(storage->decode(fragmentList)) {
                return storage;
            }
            return nullptr;
        }

        std::optional<std::pair<int, int>> encodeWithBuffer(std::string_view data, char* buffer, int size) const override {
            auto storage = GoEncodeResult(id, _dataNum+_parityNum);
            if (!storage.encode(data, buffer, size)) {
                return std::nullopt;
            }
            return std::make_pair(storage._shardLen, storage._fragmentLen);
        }

        bool decodeWithBuffer(const std::vector<std::string_view>& fragmentList, int dataLen, char* buffer, int size) const override {
            if ((int)fragmentList.size() != this->_parityNum+this->_dataNum) {
                return false;
            }
            auto storage = GoECDecodeResult(id, dataLen);
            return storage.decode(fragmentList, buffer, size);
        }

    private:
        GoInt id;
    };
}