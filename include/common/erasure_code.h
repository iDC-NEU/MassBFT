//
// Created by peng on 11/20/22.
//

#pragma once

#include "erasurecode.h"
#include "glog/logging.h"
#include <optional>
#include <memory>

namespace util {
    class ECEncodeHelper {
    public:
        ECEncodeHelper(int id, int dataNum, int parityNum)
                :_id(id),_dataNum(dataNum), _parityNum(parityNum) { }

        ECEncodeHelper(const ECEncodeHelper&) = delete;

        ~ECEncodeHelper() {
            auto ret = liberasurecode_encode_cleanup(_id, encodedData, encodedParity);
            if (ret != 0) {
                LOG(WARNING) << "ECEncodeHelper free memory failed!";
            }
        }

        // WARNING: return a string_view, be careful
        [[nodiscard]] std::optional<std::string_view> get(int index) const {
            auto partyIndex = index - _dataNum;
            if (partyIndex < 0) {
                return std::string_view(encodedData[index], _fragmentLen);
            }
            if (partyIndex < _parityNum) {
                return std::string_view(encodedParity[partyIndex], _fragmentLen);
            }
            return std::nullopt;
        }

        [[nodiscard]] inline int size() const {
            return _dataNum+_parityNum;
        }

    protected:
        friend class ErasureCode;
        bool encode(const std::string& data) {
            CHECK(encodedData == nullptr && encodedParity == nullptr);  // only call encode once for each instance
            auto ret = liberasurecode_encode(_id, data.data(), data.size(), &encodedData, &encodedParity, &_fragmentLen);
            if (ret != 0) {
                return false;
            }
            return true;
        }

    private:
        char **encodedData = nullptr;     /* array of k data buffers */
        char **encodedParity = nullptr;     /* array of m parity buffers */
        uint64_t _fragmentLen{};            /* length, in bytes of the fragments */
        const int _id, _dataNum, _parityNum;
    };

    class ECDecodeHelper {
    public:
        explicit ECDecodeHelper(int id) : _id(id) { }

        ECDecodeHelper(const ECEncodeHelper&) = delete;

        ~ECDecodeHelper() {
            auto ret = liberasurecode_decode_cleanup(_id, originPayload);
            if (ret != 0) {
                LOG(WARNING) << "ECDecodeHelper free memory failed!";
            }
        }

        // WARNING: return a string_view, be careful
        [[nodiscard]] std::optional<std::string_view> getData() const {
            if (originPayload == nullptr || originDataSize == 0) {
                return std::nullopt;
            }
            return std::string_view(originPayload, originDataSize);
        }

    protected:
        friend class ErasureCode;
        bool decode(const std::vector<std::string_view>& fragmentList) {
            if (fragmentList.empty()) {
                return false;
            }
            auto fragmentLen = fragmentList[0].size();
            std::vector<const char*> rawFL;
            rawFL.reserve(fragmentList.size());
            for (const auto& fragment: fragmentList) {
                if (fragmentLen != fragment.size()) {
                    return false;   // check size are equal
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

    private:
        const int _id;
        uint64_t originDataSize = 0;            /* data size in bytes ,from fragment hdr */
        char *originPayload = nullptr;            /* buffer to store original payload in */
    };

    class ErasureCode {
    public:
        using BackEndType = ec_backend_id_t;
        ErasureCode(int dataNum, int parityNum, BackEndType bt=EC_BACKEND_ISA_L_RS_VAND)
                :_dataNum(dataNum),_parityNum(parityNum) {
            if(!liberasurecode_backend_available(bt)) {
                LOG(WARNING) << "ErasureCode backend " << bt <<" not available, switch to LIBERASURECODE_RS_VAND.";
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

        virtual ~ErasureCode() {
            if(liberasurecode_instance_destroy(id) != 0) {
                LOG(WARNING) << "Failed to destroy erasure code instance, id: " << id;
            }
        }

        ErasureCode(const ErasureCode&) = delete;

        std::unique_ptr<ECEncodeHelper> encode(const std::string& data) {
            auto storage = std::make_unique<ECEncodeHelper>(id, _dataNum, _parityNum);
            if(storage->encode(data)) {
                return storage;
            }
            return nullptr;
        }

        std::unique_ptr<ECDecodeHelper> decode(const std::vector<std::string_view>& fragmentList) {
            auto storage = std::make_unique<ECDecodeHelper>(id);
            if(storage->decode(fragmentList)) {
                return storage;
            }
            return nullptr;
        }

    private:
        int id;
        const int _dataNum, _parityNum;
    };
}