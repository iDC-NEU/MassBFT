//
// Created by user on 23-8-8.
//

#include "pension_http_server.h"
#include "client/sdk/client_sdk.h"
#include "common/property.h"
#include "common/http_util.h"

namespace demo::pension {
    class ServerController {
    public:
        explicit ServerController(std::unique_ptr<ServiceBackend> service) {
            _service = std::move(service);
        }

        void putData(const httplib::Request &req, httplib::Response &res) {
            processDataInner(req, res, [&](const nlohmann::json& requestBody) {
                if (!requestBody.contains("key") || !requestBody.contains("value")) {
                    util::setErrorWithMessage(res, "Invalid request. Missing key or value.");
                    return client::core::ERROR;
                }
                const auto& key = requestBody["key"];
                const auto& value = requestBody["value"];
                auto ret = _service->put(key, value);
                return ret;
            }, [&] (const proto::DigestString& digest, const client::sdk::TxMerkleProof& responseBody, const client::sdk::BlockHeaderProof& header, const std::string_view payload) {
                // TODO
                nlohmann::json ret;
                ret["number"] = header.header.number;//区块号
                ret["root"] = header.header.dataHash;//根哈希
                std::string digestStr = OpenSSL::bytesToString(digest);
                ret["digest"] = digestStr;//交易摘要
                //反序列化用户请求
                proto::UserRequest u;
                zpp::bits::in in(payload);
                if (failure(in(u))) {
                    LOG(INFO) << "Payload deserialization failed!" << std::endl;
                }
                const proto::KVList& writes = responseBody.rwSet->getWrites();
                for (const auto& kvPtr : writes) {
                    // 获取 proto::KV 对象的指针
                    const proto::KV* kv = kvPtr.get();

                    // 检查指针是否有效
                    if (kv) {
                        // 访问 proto::KV 对象的成员
                        const std::string_view& key = kv->getKeySV();
                        const std::string_view& value = kv->getValueSV();
                        LOG(INFO) << "WRITE:";
                        std::vector<std::string_view> valueList;
                        zpp::bits::in inv(value);
                        if (failure(inv(valueList))) {
                            LOG(INFO) << "failure";
                        }
                        LOG(INFO) << key;
                        LOG(INFO) << value;
                        for (const auto& strView : valueList) {
                            LOG(INFO) << strView << std::endl;
                        }
                        ret["valueHash"] = valueList.back();
                        // 在这里使用 key 和 value 进行操作
                    }
                }
                ret["chaincode"] = u.getCCNameSV();
                ret["function"] = u.getFuncNameSV();
                ret["merkleProof_Siblings"] = responseBody.envelopProof.Siblings;
                ret["merkleProof_Path"] = responseBody.envelopProof.Path;
                return ret.dump();
            });
        }
        void getDigestHistory(const httplib::Request &req, httplib::Response &res) {
            processDataInner(req, res, [&](const nlohmann::json& requestBody) {
                if (!requestBody.contains("key")) {
                    util::setErrorWithMessage(res, "Invalid request. Missing key.");
                    return client::core::ERROR;
                }
                const auto& key = requestBody["key"];
                auto ret = _service->getDigest(key);
                return ret;
            }, [&] (const proto::DigestString& digest, const client::sdk::TxMerkleProof& responseBody, const client::sdk::BlockHeaderProof& header, const std::string_view payload) {
                // TODO
                nlohmann::json ret;
                ret["number"] = header.header.number;//区块号
                ret["root"] = header.header.dataHash;//根哈希
                std::string digestStr = OpenSSL::bytesToString(digest);
                ret["digest"] = digestStr;//交易摘要
                LOG(INFO) <<payload;
                //反序列化用户请求
                proto::UserRequest u;
                zpp::bits::in in(payload);
                if (failure(in(u))) {
                    LOG(INFO) << "Payload deserialization failed!" << std::endl;
                }
                const proto::KVList& reads = responseBody.rwSet->getReads();
                for (const auto& kvPtr : reads) {
                    // 获取 proto::KV 对象的指针
                    const proto::KV* kv = kvPtr.get();

                    // 检查指针是否有效
                    if (kv) {
                        // 访问 proto::KV 对象的成员
                        const std::string_view& key = kv->getKeySV();
                        const std::string_view& value = kv->getValueSV();
                        LOG(INFO) << "Read:";
                        std::vector<std::string_view> valueList;
                        zpp::bits::in inv(value);
                        if (failure(inv(valueList))) {
                            LOG(INFO) << "failure";
                        }
                        LOG(INFO) << key;
                        LOG(INFO) << value;
                        for (const auto& strView : valueList) {
                            LOG(INFO) << strView << std::endl;
                        }
                        ret["valueHash"] = valueList;

                        // 在这里使用 key 和 value 进行操作
                    }
                }
                auto hash = util::OpenSSLSHA256::generateDigest(payload.data(), payload.size());
                proto::HashString hashS = hash.value();
                ret["hash"] = hashS;
                ret["chaincode"] = u.getCCNameSV();
                ret["function"] = u.getFuncNameSV();
                ret["merkleProof_Siblings"] = responseBody.envelopProof.Siblings;
                ret["merkleProof_Path"] = responseBody.envelopProof.Path;
                return ret.dump();
            });
        }

        void putDigest(const httplib::Request &req, httplib::Response &res) {
            processDataInner(req, res, [&](const nlohmann::json& requestBody) {
                if (!requestBody.contains("key") || !requestBody.contains("value")) {
                    util::setErrorWithMessage(res, "Invalid request. Missing key or value.");
                    return client::core::ERROR;
                }
                const auto& key = requestBody["key"];
                const auto& value = requestBody["value"];
                auto ret = _service->putDigest(key, value);
                return ret;
            }, [&] (const proto::DigestString& digest, const client::sdk::TxMerkleProof& responseBody, const client::sdk::BlockHeaderProof& header, const std::string_view str) {
                // TODO
                std::stringstream ss;
                ss << "Operation success complete!";
                return ss.str();
            });
        }

        void getDigest(const httplib::Request &req, httplib::Response &res) {
            processDataInner(req, res, [&](const nlohmann::json& requestBody) {
                if (!requestBody.contains("key")) {
                    util::setErrorWithMessage(res, "Invalid request. Missing key.");
                    return client::core::ERROR;
                }
                const auto& key = requestBody["key"];
                auto ret = _service->getDigest(key);
                return ret;
            }, [&] (const proto::DigestString& digest, const client::sdk::TxMerkleProof& responseBody, const client::sdk::BlockHeaderProof& header, const std::string_view payload) {
                // TODO
                nlohmann::json ret;
                ret["number"] = header.header.number;//区块号
                ret["root"] = header.header.dataHash;//根哈希
                std::string digestStr = OpenSSL::bytesToString(digest);
                ret["digest"] = digestStr;//交易摘要
                LOG(INFO) <<payload;
                //反序列化用户请求
                proto::UserRequest u;
                zpp::bits::in in(payload);
                if (failure(in(u))) {
                    LOG(INFO) << "Payload deserialization failed!" << std::endl;
                }
                const proto::KVList& reads = responseBody.rwSet->getReads();
                for (const auto& kvPtr : reads) {
                    // 获取 proto::KV 对象的指针
                    const proto::KV* kv = kvPtr.get();

                    // 检查指针是否有效
                    if (kv) {
                        // 访问 proto::KV 对象的成员
                        const std::string_view& key = kv->getKeySV();
                        const std::string_view& value = kv->getValueSV();
                        LOG(INFO) << "Read:";
                        std::vector<std::string_view> valueList;
                        zpp::bits::in inv(value);
                        if (failure(inv(valueList))) {
                            LOG(INFO) << "failure";
                        }
                        LOG(INFO) << key;
                        LOG(INFO) << value;
                        for (const auto& strView : valueList) {
                            LOG(INFO) << strView << std::endl;
                        }
                        ret["valueHash"] = valueList.back();

                        // 在这里使用 key 和 value 进行操作
                    }
                }
                auto hash = util::OpenSSLSHA256::generateDigest(payload.data(), payload.size());
                proto::HashString hashS = hash.value();
                ret["hash"] = hashS;
                ret["chaincode"] = u.getCCNameSV();
                ret["function"] = u.getFuncNameSV();
                ret["merkleProof_Siblings"] = responseBody.envelopProof.Siblings;
                ret["merkleProof_Path"] = responseBody.envelopProof.Path;
                return ret.dump();
            });
        }

    protected:
        void processDataInner(const httplib::Request &req, httplib::Response &res,
                                     const std::function<client::core::Status(const nlohmann::json& requestBody)>& fStart,
                                     const std::function<std::string(const proto::DigestString& hash,
                                                                     const client::sdk::TxMerkleProof& responseBody,
                                                                     const client::sdk::BlockHeaderProof& header,
                                                                     const std::string_view payload)>& fFinish) {
            auto [success, body] = util::parseJson(req.body);
            if (!success) {
                util::setErrorWithMessage(res, "Invalid JSON data");
                return;
            }
            // get local chain id
            auto localNodeInfo = _service->getProperties().getNodeProperties().getLocalNodeInfo();
            // get height first
            auto startHeight = _service->getReceiver()->getChainHeight(localNodeInfo->groupId, 1000);
            auto request = fStart(body);
            std::string_view payloadSV;
            if (!request.isOk()) {
                util::setErrorWithMessage(res, "Failed to invoke chaincode.");
                return;
            }
            proto::DigestString txId;
            const auto& txIdStr = request.getDigest();
            if (txId.size() != txIdStr.size()) {
                util::setErrorWithMessage(res, "Error fStart return value");
                return;
            }
            std::copy(txIdStr.begin(), txIdStr.end(), txId.begin());
            proto::HashString root;
            std::unique_ptr<client::sdk::BlockHeaderProof> header;
            for (int i=0; i<5; i++) {   // retry 5 times
                auto result = _service->getReceiver()->getTransaction(txId, localNodeInfo->groupId, startHeight, 2000);
                if (result == nullptr) {
                    LOG(WARNING) << "failed to get execute result, retrying.";
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    continue;
                }
                for (int j=startHeight; j>=0; j--) {
                    header = _service->getReceiver()->getBlockHeader(localNodeInfo->groupId, startHeight+1, 2000);
                    auto t = _service->getReceiver()->ValidateUserRequestMerkleProof(header->header.dataHash,
                                                             result->envelopProof,
                                                             *result->envelop);
                    if (t) {
                        root = header->header.dataHash;
                        std::string hash = OpenSSL::bytesToString(root);
                        payloadSV = result->envelop->getPayload();
                        break;
                    }
                }
                util::setSuccessWithMessage(res, fFinish(txId, *result, *header, payloadSV));
                return;
            }
            util::setErrorWithMessage(res, "Failed to get response in time.");
        }

    private:
        std::unique_ptr<ServiceBackend> _service;

    };

    std::unique_ptr<ServiceBackend> ServiceBackend::NewServiceBackend(std::shared_ptr<util::Properties> prop) {
        client::sdk::ClientSDK::InitSDKDependencies();
        auto backend = std::unique_ptr<ServiceBackend>(new ServiceBackend);
        backend->_sdk = client::sdk::ClientSDK::NewSDK(*prop);
        if (backend->_sdk == nullptr) {
            LOG(ERROR) << "Create sdk failed!";
            return nullptr;
        }
        if (!backend->_sdk->connect()) {
            LOG(ERROR) << "Connect to remote server failed!";
            return nullptr;
        }
        backend->_prop = std::move(prop);
        return backend;
    }

    client::core::Status ServiceBackend::put(const std::string &key, const std::string &value) {
        auto digest = util::OpenSSLSHA256::generateDigest(value.data(), value.size());
        if (digest == std::nullopt) {
            LOG(ERROR) << "Calculate hash failed!";
            return client::core::ERROR;
        }
        return putDigest(key, *digest);
    }

    client::core::Status ServiceBackend::putDigest(const std::string &key, const util::OpenSSLSHA256::digestType &digest) {
        std::string raw;
        zpp::bits::out out(raw);
        if (failure(out(key, util::OpenSSLSHA256::toString(digest)))) {
            LOG(ERROR) << "serialize data failed!";
            return client::core::ERROR;
        }
        client::sdk::SendInterface* sender = _sdk.get();
        auto ret = sender->invokeChaincode("hash_chaincode", "Set", raw);
        DLOG_IF(INFO, ret.isOk()) << "Put data to blockchain, key: " << key
                                  << ", valueHash: " << util::OpenSSLSHA256::toString(digest)
                                  << ", request Digest: " << util::OpenSSLED25519::toString(ret.getDigest());
        return ret;
    }

    client::core::Status ServiceBackend::getDigest(const std::string &key) {
        std::string raw;
        zpp::bits::out out(raw);
        if (failure(out(key))) {
            LOG(ERROR) << "serialize data failed!";
            return client::core::ERROR;
        }
        client::sdk::SendInterface* sender = _sdk.get();
        auto ret = sender->invokeChaincode("hash_chaincode", "Get", raw);
        DLOG_IF(INFO, ret.isOk()) << "Get data from blockchain, key: " << key
                                  << ", request Digest: " << util::OpenSSLED25519::toString(ret.getDigest());
        return ret;
    }

    client::sdk::ReceiveInterface *ServiceBackend::getReceiver() {
        return _sdk.get();
    }

    ServiceBackend::~ServiceBackend() = default;

    std::unique_ptr<ServerBackend> ServerBackend::NewServerBackend(std::unique_ptr<ServiceBackend> service,
                                                                   std::shared_ptr<httplib::Server> httpServer) {
        auto server = std::unique_ptr<ServerBackend>(new ServerBackend);
        server->_controller = std::make_unique<ServerController>(std::move(service));
        server->_server = std::move(httpServer);
        server->_server->Post("/block/put_data", [&](const httplib::Request &req, httplib::Response &res) {
            server->_controller->putData(req, res);
        });

        server->_server->Post("/block/put_digest", [&](const httplib::Request &req, httplib::Response &res) {
            server->_controller->putDigest(req, res);
        });

        server->_server->Post("/block/get_digest", [&](const httplib::Request &req, httplib::Response &res) {
            server->_controller->getDigest(req, res);
        });
        server->_server->Post("/block/get_history", [&](const httplib::Request &req, httplib::Response &res) {
            server->_controller->getDigestHistory(req, res);
        });
        return server;
    }

    ServerBackend::~ServerBackend() = default;
}
