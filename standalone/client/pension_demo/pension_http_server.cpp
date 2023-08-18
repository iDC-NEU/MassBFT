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
            }, [&] (const proto::DigestString& digest, const client::sdk::TxMerkleProof& responseBody) {
                // TODO
                std::stringstream ss;
                ss << "Operation success complete!";
                return ss.str();
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
            }, [&] (const proto::DigestString& digest, const client::sdk::TxMerkleProof& responseBody) {
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
            }, [&] (const proto::DigestString& digest, const client::sdk::TxMerkleProof& responseBody) {
                // TODO
                std::stringstream ss;
                ss << "Operation success complete!";
                return ss.str();
            });
        }

    protected:
        void processDataInner(const httplib::Request &req, httplib::Response &res,
                                     const std::function<client::core::Status(const nlohmann::json& requestBody)>& fStart,
                                     const std::function<std::string(const proto::DigestString& digest,
                                                                     const client::sdk::TxMerkleProof& responseBody)>& fFinish) {
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
            for (int i=0; i<5; i++) {   // retry 5 times
                auto result = _service->getReceiver()->getTransaction(txId, localNodeInfo->groupId, startHeight, 2000);
                if (result == nullptr) {
                    LOG(WARNING) << "failed to get execute result, retrying.";
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    continue;
                }
                util::setSuccessWithMessage(res, fFinish(txId, *result));
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
        if (failure(out(key, std::string(std::begin(digest), std::end(digest))))) {
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
        return server;
    }

    ServerBackend::~ServerBackend() = default;
}
