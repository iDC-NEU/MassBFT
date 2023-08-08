//
// Created by user on 23-8-8.
//

#include "pension_http_server.h"
#include "ycsb/sdk/client_sdk.h"
#include "common/property.h"

#include "nlohmann/json.hpp"
#include "httplib.h"

namespace demo::pension {
    class ServerController {
    public:
        explicit ServerController(std::unique_ptr<ServiceBackend> service) {
            _service = std::move(service);
        }

        void putData(const httplib::Request &req, httplib::Response &res) {
            processDataInner(req, res, [&](const nlohmann::json& requestBody) -> std::unique_ptr<proto::Envelop> {
                if (!requestBody.contains("key") || !requestBody.contains("value")) {
                    SetErrorWithMessage(res, "Invalid request. Missing key or value.");
                    return nullptr;
                }
                const auto& key = requestBody["key"];
                const auto& value = requestBody["value"];
                auto ret = _service->put(key, value);
                return ret;
            }, [&] (const proto::Envelop& requestBody, const ycsb::sdk::TxMerkleProof& responseBody) {
                // TODO
                std::stringstream ss;
                ss << "Operation success complete!";
                return ss.str();
            });
        }

        void putDigest(const httplib::Request &req, httplib::Response &res) {
            processDataInner(req, res, [&](const nlohmann::json& requestBody) -> std::unique_ptr<proto::Envelop> {
                if (!requestBody.contains("key") || !requestBody.contains("value")) {
                    SetErrorWithMessage(res, "Invalid request. Missing key or value.");
                    return nullptr;
                }
                const auto& key = requestBody["key"];
                const auto& value = requestBody["value"];
                auto ret = _service->putDigest(key, value);
                return ret;
            }, [&] (const proto::Envelop& requestBody, const ycsb::sdk::TxMerkleProof& responseBody) {
                // TODO
                std::stringstream ss;
                ss << "Operation success complete!";
                return ss.str();
            });
        }

        void getDigest(const httplib::Request &req, httplib::Response &res) {
            processDataInner(req, res, [&](const nlohmann::json& requestBody) -> std::unique_ptr<proto::Envelop> {
                if (!requestBody.contains("key")) {
                    SetErrorWithMessage(res, "Invalid request. Missing key.");
                    return nullptr;
                }
                const auto& key = requestBody["key"];
                auto ret = _service->getDigest(key);
                return ret;
            }, [&] (const proto::Envelop& requestBody, const ycsb::sdk::TxMerkleProof& responseBody) {
                // TODO
                std::stringstream ss;
                ss << "Operation success complete!";
                return ss.str();
            });
        }

    protected:
        void processDataInner(const httplib::Request &req, httplib::Response &res,
                                     const std::function<std::unique_ptr<proto::Envelop>(const nlohmann::json& requestBody)>& fStart,
                                     const std::function<std::string(const proto::Envelop& requestBody,
                                                                     const ycsb::sdk::TxMerkleProof& responseBody)>& fFinish) {
            nlohmann::json body;
            try {
                body = nlohmann::json::parse(req.body);
            } catch (const nlohmann::json::parse_error &e) {
                SetErrorWithMessage(res, "Invalid JSON data");
                return;
            }
            // get local chain id
            auto localNodeInfo = _service->getProperties().getNodeProperties().getLocalNodeInfo();
            // get height first
            auto startHeight = _service->getReceiver()->getChainHeight(localNodeInfo->groupId, 1000);
            auto request = fStart(body);
            if (request == nullptr) {
                SetErrorWithMessage(res, "Failed to invoke chaincode.");
                return;
            }
            for (int i=0; i<5; i++) {   // retry 5 times
                auto result = _service->getReceiver()->getTransaction(request->getSignature().digest, localNodeInfo->groupId, startHeight, 2000);
                if (result == nullptr) {
                    LOG(WARNING) << "failed to get execute result, retrying.";
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    continue;
                }
                SetSuccessWithMessage(res, fFinish(*request, *result));
                return;
            }
            SetErrorWithMessage(res, "Failed to get response in time.");
        }

    protected:
        static void SetErrorWithMessage(httplib::Response &res, auto&& message) {
            nlohmann::json ret;
            ret["success"] = false;
            ret["message"] = message;
            res.status = 400;
            res.set_content(ret.dump(), "application/json");
        }

        static void SetSuccessWithMessage(httplib::Response &res, auto&& message) {
            nlohmann::json ret;
            ret["success"] = true;
            ret["message"] = message;
            res.set_content(ret.dump(), "application/json");
        }

    private:
        std::unique_ptr<ServiceBackend> _service;

    };

    std::unique_ptr<ServiceBackend> ServiceBackend::NewServiceBackend(std::shared_ptr<util::Properties> prop) {
        ycsb::sdk::ClientSDK::InitSDKDependencies();
        auto backend = std::unique_ptr<ServiceBackend>(new ServiceBackend);
        backend->_sdk = ycsb::sdk::ClientSDK::NewSDK(*prop);
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

    std::unique_ptr<proto::Envelop> ServiceBackend::put(const std::string &key, const std::string &value) {
        auto digest = util::OpenSSLSHA256::generateDigest(value.data(), value.size());
        if (digest == std::nullopt) {
            LOG(ERROR) << "Calculate hash failed!";
            return nullptr;
        }
        return putDigest(key, *digest);
    }

    std::unique_ptr<proto::Envelop> ServiceBackend::putDigest(const std::string &key, const util::OpenSSLSHA256::digestType &digest) {
        std::string raw;
        zpp::bits::out out(raw);
        if (failure(out(key, std::string(std::begin(digest), std::end(digest))))) {
            LOG(ERROR) << "serialize data failed!";
            return nullptr;
        }
        ycsb::sdk::SendInterface* sender = _sdk.get();
        auto ret = sender->invokeChaincode("hash_chaincode", "Set", raw);
        if (!ret) {
            LOG(ERROR) << "Failed to put data!";
            return nullptr;
        }
        DLOG(INFO) << "Put data to blockchain, key: " << key
                   << ", valueHash: " << util::OpenSSLSHA256::toString(digest)
                   << ", request Digest: " << util::OpenSSLED25519::toString(ret->getSignature().digest);
        return ret;
    }

    std::unique_ptr<proto::Envelop> ServiceBackend::getDigest(const std::string &key) {
        std::string raw;
        zpp::bits::out out(raw);
        if (failure(out(key))) {
            LOG(ERROR) << "serialize data failed!";
            return nullptr;
        }
        ycsb::sdk::SendInterface* sender = _sdk.get();
        auto ret = sender->invokeChaincode("hash_chaincode", "Get", raw);
        if (!ret) {
            LOG(ERROR) << "Failed to put data!";
            return nullptr;
        }
        DLOG(INFO) << "Get data from blockchain, key: " << key
                   << ", request Digest: " << util::OpenSSLED25519::toString(ret->getSignature().digest);
        return ret;
    }

    ycsb::sdk::ReceiveInterface *ServiceBackend::getReceiver() {
        return _sdk.get();
    }

    ServiceBackend::~ServiceBackend() = default;

    std::unique_ptr<ServerBackend> ServerBackend::NewServerBackend(std::unique_ptr<ServiceBackend> service) {
        auto server = std::unique_ptr<ServerBackend>(new ServerBackend);
        server->_controller = std::make_unique<ServerController>(std::move(service));
        server->_server = std::make_unique<httplib::Server>();
        server->_server->Post("/block/put_data", [&](const httplib::Request &req, httplib::Response &res) {
            server->_controller->putData(req, res);
        });
        server->_server->Get("/block/put_data", [&](const httplib::Request &req, httplib::Response &res) {
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

    bool ServerBackend::start(int port) {
        if (_server->listen("0.0.0.0", port)) {
            LOG(ERROR) << "Failed to start the server at port " << port;
            return false;
        }
        return true;
    }
}
