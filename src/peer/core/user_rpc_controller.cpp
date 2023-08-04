//
// Created by user on 23-6-29.
//

#include "peer/core/user_rpc_controller.h"
#include "peer/storage/mr_block_storage.h"
#include "common/meta_rpc_server.h"
#include "common/parallel_merkle_tree.h"
#include "common/lru.h"
#include "common/proof_generator.h"

namespace peer::core {
    namespace inner {
        struct ControllerImpl {
            int _rpcServerPort = -1;
            std::shared_ptr<peer::BlockLRUCache> _storage;
            using MTPair = std::pair<std::unique_ptr<util::ProofGenerator>, std::shared_ptr<pmt::MerkleTree>>;
            util::LRUCache<proto::HashString, MTPair, std::mutex> _blockBodyMerkleTree;
            util::LRUCache<proto::HashString, MTPair, std::mutex> _executeResultMerkleTree;
        };
    }

    bool peer::core::UserRPCController::NewRPCController(std::shared_ptr<peer::BlockLRUCache> storage, int rpcPort) {
        auto service = new UserRPCController();
        service->_impl = std::make_unique<inner::ControllerImpl>();
        service->_impl->_storage = std::move(storage);
        service->_impl->_rpcServerPort = rpcPort;
        if (util::DefaultRpcServer::AddService(service, rpcPort) != 0) {
            LOG(ERROR) << "Fail to add globalControlService!";
            return false;
        }
        return true;
    }

    UserRPCController::~UserRPCController() = default;

    bool UserRPCController::StartRPCService(int rpcPort) {
        if (util::DefaultRpcServer::Start(rpcPort) != 0) {
            LOG(ERROR) << "Fail to start DefaultRpcServer at port: " << rpcPort;
            return false;
        }
        return true;
    }

    void UserRPCController::StopRPCService(int rpcPort) {
        util::DefaultRpcServer::Stop(rpcPort);
    }

    void UserRPCController::hello(::google::protobuf::RpcController *,
                                  const ::ycsb::client::proto::HelloRequest *request,
                                  ::ycsb::client::proto::HelloResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard guard(done);
        response->set_success(true);
        LOG(INFO) << "Receive hello from " << request->ski();
    }

    void UserRPCController::pullBlock(::google::protobuf::RpcController *,
                                      const ::ycsb::client::proto::PullBlockRequest *request,
                                      ::ycsb::client::proto::PullBlockResponse *response,
                                      ::google::protobuf::Closure *done) {
        brpc::ClosureGuard guard(done);
        response->set_success(false);
        DLOG(INFO) << "pullBlock, chainId: " << request->chainid()  << ", blockId: " << request->blockid();
        auto timeout = -1;
        if (request->has_timeoutms()) {
            timeout = request->timeoutms();
        }
        auto block = _impl->_storage->waitForBlock(request->chainid(), request->blockid(), timeout);
        if (block == nullptr) {
            response->set_payload("Failed to get block within timeout.");
            return;
        }
        // block->setSerializedMessage(std::string(*response->mutable_payload())); is NOT thread safe!
        auto* payload = response->mutable_payload();
        auto ret = block->serializeToString(payload);
        if (!ret.valid) {
            response->set_payload("Serialize message failed.");
            return;
        }
        response->set_success(true);
    }

    void UserRPCController::getTop(::google::protobuf::RpcController *,
                                   const ::ycsb::client::proto::GetTopRequest *request,
                                   ::ycsb::client::proto::GetTopResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard guard(done);
        response->set_success(false);
        DLOG(INFO) << "pullBlock, chainId: " << request->chainid()  << ".";
        auto blockId = _impl->_storage->getMaxStoredBlockNumber(request->chainid());
        response->set_chainid(request->chainid());
        response->set_blockid(blockId);
        response->set_success(true);
    }

    void UserRPCController::getTxWithProof(::google::protobuf::RpcController *,
                                           const ::ycsb::client::proto::GetTxRequest *request,
                                           ::ycsb::client::proto::GetTxResponse *response,
                                           ::google::protobuf::Closure *done) {
        brpc::ClosureGuard guard(done);
        response->set_success(false);
        auto regionId = request->chainidhint();
        auto maxBlockNumber = _impl->_storage->getMaxStoredBlockNumber(regionId);
        auto minBlockNumber = request->blockidhint();
        for (auto i=minBlockNumber; i<(int)maxBlockNumber+1; i++) {
            auto block = _impl->_storage->waitForBlock(regionId, i);
            if (block == nullptr) {
                continue;   // block is removed by another process
            }
            // found transaction
            auto envelop = block->body.findEnvelop(request->txid());
            if (envelop == nullptr) {
                continue;
            }
            // generate request proof
            if (request->requestproof()) {
                if (!envelop->serializeToString(response->mutable_envelop())) {
                    LOG(ERROR) << "Serialize envelop failed!";
                    return;
                }
                // build the merkle tree
                auto pg = std::make_unique<util::ProofGenerator>(block->body);
                auto mt = pg->generateMerkleTree();
                auto ret = util::ProofGenerator::GenerateProof(*mt, *response->mutable_envelop());
                if (ret == std::nullopt) {
                    LOG(ERROR) << "GenerateProof failed!";
                    return;
                }
                if (!util::serializeToString(*ret, *response->mutable_envelopproof())) {
                    LOG(ERROR) << "SerializeProof failed!";
                    return;
                }
            }
            // generate response proof
            if (request->responseproof()) {
                auto rwSet = block->executeResult.findRWSet(request->txid());
                if (rwSet == nullptr) {
                    LOG(ERROR) << "Corresponding RWSets not found!";
                    return;
                }
                zpp::bits::out out(*response->mutable_rwset());
                if (failure(out(*rwSet))) {
                    return;
                }
                if (request->responseproof()) {
                    // build the merkle tree
                    auto pg = std::make_unique<util::ProofGenerator>(block->executeResult);
                    auto mt = pg->generateMerkleTree();
                    auto ret = util::ProofGenerator::GenerateProof(*mt, *response->mutable_rwset());
                    if (ret == std::nullopt) {
                        LOG(ERROR) << "GenerateProof failed!";
                        return;
                    }
                    if (!util::serializeToString(*ret, *response->mutable_rwsetproof())) {
                        LOG(ERROR) << "SerializeProof failed!";
                        return;
                    }
                }
            }
            if (!request->requirerequest()) {
                response->mutable_envelop()->clear();
            }
            if (!request->requireresponse()) {
                response->mutable_rwset()->clear();
            }
            response->set_success(true);
            return;
        }
    }
}
