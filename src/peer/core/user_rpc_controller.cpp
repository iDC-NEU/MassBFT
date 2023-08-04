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
        struct MTPair {
            std::unique_ptr<util::ProofGenerator> pg;
            std::shared_ptr<pmt::MerkleTree> mt;
        };

        struct ControllerImpl {
            std::shared_ptr<peer::BlockLRUCache> _storage;
            util::LRUCache<proto::HashString, std::shared_ptr<MTPair>, std::mutex> _blockBodyMerkleTree;
            util::LRUCache<proto::HashString, std::shared_ptr<MTPair>, std::mutex> _executeResultMerkleTree;

            std::shared_ptr<MTPair> getOrGenerateBlockBodyMT(const proto::Block& block) {
                std::shared_ptr<MTPair> ret = nullptr;
                if (_blockBodyMerkleTree.tryGetCopy(block.header.dataHash, ret)) {
                    return ret;
                }
                // build the merkle tree
                ret = std::make_shared<MTPair>();
                ret->pg = std::make_unique<util::ProofGenerator>(block.body);
                ret->mt = ret->pg->generateMerkleTree();
                if (ret->mt == nullptr) {
                    LOG(ERROR) << "merkleTree generation failed!";
                    return nullptr;
                }
                _blockBodyMerkleTree.insert(block.header.dataHash, ret);
                return ret;
            }

            std::shared_ptr<MTPair> getOrGenerateExecResultMT(const proto::Block& block) {
                std::shared_ptr<MTPair> ret = nullptr;
                if (_executeResultMerkleTree.tryGetCopy(block.header.dataHash, ret)) {
                    return ret;
                }
                // build the merkle tree
                ret = std::make_shared<MTPair>();
                ret->pg = std::make_unique<util::ProofGenerator>(block.executeResult);
                ret->mt = ret->pg->generateMerkleTree();
                if (ret->mt == nullptr) {
                    LOG(ERROR) << "merkleTree generation failed!";
                    return nullptr;
                }
                _executeResultMerkleTree.insert(block.header.dataHash, ret);
                return ret;
            }
        };
    }

    bool peer::core::UserRPCController::NewRPCController(std::shared_ptr<peer::BlockLRUCache> storage, int rpcPort) {
        auto service = new UserRPCController();
        service->_impl = std::make_unique<inner::ControllerImpl>();
        service->_impl->_storage = std::move(storage);
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
            response->set_chainid(regionId);
            response->set_blockid((int)block->header.number);
            // generate request proof
            {
                if (!envelop->serializeToString(response->mutable_envelop())) {
                    LOG(ERROR) << "Serialize envelop failed!";
                    return;
                }
                auto ret = _impl->getOrGenerateBlockBodyMT(*block);
                auto proof = util::ProofGenerator::GenerateProof(*ret->mt, *response->mutable_envelop());
                if (proof == std::nullopt) {
                    LOG(ERROR) << "GenerateProof failed!";
                    return;
                }
                if (!util::serializeToString(*proof, *response->mutable_envelopproof())) {
                    LOG(ERROR) << "SerializeProof failed!";
                    return;
                }
            }
            response->set_success(true);
            // generate response proof
            {
                auto rwSet = block->executeResult.findRWSet(request->txid());
                if (rwSet == nullptr) {
                    LOG(ERROR) << "Corresponding RWSets not found!";
                    return;
                }
                zpp::bits::out out(*response->mutable_rwset());
                if (failure(out(*rwSet))) {
                    return;
                }
                auto ret = _impl->getOrGenerateExecResultMT(*block);
                auto proof = util::ProofGenerator::GenerateProof(*ret->mt, *response->mutable_rwset());
                if (proof == std::nullopt) {
                    LOG(ERROR) << "GenerateProof failed!";
                    return;
                }
                if (!util::serializeToString(*proof, *response->mutable_rwsetproof())) {
                    LOG(ERROR) << "SerializeProof failed!";
                    return;
                }
            }
            return;
        }
    }
}
