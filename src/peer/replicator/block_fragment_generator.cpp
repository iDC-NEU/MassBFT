//
// Created by user on 23-7-4.
//

#include "peer/replicator/block_fragment_generator.h"

#include "zpp_bits.h"

namespace peer {

    bool BlockFragmentGenerator::Context::validateAndDeserializeFragments(
            const pmt::HashString &root, std::string_view raw, int start, int end) {
        if (start<0 || start>=end || end>fragmentCnt) {
            LOG(ERROR) << "index out of range!";
            return false;
        }
        auto in = zpp::bits::in(raw);
        // 1. read depth(compressed)
        int64_t depth = 0;
        in(depth).or_throw();

        pmt::Proof* previousProofPtr = nullptr;
        // When currentDS.cacheGuard is true, we need to get the omitted proof
        std::vector<pmt::HashString> cacheWhenGuard(depth);
        // must read cacheWhenGuard instead of previousProofPtr
        bool cacheWhenGuardFlag = false;

        for (auto i=start; i<end; i++) {
            auto& currentDS = decodeStorageList[i];
            // Prevent concurrent modify of the same fragment
            bool falseFlag = false;
            if (!currentDS.cacheGuard.compare_exchange_strong(falseFlag, true, std::memory_order_release, std::memory_order_relaxed)) {
                if (previousProofPtr != nullptr) {
                    // copy ALL proofs
                    for (auto j = 0; j < (int)cacheWhenGuard.size(); j++) {
                        cacheWhenGuard[j] = *previousProofPtr->Siblings[j];
                    }
                }
                for (int offset=0; offset<_ecConfig.instanceCount; offset++) {
                    in.position() += sizeof(int); // skip Path
                    int64_t currentProofSize = 0;
                    in(currentProofSize).or_throw();
                    // in.position() += currentProofSize * sizeof(pmt::hashString);
                    //---We have to read proofs but cache them locally
                    // Update cacheWhenGuard when necessary
                    for (auto j = 0; j < currentProofSize; j++) {
                        in(cacheWhenGuard[j]).or_throw();
                    }
                    //---
                    std::string_view fragment;
                    in(fragment).or_throw();
                }
                cacheWhenGuardFlag = true;
                continue;   // another thread is modifying this fragment
            }
            // resize to avoid invalid pointer
            currentDS.mtProofs.resize(_ecConfig.instanceCount);
            currentDS.encodeFragment.resize(_ecConfig.instanceCount);
            auto &currentProofCache = currentDS.cache;
            // reserve to avoid invalid pointer
            currentProofCache.reserve(_ecConfig.instanceCount*depth);
            for (int offset=0; offset<_ecConfig.instanceCount; offset++) {
                auto &currentProof = currentDS.mtProofs[offset];

                currentProof.Siblings.reserve(depth);
                // 2. write path, compressed proof size
                int64_t currentProofSize = 0;
                in(currentProof.Path, currentProofSize).or_throw();
                // 3. read proofs
                if (cacheWhenGuardFlag) {
                    DCHECK(currentProofCache.empty());
                    currentProofCache.resize(depth);
                    // first copy new data Siblings.push_back
                    for (auto j = 0; j < currentProofSize; j++) {
                        in(currentProofCache[j]).or_throw();
                        currentProof.Siblings.push_back(&currentProofCache[j]);
                    }
                    // copy CacheWhenGuard into cache
                    for (auto j=currentProofSize; j<depth; j++) {
                        currentProofCache[j] = cacheWhenGuard[j];
                        currentProof.Siblings.push_back(&currentProofCache[j]);
                    }
                    // cacheWhenGuard can be successfully deleted
                    cacheWhenGuardFlag = false;
                } else {
                    auto originalSize = currentProofCache.size();
                    currentProofCache.resize(originalSize+currentProofSize);
                    for (auto j = originalSize; j < currentProofCache.size(); j++) {
                        in(currentProofCache[j]).or_throw();
                        currentProof.Siblings.push_back(&currentProofCache[j]);
                    }
                    if (previousProofPtr != nullptr) {
                        for (auto j = currentProofSize; j < depth; j++) {
                            currentProof.Siblings.push_back(previousProofPtr->Siblings[j]);
                        }
                    }
                }
                previousProofPtr = &currentProof;
                // 4. read actual data
                std::string_view fragment;
                in(fragment).or_throw();
                ContextDataBlock fragmentView(fragment);
                // we have finished loading all proofs, verify them.
                // This function is Already Called by worker thread, no need to parallel verify
                auto verifyResult = pmt::MerkleTree::Verify(fragmentView, currentProof, root);
                if (verifyResult && *verifyResult) {
                    currentDS.encodeFragment[offset] = fragment;
                } else {
                    LOG(ERROR) << "Verification failed";
                    return false;
                }
            }
        }
        return true;
    }

    bool BlockFragmentGenerator::Context::regenerateMessage(int actualMessageSize, std::string &bufferOut) {
        // Calculate thr fragment len deterministically, using the actualMessageSize and instanceCount
        const auto fragmentLen = actualMessageSize % _ecConfig.instanceCount ? actualMessageSize / _ecConfig.instanceCount + 1: actualMessageSize / _ecConfig.instanceCount;
        std::vector<std::vector<std::string_view>> svListPartialView(_ecConfig.instanceCount);
        for(int i=0; i<_ecConfig.instanceCount; i++) {    // svListPartialView.size()
            svListPartialView[i].resize(decodeStorageList.size());
            for(auto j=0; j<fragmentCnt; j++) {   // decodeStorageList.size()
                if ((int)decodeStorageList[j].encodeFragment.size() > i) {
                    svListPartialView[i][j] = decodeStorageList[j].encodeFragment[i];
                }
            }
        }
        // init a string container with a fixed size
        if ((int)bufferOut.size() < actualMessageSize) {
            bufferOut.resize(actualMessageSize);
        }
        // the actual data size is different for the last fragment
        auto dataSize=fragmentLen;
        std::vector<std::future<bool>> futureList(_ecConfig.instanceCount);
        for(auto i=0; i<_ecConfig.instanceCount; i++) {     // (int)svListPartialView.size()
            if (i == _ecConfig.instanceCount - 1) { // adjust the size of the last fragment
                dataSize=(int)bufferOut.size() - i*fragmentLen;
            }
            futureList[i] = wpForMTAndEC->submit([&, dataSize=dataSize, idx=i]() {
                // Error handling
                if (!ec[idx]->decodeWithBuffer(svListPartialView[idx], dataSize, bufferOut.data()+idx*fragmentLen, dataSize)) {
                    util::OpenSSLSHA256 hash;
                    for (const auto &pv: svListPartialView) {
                        hash.update(pv.data(), pv.size());
                    }
                    LOG(ERROR) << "Regenerate message failed, hash: "
                               << util::OpenSSLSHA256::toString(*hash.final());
                    return false;
                }
                return true;
            });
        }
        for (auto& future: futureList) {
            if (!future.get()) {
                LOG(ERROR) << "Reconstruct message error!";
                return false;
            }
        }
        return true;
    }

    bool BlockFragmentGenerator::Context::initWithMessage(std::string_view message) {
        if (mt != nullptr) {
            return false;   // already inited
        }
        auto actualMessageSize = message.size();
        std::vector<std::future<bool>> futureList(_ecConfig.instanceCount);
        // ceil may not perform correctly in some case
        auto len = actualMessageSize % _ecConfig.instanceCount ? actualMessageSize / _ecConfig.instanceCount + 1: actualMessageSize / _ecConfig.instanceCount;
        ecEncodeResult.resize(_ecConfig.instanceCount*fragmentCnt);
        for (int i=0; i<_ecConfig.instanceCount; i++) {
            futureList[i] = wpForMTAndEC->submit([this, idx=i, &message, &len]()->bool {
                // auto solve overflow
                std::string_view msgView = message.substr(idx*len, (idx+1)*len);
                auto encodeResult = ec[idx]->encode(msgView);
                if (encodeResult == nullptr) {
                    return false;
                }
                auto ret = encodeResult->getAll();
                if (!ret) {
                    return false;
                }
                DCHECK((int)ret->size() == fragmentCnt);
                for(int j=0; j<fragmentCnt; j++) {
                    // index + offset(instance id)
                    ecEncodeResult[j*_ecConfig.instanceCount+idx] = (*ret)[j];
                }
                encodeResultHolder[idx] = std::move(encodeResult);
                return true;
            });
        }
        for (auto& ret: futureList) {
            if (!ret.get()) {
                LOG(ERROR) << "Encode failed";
                return false;
            }
        }
        pmt::Config pmtConfig;
        pmtConfig.Mode=pmt::ModeType::ModeProofGenAndTreeBuild;
        // leaf size is too big (all leaves have the equal size)
        if (ecEncodeResult[0].size() > 1024) {
            pmtConfig.LeafGenParallel = true;
        }
        // too many leaves
        if (ecEncodeResult.size() > 1024) {
            pmtConfig.RunInParallel = true;
        }
        // _ecConfig.instanceCount indicate the number of parallel running instance
        // pmtConfig.NumRoutines = (int)wpForMTAndEC->get_thread_count() / _ecConfig.instanceCount;
        std::vector<std::unique_ptr<pmt::DataBlock>> blocks;
        blocks.reserve(ecEncodeResult.size());
        // serialize perform an additional copy
        for (const auto& blockView: ecEncodeResult) {
            blocks.push_back(std::make_unique<ContextDataBlock>(blockView));
        }
        // ---- parallel merkle generation may not be useful ----
        mt = pmt::MerkleTree::New(pmtConfig, blocks, nullptr);
        // we no longer need the block view after tree generation.
        return true;
    }

    bool BlockFragmentGenerator::Context::serializeFragments(int start, int end, std::string &bufferOut, int offset) const {
        const auto& proofs = mt->getProofs();
        if (start<0 || start>=end || end>fragmentCnt) {
            LOG(ERROR) << "index out of range!";
            return false;
        }
        DCHECK(fragmentCnt*_ecConfig.instanceCount == (int)proofs.size());
        if (ecEncodeResult.empty()) {
            LOG(ERROR) << "encodeResultHolder have not encode yet";
            return false;
        }

        // create and reserve data
        const int depth = (int)proofs[start].Siblings.size();  // proofs[start] must exist
        auto reserveSize = sizeof(int)+(end-start)*_ecConfig.instanceCount*(depth*pmt::defaultHashLen+sizeof(uint32_t));
        // i: fragment id. if 3 instance, fragment#1[0, 1, 2] fragment#2[3, 4, 5]
        for (auto i=start*_ecConfig.instanceCount; i<end*_ecConfig.instanceCount; i++) {
            reserveSize += ecEncodeResult[i].size()+sizeof(int)*10;
        }
        reserveSize += offset;
        if (bufferOut.size() < reserveSize) {
            bufferOut.resize(reserveSize);
        }

        // create serializer
        auto out = zpp::bits::out(bufferOut);
        out.reset(offset);
        // 1. write depth(compressed)
        out((int64_t) depth).or_throw();

        for (auto i=start*_ecConfig.instanceCount; i<end*_ecConfig.instanceCount; i++) {
            const pmt::Proof& currentProof = proofs[i];
            auto lastIndex = depth-1; // start copy from index
            if (i != start*_ecConfig.instanceCount) {
                // reduce serialize size
                const pmt::Proof& previousProof = proofs[i-1];
                DCHECK(currentProof.Siblings.size() == previousProof.Siblings.size());
                while (*previousProof.Siblings[lastIndex] == *currentProof.Siblings[lastIndex]) {
                    lastIndex--;
                    if (lastIndex < 0) {
                        break;
                    }
                }
            }
            // 2. write path
            out((uint32_t) currentProof.Path, (int64_t) lastIndex+1).or_throw();   // record the size, not the index
            // 3. write proof vector
            for (auto j=0; j<lastIndex+1; j++) {
                const pmt::HashString& sj = *currentProof.Siblings[j];
                out(sj).or_throw();
            }
            // 4. write actual data
            out(ecEncodeResult[i]).or_throw();
        }
        DCHECK(bufferOut.size() <= reserveSize) << "please reserve data size, actual size: " << bufferOut.size() << " estimate size " << reserveSize;
        bufferOut.resize(out.position());
        return true;
    }

    std::shared_ptr<BlockFragmentGenerator::Context> BlockFragmentGenerator::getEmptyContext(const BlockFragmentGenerator::Config &cfg) {
        auto x = cfg.dataShardCnt;
        auto y = cfg.parityShardCnt;
        if (ecMap.x() < x+1 ||ecMap.y() < y+1) {
            LOG(ERROR) << "ecMap index out of range.";
            return nullptr;
        }
        util::wait_for_sema(semaMap(x, y), cfg.instanceCount);
        std::shared_ptr<Context> context(new Context(cfg, wpForMTAndEC), [this](Context* ret) {
            this->freeContext(std::unique_ptr<Context>(ret));
        });
        context->ec.resize(cfg.instanceCount);
        for(int i=0; i<cfg.instanceCount; i++) {
            ecMap(x, y)->pop(context->ec[i]);
        }
        return context;
    }

    bool BlockFragmentGenerator::freeContext(std::unique_ptr<Context> context) {
        auto x = context->_ecConfig.dataShardCnt;
        auto y = context->_ecConfig.parityShardCnt;
        // restore all ec
        for(int i=0; i<context->_ecConfig.instanceCount; i++) {
            ecMap(x, y)->push(std::move(context->ec[i]));
        }
        semaMap(x, y).signal(context->_ecConfig.instanceCount);
        return true;
    }
}