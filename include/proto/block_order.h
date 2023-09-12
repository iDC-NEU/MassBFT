//
// Created by user on 23-5-6.
//

#pragma once

#include "zpp_bits.h"
#include "proto/block.h"

namespace proto {
    struct BlockOrder {
        // If blockId, voteChainId, voteBlockId all == -1,
        // this is an error message indicating group with chainId is failed.
        int chainId;
        int blockId;
        int voteChainId;
        int voteBlockId;

        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, BlockOrder &b) {
            return archive(b.chainId, b.blockId, b.voteChainId, b.voteBlockId);
        }

        bool serializeToString(std::string* buf, int pos = 0) const {
            zpp::bits::out out(*buf);
            out.reset(pos);
            if(failure(out(*this))) {
                return false;
            }
            return true;
        }

        bool deserializeFromString(const std::string& buf, int pos = 0) {
            auto in = zpp::bits::in(buf);
            in.reset(pos);
            if(failure(in(*this))) {
                return false;
            }
            return true;
        }
    };

    struct SignedBlockOrder {
        std::string serializedBlockOrder;
        std::vector<SignatureString> signatures;

        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, SignedBlockOrder &s) {
            return archive(s.serializedBlockOrder, s.signatures);
        }

        bool serializeToString(std::string* buf, int pos = 0) const {
            zpp::bits::out out(*buf);
            out.reset(pos);
            if(failure(out(*this))) {
                return false;
            }
            return true;
        }

        bool deserializeFromString(const std::string& buf, int pos = 0) {
            auto in = zpp::bits::in(buf);
            in.reset(pos);
            if(failure(in(*this))) {
                return false;
            }
            return true;
        }
    };
}