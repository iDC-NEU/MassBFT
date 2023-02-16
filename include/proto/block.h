//
// Created by peng on 12/5/22.
//

#pragma once

#include "common/crypto.h"
#include "common/parallel_merkle_tree.h"
#include "zpp_bits.h"

namespace proto {
    using HashString = util::OpenSSLSHA256::digestType;
    // publicKeyHex, signature
    using SignatureString = std::pair<std::string, util::OpenSSLED25519::digestType>;

    using BlockNumber = uint64_t;

    struct KV {
        std::string key;
        std::string value;
    };

    struct TxReadWriteSet {
        std::string chaincodeName;
        std::vector<KV> reads;
        std::vector<KV> writes;
    };

    struct QueryResult {
        std::string chaincodeName;
        std::vector<KV> reads;
    };

    struct UserRequest {
        std::string chaincodeName;
        std::vector<std::string> args;
    };

    // WARNING: ENVELOP HAS STRING_VIEW TYPE, MUST CONSIDER DANGING POINTER
    struct Envelop {
        constexpr static auto serialize(auto & archive, auto & self) {
            return archive(self.payload, self.signature);
        }
        std::string_view payload;
        SignatureString signature;
    };

    struct Block {
        struct Header {
            BlockNumber number{};
            // previous hash of ALL the user request
            HashString previousHash{};
            // current user request hash
            HashString dataHash{};
        };

        // a vector of transaction
        struct Body {
            // userRequests: represent the raw request sent by the user
            std::vector<Envelop> userRequests;
        };

        struct ExecuteResult {
            std::vector<std::string> txReadWriteSet;
            // check if a transaction is valid
            std::vector<bool> transactionFilter;
        };

        struct Metadata {
            // when a peer validate a block, it adds its signature of the HEADER to signatures.
            std::vector<SignatureString> consensusSignatures;
            // when a peer validate a block, it adds its signature of the (HEADER+ExecuteResult) to signatures.
            std::vector<SignatureString> validateSignatures;
        };

        Header header;
        Body body;
        ExecuteResult executeResult;
        Metadata metadata;
    };

    // EncodeMessage has type sv, so encoder / decoder must keep the actual message
    struct EncodeBlockFragment {
        constexpr static auto serialize(auto& archive, auto& self) {
            return archive(self.blockNumber, self.root, self.start, self.end, self.encodeMessage);
        }
        BlockNumber blockNumber;
        pmt::HashString root;
        // the start and end fragment id
        uint32_t start;
        uint32_t end;
        std::string_view encodeMessage;
    };
}
