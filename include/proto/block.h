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
    using SignatureString = std::pair<std::shared_ptr<std::string>, util::OpenSSLED25519::digestType>;

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

    struct Block {
        struct Header {
            friend zpp::bits::access;
            constexpr static auto serialize(auto & archive, Header& h) {
                return archive(h.number, h.previousHash, h.dataHash);
            }
            BlockNumber number{};
            // previous hash of ALL the user request
            HashString previousHash{};
            // current user request hash
            HashString dataHash{};
        };

        // a vector of transaction
        struct Body {
            friend zpp::bits::access;
            constexpr static auto serialize(auto & archive, Body& b) {
                return archive(b.userRequests);
            }
            // WARNING: ENVELOP HAS STRING_VIEW TYPE, MUST CONSIDER DANGING POINTER
            struct Envelop {
                Envelop() = default;

                Envelop(const Envelop& rhs) {
                    this->_raw = rhs._raw;
                    this->_payload = this->_raw;
                    this->signature = rhs.signature;
                }

                Envelop(Envelop&& rhs) noexcept {
                    this->_raw = std::move(rhs._raw);
                    this->_payload = this->_raw;
                    this->signature = std::move(rhs.signature);
                }

                friend zpp::bits::access;
                constexpr static auto serialize(auto & archive, Envelop& e) {
                    return archive(e._payload, e.signature);
                }

                void setPayload(std::string&& raw, std::string_view payload) {
                    _payload=payload;
                    _raw=std::move(raw);
                }

                void setPayload(std::string&& raw) {
                    _raw=std::move(raw);
                    _payload=_raw;
                }

                // owner must ensure payload is valid
                void setPayload(std::string_view payload) {
                    _payload=payload;
                }

                [[nodiscard]] const std::string_view& getPayload() const { return _payload; }

                SignatureString signature;
            private:
                std::string_view _payload;
                std::string _raw;
            };
            // userRequests: represent the raw request sent by the user
            std::vector<Envelop> userRequests;
        };

        struct ExecuteResult {
            ExecuteResult() = default;

            ExecuteResult(const ExecuteResult& rhs) {
                this->_raw = rhs._raw;
                for (const auto& it: _raw) {
                    txReadWriteSet.push_back(it);
                }
            }

            ExecuteResult(ExecuteResult&& rhs) noexcept {
                this->_raw = std::move(rhs._raw);
                for (const auto& it: _raw) {
                    txReadWriteSet.push_back(it);
                }
            }

            friend zpp::bits::access;
            constexpr static auto serialize(auto & archive, ExecuteResult& e) {
                return archive(e.txReadWriteSet, e.transactionFilter);
            }

            void setRWSets(std::vector<std::string>&& raw) {
                _raw=std::move(raw);
                txReadWriteSet.clear();
                txReadWriteSet.reserve(_raw.size());
                for (const auto& it: _raw) {
                    txReadWriteSet.push_back(it);
                }
            }

            // owner must ensure payload is valid
            void setRWSets(std::vector<std::string_view>&& rwSets) {
                txReadWriteSet=std::move(rwSets);
            }

            [[nodiscard]] const std::vector<std::string_view>& getRWSets() const { return txReadWriteSet; }

            // check if a transaction is valid
            std::vector<std::byte> transactionFilter;

        private:
            std::vector<std::string_view> txReadWriteSet;
            std::vector<std::string> _raw;
        };

        struct Metadata {
            friend zpp::bits::access;
            constexpr static auto serialize(auto & archive, Metadata& m) {
                return archive(m.consensusSignatures, m.validateSignatures);
            }
            // when a peer validate a block, it adds its signature of the HEADER to signatures.
            std::vector<SignatureString> consensusSignatures;
            // when a peer validate a block, it adds its signature of the (HEADER+ExecuteResult) to signatures.
            std::vector<SignatureString> validateSignatures;
        };

        Header header;
        Body body;
        ExecuteResult executeResult;
        Metadata metadata;

        friend zpp::bits::access;
        constexpr static auto serialize(auto & archive, Block& b) {
            return archive(b.header, b.body, b.executeResult, b.metadata);
        }

        static auto DeserializeBlock(std::string&& raw, int pos=0) {
            auto block = std::make_unique<Block>();
            block->_raw=std::move(raw);
            auto in = zpp::bits::in(block->_raw);
            in.reset(pos);
            in(block).or_throw();
            return block;
        }

        // all pointer must be not null!
        bool serializeToString(std::string* buf, int pos=0) {
            zpp::bits::out out(*buf);
            out.reset(pos);
            out(*this).or_throw();
            return true;
        }

    private:
        std::string _raw;
    };

    // EncodeMessage has type sv, so encoder / decoder must keep the actual message
    struct EncodeBlockFragment {
        constexpr static auto serialize(auto& archive, EncodeBlockFragment& self) {
            return archive(self.blockNumber, self.root, self.size, self.start, self.end, self.encodeMessage);
        }
        BlockNumber blockNumber;
        // the size hint of the actual data, and root
        pmt::HashString root;
        size_t size;
        // the start and end fragment id
        uint32_t start;
        uint32_t end;
        std::string_view encodeMessage;
    };
}
