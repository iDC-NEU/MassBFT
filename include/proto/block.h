//
// Created by peng on 12/5/22.
//

#pragma once

#include "proto/read_write_set.h"
#include "proto/user_request.h"
#include "zpp_bits.h"

namespace proto {
    using HashString = util::OpenSSLSHA256::digestType;

    using BlockNumber = uint64_t;

    using AggregatedValue = uint64_t;

    template<class T>
    inline int CompareDigest(const proto::DigestString& lhs, const T &rhs) {
        return std::memcmp(lhs.data(), rhs.data(), lhs.size());
    }

    class Block : public DeserializeStorage {
    public:
        class Header {
        public:
            // current block number
            BlockNumber number{};
            // previous block header hash
            HashString previousHash{};
            // current block body (user request) hash
            // exclude the execution result
            HashString dataHash{};
//            // 区块生成时间戳
//            std::string timeStamp;
//            // 聚合属性（聚合值类型， 聚合值）
//            std::unordered_map<std::string, AggregatedValue> aggregatedAttributes;

        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, Header &h) {
                return archive(h.number, h.previousHash, h.dataHash);
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

        // a vector of transaction
        class Body {
        public:
            // userRequests: represent the raw request sent by the user
            std::vector<std::unique_ptr<Envelop>> userRequests;

        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, Body &b) {
                return archive(b.userRequests);
            }

            [[nodiscard]] Envelop* findEnvelop(const auto& digest) const {
                for (auto& it: userRequests) {
                    if (proto::CompareDigest(it->getSignature().digest, digest) != 0) {
                        continue;
                    }
                    return it.get();
                }
                return nullptr;
            }
        };

        class ExecuteResult {
        public:
            std::vector<std::unique_ptr<TxReadWriteSet>> txReadWriteSet;
            // check if a transaction is valid
            std::vector<std::byte> transactionFilter;

        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, ExecuteResult &e) {
                return archive(e.txReadWriteSet, e.transactionFilter);
            }

            [[nodiscard]] bool findRWSet(const auto& digest, TxReadWriteSet*& rwSet, std::byte& valid) const {
                for (int i=0; i<(int)txReadWriteSet.size(); i++) {
                    if (proto::CompareDigest(txReadWriteSet[i]->getRequestDigest(), digest) != 0) {
                        continue;
                    }
                    rwSet = txReadWriteSet[i].get();
                    valid = transactionFilter[i];
                    return true;
                }
                return false;
            }
        };

        // std::string: the metadata to be signed (may leave empty)
        // SignatureString: the actual signature
        using SignaturePair = std::pair<std::string, SignatureString>;

        class Metadata {
        public:
            // when a peer received a block after BFT consensus, it adds the results to signatures.
            std::vector<SignaturePair> consensusSignatures;
            // when a peer executed a block, it adds its signature of the (HEADER+ExecuteResult) to signatures.
            std::vector<SignaturePair> validateSignatures;
        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, Metadata &m) {
                return archive(m.consensusSignatures, m.validateSignatures);
            }
        };

    public:
        Block() = default;

        Block(const Block &) = delete;

        Block(Block &&) = delete;

        Header header;
        Body body;
        ExecuteResult executeResult;
        Metadata metadata;

        struct PosList {
            bool valid = false;
            std::size_t headerPos{};
            std::size_t bodyPos{};
            std::size_t execResultPos{};
            std::size_t metadataPos{};
            std::size_t endPos{};
        };

        PosList deserializeFromString(std::unique_ptr<std::string> raw, int pos = 0) {
            this->setSerializedMessage(std::move(raw));
            return deserializeFromString(pos);
        }

        PosList deserializeFromString(std::string&& raw, int pos = 0) {
            this->setSerializedMessage(std::make_unique<std::string>(std::move(raw)));
            return deserializeFromString(pos);
        }

        PosList deserializeFromString(int pos = 0) {
            PosList posList;
            if (this->storage == nullptr) {
                return posList;
            }
            auto in = zpp::bits::in(*(this->storage));
            in.reset(pos);
            posList.headerPos = in.position();
            if(failure(in(this->header))) {
                return posList;
            }
            posList.bodyPos = in.position();
            if(failure(in(this->body))) {
                return posList;
            }
            posList.execResultPos = in.position();
            if(failure(in(this->executeResult))) {
                return posList;
            }
            posList.metadataPos = in.position();
            if(failure(in(this->metadata))) {
                return posList;
            }
            posList.endPos = in.position();
            posList.valid = true;
            return posList;
        }

        // all pointer must be not null!
        PosList serializeToString(std::string *buf, int pos = 0) const {
            PosList posList;
            zpp::bits::out out(*buf);
            out.reset(pos);
            posList.headerPos = out.position();
            if(failure(out(this->header))) {
                return posList;
            }
            posList.bodyPos = out.position();
            if(failure(out(this->body))) {
                return posList;
            }
            posList.execResultPos = out.position();
            if(failure(out(this->executeResult))) {
                return posList;
            }
            posList.metadataPos = out.position();
            if(failure(out(this->metadata))) {
                return posList;
            }
            posList.endPos = out.position();
            posList.valid = true;
            return posList;
        }

        // return 0 on failure
        static PosList UpdateSerializedHeader(const Header& h, std::string *buf, int headerPos = 0) {
            PosList posList;
            zpp::bits::out out(*buf);
            out.reset(headerPos);
            posList.headerPos = out.position();
            if(failure(out(h))) {
                return posList;
            }
            posList.bodyPos = out.position();
            posList.valid = true;
            return posList;
        }

        // Append Execution result and metadata
        static PosList AppendSerializedExecutionResult(const Block& b, std::string *buf, int execResultPos = 0) {
            PosList posList;
            zpp::bits::out out(*buf);
            out.reset(execResultPos);
            posList.execResultPos = out.position();
            if(failure(out(b.executeResult))) {
                return posList;
            }
            posList.metadataPos = out.position();
            if(failure(out(b.metadata))) {
                return posList;
            }
            posList.endPos = out.position();
            posList.valid = true;
            return posList;
        }

        // Append metadata
        static PosList AppendSerializedMetadata(const Metadata& m, std::string *buf, int metadataPos = 0) {
            PosList posList;
            zpp::bits::out out(*buf);
            out.reset(metadataPos);
            posList.metadataPos = out.position();
            if(failure(out(m))) {
                return posList;
            }
            posList.endPos = out.position();
            posList.valid = true;
            return posList;
        }
    };
}
