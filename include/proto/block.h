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


    class Block : public DeserializeStorage {
    public:
        class Header {
        public:
            BlockNumber number{};
            // previous hash of ALL the user request
            HashString previousHash{};
            // current user request hash
            HashString dataHash{};

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
        };

        class Metadata {
        public:
            // when a peer validated a block(before execution), it adds its signature of the HEADER+BODY to signatures.
            std::vector<SignatureString> consensusSignatures;
            // when a peer validated a block(after execution), it adds its signature of the (HEADER+BODY+ExecuteResult) to signatures.
            std::vector<SignatureString> validateSignatures;
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
        PosList serializeToString(std::string *buf, int pos = 0) {
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
