//
// Created by peng on 2/19/23.
//

#pragma once

#include <memory>

#include "proto/user_request.h"
#include "proto/read_write_set.h"

namespace proto {
    using tid_type = DigestString;

    // <0: left<right
    // =0; left=right
    // >0; left>right
    template<class T>
    inline int CompareTID(const auto& lhs, const T &rhs) {
        return std::memcmp(lhs.data(), rhs.data(), lhs.size());
    }

    class Transaction {
    public:
        enum class ExecutionResult {
            COMMIT = 0,
            PENDING = -1,
            ABORT = 1,
            ABORT_NO_RETRY = 2
        };
        // Input a user request envelop, return a transaction.
        // The caller need to validate the signature.
        static std::unique_ptr<Transaction> NewTransactionFromEnvelop(std::unique_ptr<Envelop> envelop) {
            std::unique_ptr<Transaction> txn(new Transaction());
            txn->_envelop = std::move(envelop);
            txn->_userRequest = std::make_unique<UserRequest>();
            zpp::bits::in in(txn->_envelop->getPayload());
            if (failure(in(*txn->_userRequest))) {
                LOG(WARNING) << "Deserialize user request failed!";
                return nullptr;
            }
            txn->_executionResult = std::make_unique<TxReadWriteSet>();
            txn->_executionResult->setRequestDigest(txn->_envelop->getSignature().digest);
            txn->tid = std::make_shared<tid_type>(txn->_envelop->getSignature().digest);
            return txn;
        }

        static auto DestroyTransaction(std::unique_ptr<Transaction> txn) {
            return std::make_pair(std::move(txn->_envelop), std::move(txn->_executionResult));
        }

        virtual ~Transaction() = default;

        Transaction(const Transaction&) = delete;

        Transaction(Transaction&&) = delete;

        // Set the txn id through NewTransactionFromEnvelop
        [[nodiscard]] const tid_type& getTransactionId() const {
            DCHECK(tid != nullptr);
            return *tid;
        }

        [[nodiscard]] std::shared_ptr<const tid_type> getTransactionIdPtr() const {
            DCHECK(tid != nullptr);
            return tid;
        }

        [[nodiscard]] const KVList& getReads() const { return _executionResult->getReads(); }

        [[nodiscard]] const KVList& getWrites() const { return _executionResult->getWrites(); }

        [[nodiscard]] KVList& getReads() { return _executionResult->getReads(); }

        [[nodiscard]] KVList& getWrites() { return _executionResult->getWrites(); }

        [[nodiscard]] const UserRequest& getUserRequest() const { return *_userRequest; }

        void setExecutionResult(ExecutionResult er) { _executionResult->setRetCode((int32_t) er); }

        [[nodiscard]] ExecutionResult getExecutionResult() const {
            return static_cast<ExecutionResult>(_executionResult->getRetCode());
        }

        void setRetValue(std::string &&retValue) { _executionResult->setRetValue(std::move(retValue)); }

        [[nodiscard]] const std::string_view &getRetValueSV() const { return _executionResult->getRetValueSV(); }

    protected:
        Transaction() = default;

    private:
        std::shared_ptr<tid_type> tid;
        // Each transaction must contain an envelope
        // Contains the user's raw transaction (serialized and un-serialized)
        std::unique_ptr<Envelop> _envelop;
        std::unique_ptr<UserRequest> _userRequest;
        std::unique_ptr<TxReadWriteSet> _executionResult;
    };
}