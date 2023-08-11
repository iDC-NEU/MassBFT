//
// Created by user on 23-8-10.
//

#include "peer/chaincode/chaincode.h"
#include "client/core/db.h"
#include "client/core/status.h"

namespace client::core {
    class WriteThroughDB: public client::core::DB {
    public:
        explicit WriteThroughDB(peer::chaincode::Chaincode* cc) :cc(cc) { }

        void stop() override { }

        client::core::Status sendInvokeRequest(const std::string&, const std::string& func, const std::string& args) override {
            if (cc->InvokeChaincode(func, args) != 0) {
                return core::Status(core::Status::State::ERROR, util::Timer::time_now_ms(), std::to_string(nonce++));
            }
            return core::Status(core::Status::State::OK, util::Timer::time_now_ms(), std::to_string(nonce++));
        }

    private:
        peer::chaincode::Chaincode* cc;
        std::atomic<int> nonce;
    };

}