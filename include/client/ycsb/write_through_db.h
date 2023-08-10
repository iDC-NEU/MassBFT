//
// Created by user on 23-8-10.
//

#include "peer/chaincode/chaincode.h"
#include "client/core/db.h"
#include "client/core/status.h"

namespace client::ycsb {
    class WriteThroughDB: public client::core::DB {
    public:
        explicit WriteThroughDB(peer::chaincode::Chaincode* cc) :cc(cc) { }

        void stop() override { }

        client::core::Status sendInvokeRequest(const std::string&, const std::string& func, const std::string& args) override {
            if (cc->InvokeChaincode(func, args) != 0) {
                return client::core::ERROR;
            }
            return client::core::STATUS_OK;
        }

    private:
        peer::chaincode::Chaincode* cc;
    };

}