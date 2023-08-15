//
// Created by user on 23-8-15.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/core/write_through_db.h"
#include "tests/mock_property_generator.h"

namespace tests {
    class MockStatus: public client::core::DBStatus {
    public:
        std::unique_ptr<::proto::Block> getBlock(int) override { return nullptr; }

        bool connect(int, int) override  { return true; }

        bool getTop(int& blockNumber, int, int) override  {
            blockNumber = -1;
            return true;
        }
    };

    class MockDBFactory : public client::core::DBFactory {
    public:
        explicit MockDBFactory(const util::Properties &n, const std::string& chaincodeName)
                : client::core::DBFactory(n) {
            db = peer::db::DBConnection::NewConnection("ChaincodeTestDB");
            CHECK(db != nullptr) << "failed to init db!";
            auto orm = peer::chaincode::ORM::NewORMFromDBInterface(db);
            chaincode = peer::chaincode::NewChaincodeByName(chaincodeName, std::move(orm));
            CHECK(chaincode != nullptr) << "failed to init chaincode!";
        }

        [[nodiscard]] std::unique_ptr<client::core::DB> newDB() const override {
            initDatabase();
            return std::make_unique<client::core::WriteThroughDB>(chaincode.get());
        }

        [[nodiscard]] std::unique_ptr<client::core::DBStatus> newDBStatus() const override {
            return std::make_unique<MockStatus>();
        }

        void initDatabase() const {
            CHECK(chaincode->InitDatabase() == 0);
            ::proto::KVList reads, writes;
            chaincode->reset(reads, writes);
            CHECK(db->syncWriteBatch([&](auto* batch) ->bool {
                for (const auto& it: writes) {
                    batch->Put({it->getKeySV().data(), it->getKeySV().size()}, {it->getValueSV().data(), it->getValueSV().size()});
                }
                return true;
            }));
        }

        std::shared_ptr<peer::db::DBConnection> db;
        std::unique_ptr<peer::chaincode::Chaincode> chaincode;
    };

}