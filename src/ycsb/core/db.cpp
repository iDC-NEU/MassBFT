//
// Created by user on 23-5-25.
//

#include "ycsb/core/db.h"
#include "ycsb/neuchain_db.h"
#include "common/zmq_port_util.h"
#include "common/yaml_key_storage.h"

namespace ycsb::core {
    DB::~DB() = default;

    DBStatus::~DBStatus() = default;

    DBFactory::DBFactory(const util::Properties &p) {
        server = p.getNodeProperties().getLocalNodeInfo();
        // calculate port
        portConfig = util::ZMQPortUtil::InitLocalPortsConfig(p);
        // init bccsp
        auto node = p.getCustomPropertiesOrPanic("bccsp");
        bccsp = std::make_unique<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(node));
        CHECK(bccsp) << "Can not init bccsp";
        auto port = portConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[server->nodeId];
        dbc = ::ycsb::client::NeuChainDBConnection::NewNeuChainDBConnection(server->priIp, port);
    }

    std::unique_ptr<DB> DBFactory::newDB() const {
        auto priKey = bccsp->GetKey(server->ski);
        if (!priKey || !priKey->Private()) {
            return nullptr;
        }
        return std::make_unique<client::NeuChainDB>(server, dbc, std::move(priKey));
    }

    std::unique_ptr<DBStatus> DBFactory::newDBStatus() const {
        auto priKey = bccsp->GetKey(server->ski);
        if (!priKey || !priKey->Private()) {
            return nullptr;
        }
        auto port = portConfig->getLocalServicePorts(util::PortType::BFT_RPC)[server->nodeId];
        return std::make_unique<client::NeuChainStatus>(server, port, std::move(priKey));
    }

    DBFactory::~DBFactory() = default;
}