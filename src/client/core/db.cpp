//
// Created by user on 23-5-25.
//

#include "client/core/db.h"
#include "client/neuchain_db.h"
#include "client/neuchain_dbc.h"
#include "common/zmq_port_util.h"
#include "common/yaml_key_storage.h"

namespace client::core {
    DB::~DB() = default;

    DBStatus::~DBStatus() = default;

    DBFactory::DBFactory(const util::Properties &p) {
        queryServer = p.getNodeProperties().getLocalNodeInfo();
        invokeServer = p.getNodeProperties().getLocalNodeInfo();
        // For Steward (1 of 2)
        // invokeServer = p.getNodeProperties().getGroupNodesInfo(0)[0];
        // queryServer->groupId = 0;
        // calculate port
        portConfig = util::ZMQPortUtil::InitLocalPortsConfig(p);
        // init bccsp
        auto node = p.getCustomPropertiesOrPanic("bccsp");
        bccsp = std::make_unique<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(node));
        CHECK(bccsp) << "Can not init bccsp";
        auto port = portConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[invokeServer->nodeId];
        // For Steward (1 of 2)
        // dbc = ::client::NeuChainDBConnection::NewNeuChainDBConnection(invokeServer->pubIp, port);
        dbc = ::client::NeuChainDBConnection::NewNeuChainDBConnection(invokeServer->priIp, port);
    }

    std::unique_ptr<DB> DBFactory::newDB() const {
        auto priKey = bccsp->GetKey(invokeServer->ski);
        if (!priKey || !priKey->Private()) {
            return nullptr;
        }
        return std::make_unique<client::NeuChainDB>(invokeServer, dbc, std::move(priKey));
    }

    std::unique_ptr<DBStatus> DBFactory::newDBStatus() const {
        auto priKey = bccsp->GetKey(queryServer->ski);
        if (!priKey || !priKey->Private()) {
            return nullptr;
        }
        auto port = portConfig->getLocalServicePorts(util::PortType::BFT_RPC)[queryServer->nodeId];
        return std::make_unique<client::NeuChainStatus>(queryServer, port, std::move(priKey));
    }

    DBFactory::~DBFactory() = default;
}