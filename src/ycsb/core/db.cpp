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
        auto ports = util::ZMQPortUtil::InitPortsConfig(p);
        CHECK(ports->contains(server->groupId) && (int)ports->at(server->groupId).size() > server->nodeId) << "index out of range!";
        portConfig = std::move(ports->at(server->groupId).at(server->nodeId));
        // init bccsp
        auto node = p.getCustomPropertiesOrPanic("bccsp");
        bccsp = std::make_unique<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(node));
        CHECK(bccsp) << "Can not init bccsp";
    }

    std::unique_ptr<DB> DBFactory::newDB() const {
        auto priKey = bccsp->GetKey(server->ski);
        if (!priKey || !priKey->Private()) {
            return nullptr;
        }
        auto port = portConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[server->nodeId];
        return std::make_unique<client::NeuChainDB>(server, port, std::move(priKey));
    }

    std::unique_ptr<DBStatus> DBFactory::newDBStatus() const {
        auto priKey = bccsp->GetKey(server->ski);
        if (!priKey || !priKey->Private()) {
            return nullptr;
        }
        auto port = portConfig->getLocalServicePorts(util::PortType::CLIENT_TO_SERVER)[server->nodeId];
        return std::make_unique<client::NeuChainStatus>(server, port, std::move(priKey));
    }

    DBFactory::~DBFactory() = default;
}