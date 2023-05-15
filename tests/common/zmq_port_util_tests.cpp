//
// Created by user on 23-3-20.
//

#include "common/zmq_port_util.h"
#include "common/matrix_2d.h"

#include "gtest/gtest.h"

class ZMQPortUtilTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};


TEST_F(ZMQPortUtilTest, IntrgrateTest4_4) {
    std::unordered_map<int, int> regionsNodeCount;
    regionsNodeCount[0] = 4;
    regionsNodeCount[1] = 5;
    regionsNodeCount[2] = 6;

    int portOffset = 0;
    util::Matrix2D<std::unique_ptr<util::ZMQPortUtil>> matrix(3, 6);
    for (const auto& it: regionsNodeCount) {
        for (int i=0; i<it.second; i++) {
            matrix(it.first, i) = util::ZMQPortUtil::NewZMQPortUtil(regionsNodeCount, it.first, i, portOffset, false);
        }
    }
    std::vector<bool> portOpen(65536);
    for (const auto& it: regionsNodeCount) {
        for (int i=0; i<it.second; i++) {
            auto stsPort = matrix(it.first, i)->getLocalServicePorts(util::PortType::SERVER_TO_SERVER)[i];
            auto ctsPort = matrix(it.first, i)->getLocalServicePorts(util::PortType::CLIENT_TO_SERVER)[i];
            auto rcPort = matrix(it.first, i)->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[i];
            auto bpPort = matrix(it.first, i)->getLocalServicePorts(util::PortType::BFT_PAYLOAD)[i];
            auto brPort = matrix(it.first, i)->getLocalServicePorts(util::PortType::BFT_RPC)[i];
            auto frList = matrix(it.first, i)->getRemoteServicePorts(util::PortType::LOCAL_FRAGMENT_BROADCAST);
            auto rfrList = matrix(it.first, i)->getRemoteServicePorts(util::PortType::REMOTE_FRAGMENT_RECEIVE);

            for (auto port: {stsPort, ctsPort, rcPort, bpPort, brPort}) {
                if (portOpen[port] != false) {
                    CHECK(false) << "port re-open!";
                }
                portOpen[port] = true;
            }

            for (auto port: frList) {
                if (portOpen[port.second] != false) {
                    CHECK(false) << "port re-open!";
                }
                portOpen[port.second] = true;
            }

            for (auto port: rfrList) {
                if (portOpen[port.second] != false) {
                    CHECK(false) << "port re-open!";
                }
                portOpen[port.second] = true;
            }
        }
    }
}