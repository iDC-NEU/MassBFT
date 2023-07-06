//
// Created by user on 23-7-4.
//

#include "common/property.h"
#include "common/crypto.h"
#include "ycsb/core/common/ycsb_property.h"

/* The initializer is responsible for initializing the public and private keys of the node
 * and generating the default configuration file.
 * In subsequent versions, the initializer will also be responsible for tasks
 * such as distributing binary files. */
class Initializer {
public:
    explicit Initializer(std::vector<int> groupNodeCount)
            : _groupNodeCount(std::move(groupNodeCount)) {
        util::OpenSSLSHA256::initCrypto();
        util::OpenSSLED25519::initCrypto();
    }

    static void SetLocalId(int groupId, int nodeId) {
        auto* properties = util::Properties::GetProperties();
        auto nodeProperties = properties->getNodeProperties();
        nodeProperties.setLocalNodeInfo(groupId, nodeId);
    }

    bool initDefaultConfig() {
        if (!util::Properties::LoadProperties()) {
            return false;
        }
        auto* properties = util::Properties::GetProperties();
        auto nodeProperties = properties->getNodeProperties();
        auto bccspProperties = properties->getCustomProperties("bccsp");

        util::NodeConfig cfg;
        for (int i=0; i<(int)_groupNodeCount.size(); i++) {
            for (int j=0; j<_groupNodeCount[i]; j++) {
                cfg.groupId = i;
                cfg.nodeId = j;
                cfg.ski = std::to_string(i) + "_" + std::to_string(j);
                cfg.priIp = "{pri_ip_" + cfg.ski + "}";
                cfg.pubIp = "{pub_ip_" + cfg.ski + "}";
                nodeProperties.setSingleNodeInfo(cfg);

                // generate private keys
                auto ret = util::OpenSSLED25519::generateKeyFiles({}, {}, {});
                if(!ret) {
                    return false;
                }

                auto [pub, pri] = std::move(*ret);
                auto signer = util::OpenSSLED25519::NewFromPemString(pri, {});
                auto rawPriHex = signer->getHexFromPrivateKey();
                YAML::Node node;
                node["raw"] = OpenSSL::bytesToString(*rawPriHex);
                node["private"] = true;
                bccspProperties[cfg.ski] = node;
            }
        }

        // init properties
        SetLocalId(0, 0);
        util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 1000);
        util::Properties::SetProperties(util::Properties::BATCH_TIMEOUT_MS, 1000);
        util::Properties::SetProperties(util::Properties::DISTRIBUTED_SETTING, true);
        util::Properties::SetProperties(util::Properties::SSH_USERNAME, "user");
        util::Properties::SetProperties(util::Properties::SSH_PASSWORD, "123456");
        util::Properties::SetProperties(util::Properties::JVM_PATH, "/home/user/.jdks/corretto-16.0.2/bin/java");
        util::Properties::SetProperties(util::Properties::RUNNING_PATH, "/home/user/nc_bft");

        // init ycsb property
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::RECORD_COUNT_PROPERTY, 10000);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::OPERATION_COUNT_PROPERTY, 1000 * 20* 30);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 1008 * 20);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::THREAD_COUNT_PROPERTY, 10);
        // ycsb-a example
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::READ_PROPORTION_PROPERTY, 0.50);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::UPDATE_PROPORTION_PROPERTY, 0.50);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::INSERT_PROPORTION_PROPERTY, 0.00);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::SCAN_PROPORTION_PROPERTY, 0.00);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::READMODIFYWRITE_PROPORTION_PROPERTY, 0.00);
        return true;
    }

    static bool SaveConfig(auto&& fileName) {
        return util::Properties::SaveProperties(fileName);
    }

private:
    const std::vector<int> _groupNodeCount;
};

int main(int argc, char *argv[]) {
    Initializer i({4, 7, 4});
    i.initDefaultConfig();
    Initializer::SetLocalId(1, 5);
    Initializer::SaveConfig("cfg.yaml");
    return 0;
}