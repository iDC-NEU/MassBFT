//
// Created by user on 23-7-7.
//
#include "ca/config_initializer.h"
#include "common/property.h"
#include "common/crypto.h"
#include "ycsb/core/common/ycsb_property.h"
#include "common/ssh.h"

namespace ca {

    bool Initializer::initDefaultConfig() {
        if (!util::Properties::LoadProperties()) {
            return false;
        }
        auto* properties = util::Properties::GetProperties();
        auto ccProperties = properties->getChaincodeProperties();
        ccProperties.install("ycsb");
        ccProperties.install("smallbank");
        auto nodeProperties = properties->getNodeProperties();
        auto bccspProperties = properties->getCustomProperties("bccsp");

        util::NodeConfig cfg;
        for (int i=0; i<(int)_groupNodeCount.size(); i++) {
            for (int j=0; j<_groupNodeCount[i]; j++) {
                cfg.groupId = i;
                cfg.nodeId = j;
                cfg.ski = std::to_string(i) + "_" + std::to_string(j);
                cfg.priIp = "127.0." + std::to_string(cfg.groupId) + "." + std::to_string(cfg.nodeId);
                cfg.pubIp = "127.1." + std::to_string(cfg.groupId) + "." + std::to_string(cfg.nodeId);
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
        util::Properties::SetProperties(util::Properties::SSH_USERNAME, "root");
        util::Properties::SetProperties(util::Properties::SSH_PASSWORD, "neu1234.");
        util::Properties::SetProperties(util::Properties::JVM_PATH, "/root/nc_bft/corretto-16.0.2/bin/java");
        util::Properties::SetProperties(util::Properties::RUNNING_PATH, "/root/nc_bft");

        // init ycsb property
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::RECORD_COUNT_PROPERTY, 1000 * 1000);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::OPERATION_COUNT_PROPERTY, 1000 * 20* 30);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 1000 * 20);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::THREAD_COUNT_PROPERTY, 10);
        // ycsb-a example
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::READ_PROPORTION_PROPERTY, 0.50);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::UPDATE_PROPORTION_PROPERTY, 0.50);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::INSERT_PROPORTION_PROPERTY, 0.00);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::SCAN_PROPORTION_PROPERTY, 0.00);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::READMODIFYWRITE_PROPORTION_PROPERTY, 0.00);
        return true;
    }

    Initializer::Initializer(std::vector<int> groupNodeCount)
            : _groupNodeCount(std::move(groupNodeCount)) {
        util::OpenSSLSHA256::initCrypto();
        util::OpenSSLED25519::initCrypto();
    }

    void Initializer::SetLocalId(int groupId, int nodeId) {
        auto* properties = util::Properties::GetProperties();
        auto nodeProperties = properties->getNodeProperties();
        nodeProperties.setLocalNodeInfo(groupId, nodeId);
    }

    bool Initializer::SetNodeIp(int groupId, int nodeId, const std::string &pub, const std::string &pri) {
        auto* properties = util::Properties::GetProperties();
        auto nodeProperties = properties->getNodeProperties();
        auto nodePtr = nodeProperties.getSingleNodeInfo(groupId, nodeId);
        if (nodePtr == nullptr) {
            return false;
        }
        nodePtr->pubIp = pub;
        nodePtr->priIp = pri;
        nodeProperties.setSingleNodeInfo(*nodePtr);
        return true;
    }

    bool Initializer::SaveConfig(const std::string &fileName) {
        return util::Properties::SaveProperties(fileName);
    }

    Dispatcher::Dispatcher(std::filesystem::path runningPath, std::string bftFolderName, std::string ncZipFolderName)
            :_runningPath(std::move(runningPath)),
             _bftFolderName(std::move(bftFolderName)),
             _ncZipFolderName(std::move(ncZipFolderName)) { }

    bool Dispatcher::transmitFileToRemote(const std::string &ip) {
        auto* properties = util::Properties::GetProperties();
        // create session
        auto session = util::SSHSession::NewSSHSession(ip);
        CHECK(session != nullptr);
        auto [userName, password, success] = properties->getSSHInfo();
        if (!success) {
            return false;
        }
        success = session->connect(userName, password);
        if (!success) {
            return false;
        }
        // transmit file
        auto channel = session->createChannel();
        if (channel == nullptr) {
            return false;
        }
        if (!channel->blockingExecute( {"mkdir -p", _runningPath} )) {
            return false;
        }
        // clear the old file
        std::vector<std::string> builder = {
                "cd",
                _runningPath,
                "&&",
                "rm -rf",
                _bftFolderName,
                _bftFolderName.append(".zip"),
                _ncZipFolderName,
                _ncZipFolderName.append(".zip"), };
        channel = session->createChannel();
        if (channel == nullptr) {
            return false;
        }
        if (!channel->blockingExecute(builder)) {
            return false;
        }
        // install unzip
        builder = {
                "echo",
                password,
                "|",
                "sudo -S apt update",
                "&&",
                "sudo apt install unzip openssh-server -y", };
        channel = session->createChannel();
        if (channel == nullptr) {
            return false;
        }
        if (!channel->blockingExecute(builder)) {
            return false;
        }
        // upload the new files
        auto sftp = session->createSFTPSession();
        if (!sftp->putFile(_runningPath / _ncZipFolderName, true, _runningPath / _ncZipFolderName)) {
            return false;
        }
        if (!sftp->putFile(_runningPath / _bftFolderName, true, _runningPath / _bftFolderName)) {
            return false;
        }
        // unzip the files
        builder = {
                "cd",
                _runningPath,
                "&&",
                "unzip -q",
                _runningPath / _bftFolderName,
                "&&",
                "unzip -q",
                _runningPath / _ncZipFolderName, };
        channel = session->createChannel();
        if (channel == nullptr) {
            return false;
        }
        if (!channel->blockingExecute(builder)) {
            return false;
        }
        return true;
    }
}
