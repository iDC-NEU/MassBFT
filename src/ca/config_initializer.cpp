//
// Created by user on 23-7-7.
//
#include "ca/config_initializer.h"
#include "ycsb/core/common/ycsb_property.h"
#include "common/property.h"
#include "common/crypto.h"
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
        if (pub.empty()) {
            nodePtr->pubIp = pri;
        } else {
            nodePtr->pubIp = pub;
        }
        if (pri.empty()) {
            nodePtr->priIp = pub;
        } else {
            nodePtr->priIp = pri;
        }
        nodeProperties.setSingleNodeInfo(*nodePtr);
        return true;
    }

    Dispatcher::Dispatcher(std::filesystem::path runningPath, std::string bftFolderName, std::string ncZipFolderName)
            : _runningPath(std::move(runningPath)),
              _bftFolderName(std::move(bftFolderName)),
              _ncFolderName(std::move(ncZipFolderName)) { }

    bool Dispatcher::transmitFileToRemote(const std::string &ip) const {
        auto session = Connect(ip);
        if (session == nullptr) {
            return false;
        }
        LOG(INFO) << "Cleaning the old files.";
        // transmit file
        if (!session->executeCommand({"mkdir -p", _runningPath}, true)) {
            return false;
        }
        auto bftZipName = _bftFolderName + ".zip";
        auto ncZipName = _ncFolderName + ".zip";
        // clear the old file
        std::vector<std::string> builder = {
                "cd",
                _runningPath,
                "&&",
                "rm -rf",
                _bftFolderName,
                bftZipName,
                _ncFolderName,
                ncZipName, };
        if (!session->executeCommand(builder, true)) {
            return false;
        }
        LOG(INFO) << "Installing dependencies.";
        // install unzip, load the password
        auto* properties = util::Properties::GetProperties();
        auto [userName, password, success] = properties->getSSHInfo();
        if (!success) {
            return false;
        }
        builder = {
                "echo",
                password,
                "|",
                "sudo -S apt update",
                "&&",
                "sudo apt install unzip -y", };
        if (!session->executeCommand(builder, true)) {
            return false;
        }
        LOG(INFO) << "Uploading sourcecode.";
        // upload the new files
        auto sftp = session->createSFTPSession();
        if (!sftp->putFile(_runningPath / ncZipName, true, _runningPath / ncZipName)) {
            return false;
        }
        LOG(INFO) << "Uploading nc_bft.";
        if (!sftp->putFile(_runningPath / bftZipName, true, _runningPath / bftZipName)) {
            return false;
        }
        LOG(INFO) << "Unzip sourcecode and nc_bft.";
        // unzip the files
        builder = {
                "cd",
                _runningPath,
                "&&",
                "unzip -q",
                _runningPath / _bftFolderName,
                "&&",
                "unzip -q",
                _runningPath / _ncFolderName, };
        if (!session->executeCommand(builder, true)) {
            return false;
        }
        return true;
    }

    void Dispatcher::overrideProperties() {
        auto peerRunningPath = _runningPath / _bftFolderName;
        auto jvmPath = peerRunningPath / "corretto-16.0.2/bin/java";
        util::Properties::SetProperties(util::Properties::JVM_PATH, jvmPath.string());
        util::Properties::SetProperties(util::Properties::RUNNING_PATH, peerRunningPath.string());
    }

    bool Dispatcher::transmitPropertiesToRemote(const std::string &ip) const {
        auto session = Connect(ip);
        if (session == nullptr) {
            return false;
        }
        auto configFilename = "peer_cfg_" + ip + "_tmp";
        if (!util::Properties::SaveProperties(configFilename)) {
            return false;
        }
        LOG(INFO) << "Uploading config file.";
        // upload the new files
        auto sftp = session->createSFTPSession();
        if (!sftp->putFile(_runningPath / _bftFolderName / "peer.yaml", true, configFilename)) {
            return false;
        }
        return true;
    }

    std::unique_ptr<util::SSHSession> Dispatcher::Connect(const std::string &ip) {
        auto* properties = util::Properties::GetProperties();
        // create session
        auto session = util::SSHSession::NewSSHSession(ip);
        if (session == nullptr) {
            return nullptr;
        }
        auto [userName, password, success] = properties->getSSHInfo();
        if (!success) {
            return nullptr;
        }
        success = session->connect(userName, password);
        if (!success) {
            return nullptr;
        }
        return session;
    }

    bool Dispatcher::transmitFileParallel(const std::vector<std::string> &ips) const {
        volatile bool success = true;
        std::vector<std::thread> threads;
        threads.reserve(ips.size());
        for (const auto& it: ips) {
            threads.emplace_back([&, ip=it] {
                auto ret = transmitFileToRemote(ip);
                if (!ret) {
                    LOG(WARNING) << "Send files to " << ip << " failed!";
                    success = false;
                }
            });
        }
        for (auto& it: threads) {
            it.join();
        }
        return success;
    }
}
