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
        auto session = connect(ip);
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
                "export DEBIAN_FRONTEND=noninteractive",
                "&&",
                "echo",
                password,
                "|",
                "sudo -S apt update",
                "&&",
                "sudo apt install zip unzip git cmake libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev g++ openssh-server -y", };
        if (!session->executeCommand(builder, true)) {
            return false;
        }
        if (!updateRemoteSourcecode(ip)) {
            return false;
        }
        if (!updateRemoteBFTPack(ip)) {
            return false;
        }
        return true;
    }

    bool Dispatcher::remoteCompileSystem(const std::string &ip) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return false;
        }
        LOG(INFO) << "Stopping proxy.";
        if (!session->executeCommand({ "kill -9 $(pidof clash-linux-amd64-v3)" }, true)) {
            return false;
        }
        LOG(INFO) << "Sleep for 5 seconds.";
        std::this_thread::sleep_for(std::chrono::seconds(5));
        LOG(INFO) << "Starting proxy.";
        std::vector<std::string> builder = {
                "cd",
                _runningPath / _bftFolderName,
                "&&",
                "chmod +x clash-linux-amd64-v3",
                "&&",
                "./clash-linux-amd64-v3 -f proxy.yaml", };
        auto clashChannel = session->executeCommandNoWait(builder);
        LOG(INFO) << "Sleep for 5 seconds.";
        std::this_thread::sleep_for(std::chrono::seconds(5));
        LOG(INFO) << "Configure and installing nc_bft.";
        builder = {
                "cd",
                _runningPath / _ncFolderName,
                "&&",
                "export https_proxy=http://127.0.0.1:7890 && export http_proxy=http://127.0.0.1:7890 && export all_proxy=socks5://127.0.0.1:7890",
                "&&",
                "cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/gcc-11 -DCMAKE_CXX_COMPILER=/usr/bin/g++-11 -B build",
                "&&",
                "cmake --build build --target NBPStandalone_peer NBPStandalone_ycsb NBPStandalone_ca -j", };
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
        auto session = connect(ip);
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
        if (!sftp->putFile(_runningPath / _bftFolderName / "peer.yaml", true, std::filesystem::current_path() / configFilename)) {
            return false;
        }
        return true;
    }

    bool Dispatcher::generateDatabase(const std::string &ip, const std::string &chaincodeName) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return false;
        }
        LOG(INFO) << "Generating database.";
        auto peerExecFull = _runningPath / _ncFolderName / "build" / "standalone" / _peerExecName;
        std::vector<std::string> builder = {
                "cd",
                _runningPath / _bftFolderName,
                "&&",
                peerExecFull,
                "-i=" + chaincodeName, };
        if (!session->executeCommand(builder, true)) {
            return false;
        }
        return true;
    }

    util::SSHSession * Dispatcher::connect(const std::string &ip) const {
        std::unique_lock lock(createMutex);
        util::SSHSession* ret = nullptr;
        if (sessionPool.if_contains(ip, [&](const auto &v) { ret = v.second.get(); })) {
            return ret;
        }
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
        sessionPool[ip] = std::move(session);
        return sessionPool[ip].get();
    }

    template<typename Func>
    void Dispatcher::processParallel(Func f, int count) const {
        std::vector<std::thread> threads;
        threads.reserve(count);
        for (int i=0; i<count; i++) {
            threads.emplace_back(f, i);
        }
        for (auto& it: threads) {
            it.join();
        }
    }

    bool Dispatcher::transmitFileParallel(const std::vector<std::string> &ips, bool send, bool compile) const {
        volatile bool success = true;
        processParallel([&] (int idx) {
            const auto& ip = ips.at(idx);
            if (send) {
                auto ret = transmitFileToRemote(ip);
                if (!ret) {
                    LOG(WARNING) << "Send files to " << ip << " failed!";
                    success = false;
                }
            }
            if (compile) {
                auto ret = remoteCompileSystem(ip);
                if (!ret) {
                    LOG(WARNING) << "Compile system at " << ip << " failed!";
                    success = false;
                }
            }
        }, (int)ips.size());
        return success;
    }

    bool Dispatcher::generateDatabaseParallel(const std::vector<std::string> &ips, const std::string &chaincodeName) const {
        volatile bool success = true;
        processParallel([&] (int idx) {
            const auto& ip = ips.at(idx);
            auto ret = generateDatabase(ip, chaincodeName);
            if (!ret) {
                LOG(WARNING) << "generateDatabase to " << ip << " failed!";
                success = false;
            }
        }, (int)ips.size());
        return success;
    }

    std::unique_ptr<util::SSHChannel> Dispatcher::startPeer(const std::string &ip) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return nullptr;
        }
        LOG(INFO) << "Starting peer.";
        auto peerExecFull = _runningPath / _ncFolderName / "build" / "standalone" / _peerExecName;
        std::vector<std::string> builder = {
                "cd",
                _runningPath / _bftFolderName,
                "&&",
                peerExecFull, };
        return session->executeCommandNoWait(builder);
    }

    std::unique_ptr<util::SSHChannel> Dispatcher::startUser(const std::string &ip) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return nullptr;
        }
        LOG(INFO) << "Starting user.";
        auto userExecFull = _runningPath / _ncFolderName / "build" / "standalone" / _userExecName;
        std::vector<std::string> builder = {
                "cd",
                _runningPath / _bftFolderName,
                "&&",
                userExecFull, };
        return session->executeCommandNoWait(builder);
    }

    std::vector<std::unique_ptr<util::SSHChannel>> Dispatcher::startPeerParallel(const std::vector<std::string> &ips) const {
        volatile bool success = true;
        std::vector<std::unique_ptr<util::SSHChannel>> peerList;
        peerList.resize(ips.size());
        processParallel([&] (int idx) {
            const auto& ip = ips.at(idx);
            peerList[idx] = startPeer(ip);
            if (peerList[idx] == nullptr) {
                LOG(WARNING) << "Start peer ip: " << ip << " failed!";
                success = false;
            }
        }, (int)ips.size());
        return peerList;
    }

    bool Dispatcher::updateRemoteSourcecode(const std::string &ip) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return false;
        }
        LOG(INFO) << "Uploading sourcecode.";
        // upload the new files
        auto sftp = session->createSFTPSession();
        auto ncZipName = _ncFolderName + ".zip";
        if (!sftp->putFile(_runningPath / ncZipName, true, _runningPath / ncZipName)) {
            return false;
        }
        LOG(INFO) << "Unzip sourcecode.";
        // unzip the files
        std::vector<std::string> builder = {
                "cd",
                _runningPath,
                "&&",
                "unzip -q -o",
                _runningPath / _ncFolderName, };
        if (!session->executeCommand(builder, true)) {
            return false;
        }
        return true;
    }

    bool Dispatcher::updateRemoteBFTPack(const std::string &ip) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return false;
        }
        LOG(INFO) << "Uploading nc_bft.";
        // upload the new files
        auto sftp = session->createSFTPSession();
        auto bftZipName = _bftFolderName + ".zip";
        if (!sftp->putFile(_runningPath / bftZipName, true, _runningPath / bftZipName)) {
            return false;
        }
        LOG(INFO) << "Unzip nc_bft.";
        // unzip the files
        std::vector<std::string> builder = {
                "cd",
                _runningPath,
                "&&",
                "unzip -q -o",
                _runningPath / _bftFolderName, };
        if (!session->executeCommand(builder, true)) {
            return false;
        }
        return true;
    }

    bool Dispatcher::backupRemoteDatabase(const std::string &ip) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return false;
        }
        LOG(INFO) << "Backup peer data.";
        std::vector<std::string> builder = {
                "cp -r -f",
                _runningPath / _bftFolderName / "data",
                _runningPath / _bftFolderName / "data_bk", };
        if (!session->executeCommand(builder, true)) {
            LOG(ERROR) << "Backup peer data failed.";
        }
        return true;
    }

    bool Dispatcher::restoreRemoteDatabase(const std::string &ip) const {
        auto session = connect(ip);
        if (session == nullptr) {
            return false;
        }
        LOG(INFO) << "Clear peer data.";
        std::vector<std::string> builder = {
                "rm -rf ",
                _runningPath / _bftFolderName / "data",
                "&&",
                "cp -r -f",
                _runningPath / _bftFolderName / "data_bk",
                _runningPath / _bftFolderName / "data", };
        if (!session->executeCommand(builder, true)) {
            LOG(ERROR) << "Clear peer data failed.";
        }
        return true;
    }

    Dispatcher::~Dispatcher() = default;
}
