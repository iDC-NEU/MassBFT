//
// Created by user on 23-4-16.
//

#pragma once

#include "common/ssh.h"
#include "common/timer.h"
#include <filesystem>
#include <fstream>

#include "glog/logging.h"

namespace ca {
    struct SSHConfig {
        std::string ip;
        int port=-1;
        std::string userName;
        std::string password;
    };

    struct NodeHostConfig {
        int processId;
        std::string ip;
        int serverToServerPort;
        int serverToClientPort;
        int rpcPort;
    };

    class BFTInstanceController {
    public:
        // Try to establish an ssh session with remote host
        static std::unique_ptr<BFTInstanceController> NewBFTInstanceController(SSHConfig config,
                                                                               int groupId,
                                                                               int processId,
                                                                               std::filesystem::path runningPath,
                                                                               std::filesystem::path jvmExec) {
            std::unique_ptr<BFTInstanceController> controller(new BFTInstanceController(std::move(config), groupId, processId));
            auto session = util::SSHSession::NewSSHSession(controller->_config.ip, controller->_config.port);
            if(session == nullptr) {
                return nullptr;
            }
            if(!session->connect(controller->_config.userName, controller->_config.password)) {
                return nullptr;
            }
            controller->_session = std::move(session);
            controller->_runningPath = std::move(runningPath);
            controller->_jvmExec = std::move(jvmExec);
            return controller;
        }

        bool prepareConfigurationFile(const std::vector<NodeHostConfig>& nodes) {
            auto remoteFilePath = _runningPath / "config" / ("hosts_" + std::to_string(_groupId) + "_" + std::to_string(_processId) + ".config");
            std::ofstream file(remoteFilePath, std::ios::out | std::ios::trunc);
            if (!file.is_open()) {
                LOG(INFO) << "Failed to create file: " << remoteFilePath;
                return false;
            }
            for (const auto& node: nodes) {
                file << node.processId << " "
                     << node.ip << " "
                     << node.serverToServerPort << " "
                     << node.serverToClientPort << " "
                     << node.rpcPort << std::endl;
            }
            file.close();
            return true;
        }

        BFTInstanceController(const BFTInstanceController &) = delete;

        BFTInstanceController(BFTInstanceController &&) = delete;

        // leave the hostConfigPath empty of you don't want to overwrite the file
        bool startInstance(const std::filesystem::path& hostConfigPath) {
            if (!hostConfigPath.empty()) {
                // transmit config file to remote server
                LOG(INFO) << "Transmitting files to remote instance " << _processId;
                auto remoteFilePath = _runningPath / "config" / ("hosts_" + std::to_string(_groupId) + "_" + std::to_string(_processId) + ".config");
                auto sftp = _session->createSFTPSession();
                if (sftp == nullptr) {
                    return false;
                }
                if (!sftp->putFile(remoteFilePath, true, hostConfigPath)) {
                    return false;
                }
            }
            // 2. start the server
            LOG(INFO) << "Preparing to start instance:, groupId: " << _groupId << ", process id:" << _processId;
            std::vector<std::string> builder {
                    "cd",
                    _runningPath.string(),
                    "&&",
                    _jvmExec.string(),
                    "-Dlogback.configurationFile=./config/logback.xml",
                    "-classpath",
                    "./nc_bft.jar bftsmart.demo.neuchainplus.NeuChainServer",
                    std::to_string(_groupId),
                    std::to_string(_processId),
            };
            std::string command;
            for (const auto& it: builder) {
                command.append(it).append(" ");
            }
            DLOG(INFO) << "The execute command: " << command;
            auto channel = this->_session->createChannel();
            if(channel == nullptr) {
                return false;
            }
            _bftInstanceChannel = std::move(channel);
            startTime = util::Timer::time_now_ms();
            return _bftInstanceChannel->execute(command);
        }

        // return isSuccess, stdout, stderr
        std::tuple<bool, std::string, std::string> getChannelResponse(int timedWait=15) {
            auto callback = [&](std::string_view sv) {
                auto spanMs = startTime + timedWait * 1000 - util::Timer::time_now_ms();
                util::Timer::sleep_ns(spanMs * 1000 * 1000);
                if (sv.empty()) {
                    return false;
                }
                LOG(INFO) << sv;
                return true;
            };
            std::string out, error;
            if (_bftInstanceChannel == nullptr) {
                return {false, {}, {}};
            }
            // do not catch the return value!
            _bftInstanceChannel->read(out, false, callback);
            _bftInstanceChannel->read(error, true, callback);
            return {true, out, error};
        }

        void stopAndClean() {
            auto channel = _session->createChannel();
            if (!channel) {
                return;
            }
            channel->execute("pkill -f nc_bft.jar");
            std::string out;
            channel->read(out, false);

            channel = _session->createChannel();
            if (!channel) {
                return;
            }
            std::vector<std::string> builder {
                    "cd",
                    _runningPath / "config",
                    "&&",
                    "rm",
                    "hosts_" + std::to_string(_processId) + ".config",
                    "currentView",
            };
            std::string command;
            for (const auto& it: builder) {
                command.append(it).append(" ");
            }
            channel->execute(command);
        }

        [[nodiscard]] const auto& getConfig() const { return _config; };

    protected:
        explicit BFTInstanceController(auto&& config, int groupId, int processId)
                : _config(std::forward<decltype(config)>(config)), _groupId(groupId), _processId(processId) { }

    private:
        const SSHConfig _config;
        const int _groupId;
        const int _processId;
        int64_t startTime = -1;
        std::unique_ptr<util::SSHSession> _session;
        std::unique_ptr<util::SSHChannel> _bftInstanceChannel;
        std::filesystem::path _runningPath;
        std::filesystem::path _jvmExec;
    };
}
