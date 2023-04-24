//
// Created by user on 23-4-16.
//

#pragma once

#include "common/ssh.h"
#include "common/timer.h"
#include <filesystem>

#include "glog/logging.h"

namespace ca {
    struct SSHConfig {
        std::string ip;
        int port=-1;
        std::string userName;
        std::string password;
    };

    class BFTInstanceController {
    public:
        // Try to establish an ssh session with remote host
        static std::unique_ptr<BFTInstanceController> NewBFTInstanceController(SSHConfig config,
                                                                               int processId,
                                                                               std::filesystem::path runningPath,
                                                                               std::filesystem::path jvmExec) {
            std::unique_ptr<BFTInstanceController> controller(new BFTInstanceController(std::move(config), processId));
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

        BFTInstanceController(const BFTInstanceController &) = delete;

        BFTInstanceController(BFTInstanceController &&) = delete;

        bool startInstance(const std::filesystem::path& hostConfigPath) {
            // transmit config file to remote server
            LOG(INFO) << "Transmitting files to remote instance " << _processId;
            auto remoteFilePath = _runningPath / "config" / ("hosts_" + std::to_string(_processId) + ".config");
            auto sftp = _session->createSFTPSession();
            if (sftp == nullptr) {
                return false;
            }
            if (!sftp->putFile(remoteFilePath, true, hostConfigPath)) {
                return false;
            }
            // 2. start the server
            LOG(INFO) << "Preparing to start instance " << _processId;
            std::vector<std::string> builder {
                    "cd",
                    _runningPath.string(),
                    "&&",
                    _jvmExec.string(),
                    "-Dlogback.configurationFile=./config/logback.xml",
                    "-classpath",
                    "./nc_bft.jar bftsmart.demo.neuchainplus.NeuChainServer",
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
            waitUntil = util::Timer::time_now_ms() + 10 * 1000;
            return _bftInstanceChannel->execute(command);
        }

        // return isSuccess, stdout, stderr
        std::tuple<bool, std::string, std::string> getChannelResponse() {
            auto callback = [this](std::string_view sv) {
                auto spanMs = waitUntil - util::Timer::time_now_ms();
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

        [[nodiscard]] const auto& getConfig() const { return _config; };

    protected:
        explicit BFTInstanceController(auto&& config, int processId)
                : _config(std::forward<decltype(config)>(config)), _processId(processId) { }

    private:
        const SSHConfig _config;
        const int _processId;
        int64_t waitUntil = -1;
        std::unique_ptr<util::SSHSession> _session;
        std::unique_ptr<util::SSHChannel> _bftInstanceChannel;
        std::filesystem::path _runningPath;
        std::filesystem::path _jvmExec;
    };
}
