//
// Created by user on 23-7-7.
//

#pragma once

#include "common/phmap.h"
#include "common/ssh.h"
#include <vector>
#include <string>
#include <filesystem>
#include <thread>

namespace ca {
    /* The initializer is responsible for initializing the public and private keys of the node
     * and generating the default configuration file.
     * In subsequent versions, the initializer will also be responsible for tasks
     * such as distributing binary files. */
    class Initializer {
    public:
        explicit Initializer(std::vector<int> groupNodeCount);

        static void SetLocalId(int groupId, int nodeId);

        static bool SetNodeIp(int groupId, int nodeId, const std::string& pub, const std::string& pri);

        bool initDefaultConfig();

    private:
        const std::vector<int> _groupNodeCount;
    };

    class Dispatcher {
    public:
        Dispatcher(std::filesystem::path runningPath,
                   std::string bftFolderName,
                   std::string ncZipFolderName);

        ~Dispatcher();

    public:
        [[nodiscard]] bool transmitFileToRemote(const std::string &ip) const;

        [[nodiscard]] bool transmitPropertiesToRemote(const std::string &ip) const;

        [[nodiscard]] bool compileRemoteSourcecode(const std::string &ip) const;

        [[nodiscard]] bool generateDatabase(const std::string &ip, const std::string& chaincodeName) const;

        [[nodiscard]] std::unique_ptr<util::SSHChannel> startPeer(const std::string &ip) const;

        [[nodiscard]] std::unique_ptr<util::SSHChannel> startUser(const std::string &ip) const;

        [[nodiscard]] bool updateRemoteSourcecode(const std::string &ip) const;

        [[nodiscard]] bool updateRemoteBFTPack(const std::string &ip) const;

        [[nodiscard]] bool backupRemoteDatabase(const std::string &ip) const;

        [[nodiscard]] bool restoreRemoteDatabase(const std::string &ip) const;

        bool stopAll(const std::string& ip) const;

    public:
        void overrideProperties();

        void setUserExecName(auto&& rhs) { _userExecName = std::forward<decltype(rhs)>(rhs); }

        void setPeerExecName(auto&& rhs) { _peerExecName = std::forward<decltype(rhs)>(rhs); }

        template <typename F, typename... A>
        bool processParallel(F&& task, const std::vector<std::string>& ips, A&&... args) {
            volatile bool success = true;
            std::vector<std::thread> threads;
            threads.reserve(ips.size());
            for (const auto & ip : ips) {
                threads.emplace_back([&, ip=ip]() {
                    auto ret = (this->*task)(ip, std::forward<A>(args)...);
                    if (!ret) {
                        success = false;
                    }
                });
            }
            for (auto& it: threads) {
                it.join();
            }
            return success;
        }

    protected:
        util::SSHSession * connect(const std::string &ip) const;

    private:
        // The local and remote node share the same running path
        std::filesystem::path _runningPath;
        std::string _bftFolderName;
        std::string _ncFolderName;

        std::string _peerExecName = "peer";
        std::string _userExecName = "ycsb";

        mutable std::mutex createMutex;
        mutable util::MyFlatHashMap<std::string, std::unique_ptr<util::SSHSession>> sessionPool;
    };
}