//
// Created by user on 23-7-7.
//

#pragma once

#include "common/phmap.h"
#include "common/ssh.h"
#include <vector>
#include <string>
#include <filesystem>

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

        [[nodiscard]] bool remoteCompileSystem(const std::string &ip) const;

        [[nodiscard]] bool generateDatabase(const std::string &ip, const std::string& chaincodeName) const;

        [[nodiscard]] std::unique_ptr<util::SSHChannel> startPeer(const std::string &ip) const;

        [[nodiscard]] std::unique_ptr<util::SSHChannel> startUser(const std::string &ip) const;

        [[nodiscard]] bool updateRemoteSourcecode(const std::string &ip) const;

        [[nodiscard]] bool updateRemoteBFTPack(const std::string &ip) const;

        [[nodiscard]] bool backupRemoteDatabase(const std::string &ip) const;

        [[nodiscard]] bool restoreRemoteDatabase(const std::string &ip) const;

    public:
        void overrideProperties();

        void setUserExecName(auto&& rhs) { _userExecName = std::forward<decltype(rhs)>(rhs); }

        void setPeerExecName(auto&& rhs) { _peerExecName = std::forward<decltype(rhs)>(rhs); }

        // Note: caller must not transmit prop concurrently
        [[nodiscard]] bool transmitPropertiesToRemote(const std::string &ip) const;

        [[nodiscard]] bool transmitFileParallel(const std::vector<std::string>& ips, bool send=true, bool compile=false) const;

        [[nodiscard]] bool generateDatabaseParallel(const std::vector<std::string> &ips, const std::string& chaincodeName) const;

        [[nodiscard]] std::vector<std::unique_ptr<util::SSHChannel>> startPeerParallel(const std::vector<std::string> &ips) const;

    protected:
        util::SSHSession * connect(const std::string &ip) const;

        template<typename Func>
        void processParallel(Func f, int count) const;

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