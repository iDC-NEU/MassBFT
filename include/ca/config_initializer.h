//
// Created by user on 23-7-7.
//

#pragma once

#include <vector>
#include <string>
#include <filesystem>

namespace util {
    class SSHSession;
}

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

        [[nodiscard]] bool transmitFileToRemote(const std::string &ip) const;

        [[nodiscard]] bool remoteCompileSystem(const std::string &ip) const;

        void overrideProperties();

        // Note: caller must not transmit prop concurrently
        [[nodiscard]] bool transmitPropertiesToRemote(const std::string &ip) const;

        [[nodiscard]] bool transmitFileParallel(const std::vector<std::string>& ips, bool send=true, bool compile=false) const;

    protected:
        static std::unique_ptr<util::SSHSession> Connect(const std::string &ip);

    private:
        // The local and remote node share the same running path
        std::filesystem::path _runningPath;
        std::string _bftFolderName;
        std::string _ncFolderName;
    };
}