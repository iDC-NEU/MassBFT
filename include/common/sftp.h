//
// Created by user on 23-4-15.
//

#pragma once

#include <utility>
#include <memory>
#include <functional>
#include <optional>

#include "ssh.h"

struct ssh_session_struct;
struct ssh_channel_struct;
struct sftp_session_struct;

namespace util {
    class SFTPSession {
    public:
        static std::unique_ptr<SFTPSession> NewSFTPSession(ssh_session_struct* session);

        ~SFTPSession();

        SFTPSession(const SFTPSession&) = delete;

        SFTPSession(SFTPSession&&) = delete;

        void printError() const;

        // remoteFilePath: if you want to store the file in remote working path please set with "./fileName"
        bool putFile(const std::string& remoteFilePath, bool override, void* data, int size);

        // remoteFilePath: if you want to store the file in remote working path please set with "./fileName"
        bool putFile(const std::string& remoteFilePath, bool override, const std::string& localFilePath);

        bool setWorkingDirectory(const std::string& remotePath);

        // remoteFilePath: the file name and the relevant path
        std::optional<std::string> getFileToMemory(const std::string& remoteFilePath);

        // localPath: if you want to store the file in current running path please set with "./"
        bool getFileToDisk(const std::string& remoteFilePath, const std::string& localPath);

    protected:
        SFTPSession() = default;

    private:
        sftp_session_struct* _sftp{};
        ssh_session_struct* _session{};

    };
}