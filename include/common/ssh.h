//
// Created by user on 23-3-30.
//

#pragma once

#include <utility>
#include <memory>
#include <functional>


struct ssh_session_struct;
struct ssh_channel_struct;
struct sftp_session_struct;

namespace util {
    class SSHChannel {
    public:
        static std::unique_ptr<SSHChannel> NewSSHChannel(ssh_session_struct* session);

        ~SSHChannel();

        SSHChannel(const SSHChannel&) = delete;

        SSHChannel(SSHChannel&&) = delete;

        bool execute(const std::string& command);

        // When the buffer is not full, wait until timeout occurs
        bool read(std::ostream& buf, int errFlag, const std::function<bool(std::string_view append)>& callback=nullptr);

        void setTimeout(int timeout) { _timeout = timeout; }

    protected:
        SSHChannel() = default;

    private:
        ssh_channel_struct* _channel{};
        int _timeout = 1000;
    };

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

        // remoteFilePath: the file name and the relevant path
        bool getFileToDisk(const std::string& remoteFilePath, const std::string &localFilePath, bool override);

        bool getFileToBuffer(const std::string& remoteFilePath, std::string& buffer);

    protected:
        SFTPSession() = default;

        bool getFileWithCallback(const std::string& remoteFilePath, const std::function<bool(void* data, int size)>& callback);

    private:
        sftp_session_struct* _sftp{};
        ssh_session_struct* _session{};
    };

    class SSHSession {
    public:
        static std::unique_ptr<SSHSession> NewSSHSession(std::string ip, int port=-1);

        ~SSHSession();

        SSHSession(const SSHSession&) = delete;

        SSHSession(SSHSession&&) = delete;

        bool connect(const std::string& user, const std::string& password);

        [[nodiscard]] auto getPort() const { return _port; }

        [[nodiscard]] auto getIp() const { return _ip; }

        auto createChannel() { return SSHChannel::NewSSHChannel(this->_session); }

        auto createSFTPSession() { return SFTPSession::NewSFTPSession(this->_session); }

        bool executeCommand(const std::vector<std::string> &builder, bool printInfo=false);

    protected:
        SSHSession() = default;

        bool verifyKnownHost();

    private:
        ssh_session_struct* _session{};
        std::string _ip;
        int _port = -1;
    };
}