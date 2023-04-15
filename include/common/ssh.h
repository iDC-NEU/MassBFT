//
// Created by user on 23-3-30.
//

#pragma once

#include <utility>
#include <memory>
#include <functional>

#include "sftp.h"

struct ssh_session_struct;
struct ssh_channel_struct;

namespace util {
    class SSHChannel {
    public:
        static std::unique_ptr<SSHChannel> NewSSHChannel(ssh_session_struct* session);

        ~SSHChannel();

        SSHChannel(const SSHChannel&) = delete;

        SSHChannel(SSHChannel&&) = delete;

        bool execute(const std::string &command);

        bool read(std::string& buf, int errFlag, const std::function<bool(std::string_view append)>& callback=nullptr);

        void setTimeout(int timeout) { _timeout = timeout; }

    protected:
        SSHChannel() = default;

    private:
        ssh_channel_struct* _channel{};
        int _timeout = 1000;
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

    protected:
        SSHSession() = default;

        bool verifyKnownHost();

    private:
        ssh_session_struct* _session{};
        std::string _ip;
        int _port = -1;
    };
}