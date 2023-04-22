//
// Created by user on 23-4-16.
//

#pragma once

#include <utility>
#include <memory>
#include <functional>

#include "common/ssh.h"


namespace util{
    class BFTInstanceController{
    public:
        // prepare necessary message for connection to host
        static std::unique_ptr<BFTInstanceController> NewBFTInstanceController(const std::string& ip, const std::string& user, const std::string &password,  int port=-1);

        ~BFTInstanceController() = default;

        BFTInstanceController(const BFTInstanceController&) = delete;

        BFTInstanceController(BFTInstanceController&&) = delete;

        // start specific host by processId
        bool StartInstance(const std::string &command, int processId);

        // read feedback from specific instance by processId
        bool readFeedback(std::string& buf, int errFlag, const std::function<bool(std::string_view append)>& callback, int processId);

        [[nodiscard]] std::string getIP() const { return this->_ip; };

        [[nodiscard]] int getPort() const { return this->_port; };

        [[nodiscard]] std::string getUser() const { return this->_user; };

    protected:
        BFTInstanceController() = default;

    private:
        std::unique_ptr<SSHSession> _session;
        // _channel store the channel at the processId(value) position
        std::vector<std::unique_ptr<SSHChannel>> _channels;
        std::string _ip;
        int _port = -1;
        std::string _user;
    };
}
