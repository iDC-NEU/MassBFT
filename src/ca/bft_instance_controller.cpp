//
// Created by user on 23-4-21.
//

#include "ca/bft_instance_controller.h"

#include <utility>
#include "glog/logging.h"
#include "common/ssh.h"

std::unique_ptr<util::BFTInstanceController> util::BFTInstanceController::NewBFTInstanceController(const std::string& ip, const std::string& user, const std::string &password,  int port) {
    std::unique_ptr<util::BFTInstanceController> bftInstanceController(new util::BFTInstanceController());
    auto session = SSHSession::NewSSHSession(ip, port);
    if(session == nullptr){
        LOG(ERROR) << "open session error. ";
        return nullptr;
    }
    auto ret = session->connect(user, password);
    if(!ret){
        LOG(ERROR) << "session connection error. ";
    }

    bftInstanceController->_session = std::move(session);
    bftInstanceController->_channels.resize(4);
    bftInstanceController->_ip = ip;
    bftInstanceController->_user = user;
    bftInstanceController->_port = port;

    return bftInstanceController;
}

bool util::BFTInstanceController::StartInstance(const std::string &command, int processId) {
    auto channel = this->_session->createChannel();
    if(channel == nullptr){
        LOG(ERROR) << "open channel error. ";
        return false;
    }

    this->_channels.insert(_channels.begin() + processId, std::move(channel));
    return this->_channels.at(processId)->execute(command + std::to_string(processId));
}

bool util::BFTInstanceController::readFeedback(std::string& buf, int errFlag, const std::function<bool(std::string_view append)>& callback, int processId) {
        return this->_channels.at(processId)->read(buf, errFlag, callback);
}