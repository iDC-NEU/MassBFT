//
// Created by user on 23-4-21.
//

#include "ca/bft_instance_controller.h"

#include <utility>
#include <libssh/libssh.h>
#include "glog/logging.h"


#include "common/ssh.h"

std::unique_ptr<util::BFTInstanceController> util::BFTInstanceController::NewBFTInstanceController(std::string ip, int port) {
    std::unique_ptr<util::BFTInstanceController> bftInstanceController(new util::BFTInstanceController());
    bftInstanceController->_ip = std::move(ip);
    bftInstanceController->_port = port;
    return bftInstanceController;
}

util::BFTInstanceController::~BFTInstanceController() {
    _ip = "";
    _port = -1;
}

bool util::BFTInstanceController::StartInstance(const std::string &command, int processId){
    auto session = SSHSession::NewSSHSession(_ip, _port);
    auto channel = session->createChannel();

    return channel->execute(command + std::to_string(processId));
}