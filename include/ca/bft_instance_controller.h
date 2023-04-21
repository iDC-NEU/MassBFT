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
        static std::unique_ptr<BFTInstanceController> NewBFTInstanceController(std::string ip, int port=-1);

        ~BFTInstanceController();

        BFTInstanceController(const BFTInstanceController&) = delete;

        BFTInstanceController(BFTInstanceController&&) = delete;

        bool StartInstance(const std::string &command, int processId);

    protected:
        BFTInstanceController() = default;

    private:
        std::string _ip;
        int _port = -1;
    };
}
