//
// Created by user on 23-6-28.
//

#pragma once

#include <thread>
#include "tests/mock_property_generator.h"
#include "common/bccsp.h"
#include "common/yaml_key_storage.h"
#include "proto/block.h"

namespace tests::peer {
    class Peer {
    public:
        Peer(const util::Properties &p) {
            auto node = p.getCustomPropertiesOrPanic("bccsp");
            bccsp = std::make_unique<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(node));
            CHECK(bccsp) << "Can not init bccsp";
        }

    protected:
        bool validateSignature() {

        }

    private:
        std::unique_ptr<util::BCCSP> bccsp;
        std::unique_ptr<std::thread> _collectorThread;
        std::unique_ptr<std::thread> _rpcThread;
    };
}