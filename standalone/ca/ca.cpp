//
// Created by user on 23-7-4.
//

#include "ca/config_initializer.h"

int main(int argc, char *argv[]) {
    ca::Initializer i({4, 7, 4});
    i.initDefaultConfig();
    ca::Initializer::SetLocalId(1, 5);
    ca::Initializer::SaveConfig("cfg.yaml");
    return 0;
}