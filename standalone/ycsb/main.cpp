//
// Created by peng on 11/6/22.
//

#include "ycsb/engine.h"

int main(int, char *[]) {
    auto* p = util::Properties::GetProperties();
    ycsb::YCSBEngine engine(*p);
    engine.startTest();
    return 0;
}