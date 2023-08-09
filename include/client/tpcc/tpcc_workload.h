//
// Created by user on 23-8-9.
//

#pragma once

#include "client/tpcc/tpcc_request.h"

namespace client::tpcc {
    class TPCCWorkload {
    public:
        NewOrder buildNewOrder();

    };
}