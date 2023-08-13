//
// Created by user on 23-8-9.
//

#pragma once

namespace client::ycsb {
    struct InvokeRequestType {
        constexpr static const auto YCSB = "ycsb";
        constexpr static const auto UPDATE = "u";
        constexpr static const auto INSERT = "i";
        constexpr static const auto READ = "r";
        constexpr static const auto DELETE = "d";
        constexpr static const auto SCAN = "s";
        constexpr static const auto READ_MODIFY_WRITE = "m";
    };
}