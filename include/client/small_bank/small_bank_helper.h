//
// Created by user on 23-8-13.
//

#pragma once

#include <string>

namespace client::small_bank {
    using AccountIDType = int;
    struct TableNamesPrefix {
        inline static constexpr std::string_view ACCOUNTS = "a_";
        inline static constexpr std::string_view SAVINGS = "s_";
        inline static constexpr std::string_view CHECKING = "c_";
    };

    // REF: https://ses.library.usyd.edu.au/bitstream/handle/2123/5353/michael-cahill-2009-thesis.pdf
    struct InvokeRequestType {
        inline static constexpr auto SMALL_BANK = "sb";
        // a parameterized transaction that represents calculating the total balance for a customer with name N.
        // It returns the sum of savings and checking balances for the specified customer.
        inline static constexpr auto BALANCE = "bal";
        // is a parameterized transaction that represents making a deposit into the checking account of a customer.
        // Its operation is to increase the checking balance by V for the given customer.
        // If the value V is negative or if the name N is not found in the table, the transaction will roll back.
        inline static constexpr auto DEPOSIT_CHECKING = "dc";
        // represents making a deposit or withdrawal on the savings account.
        // It increases or decreases the savings balance by V for the specified customer.
        // If the name N is not found in the table or
        // if the transaction would result in a negative savings balance for the customer,
        // the transaction will roll back.
        inline static constexpr auto TRANSACT_SAVING = "ts";
        // represents moving all the funds from one customer to another.
        // It reads the balances for both accounts of customer N1, then sets both to zero,
        // and finally increases the checking balance for N2 by the sum of N1’s previous balances.
        inline static constexpr auto AMALGAMATE = "amg";
        // represents writing a check against an account.
        // Its operation is evaluated the sum of savings and checking balances for the given customer.
        // If the sum is less than V, it decreases the checking balance by V + 1
        // (reflecting a penalty of $1 for overdrawing), otherwise it decreases the checking balance by V.
        inline static constexpr auto WRITE_CHECK = "wc";
    };

    class StaticConfig {
    public:
        constexpr static const auto MAX_BALANCE = 50000;
        constexpr static const auto MIN_BALANCE = 10000;
        // https://github.com/hyperledger/sawtooth-core/blob/f149a029dc86317c080504656c81a71a1bf83a0f/perf/smallbank_workload/src/playlist.rs#L272C13-L277C15
        constexpr static const auto Account_NAME_LENGTH = 20;
    };
}