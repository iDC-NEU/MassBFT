//
// Created by peng on 11/6/22.
//

#pragma once

#include <utility>
#include <string>

namespace ycsb::core {
    class Status {
    public:
        enum class State {
            OK,
            BATCHED_OK,
            ERROR,
            NOT_FOUND,
            NOT_IMPLEMENTED,
            UNEXPECTED_STATE,
        };

        explicit Status(State name) : name(name), timeMs(0) { }

        explicit Status(State name, uint64_t timeNowMs, auto&& rhs) : name(name), timeMs(timeNowMs), digest(std::forward<decltype(rhs)>(rhs)) { }

        Status(const Status& rhs) = default;

        virtual ~Status() = default;

        [[nodiscard]] bool isOk() const {
            return this->name == State::OK || this->name == State::BATCHED_OK;
        }

        [[nodiscard]] const auto& getName() const { return name; }

        [[nodiscard]] const auto& getGenTimeMs() const { return timeMs; }

        [[nodiscard]] bool operator==(const Status& rhs) const {
            return this->name == rhs.name;
        }

        [[nodiscard]] const auto& getDigest() const { return digest; }

    private:
        State name;
        uint64_t timeMs;
        std::string digest;
    };

    static const auto STATUS_OK = Status(Status::State::OK);
    static const auto ERROR = Status(Status::State::ERROR);
    static const auto NOT_FOUND = Status(Status::State::NOT_FOUND);
    static const auto NOT_IMPLEMENTED = Status(Status::State::NOT_IMPLEMENTED);
    static const auto UNEXPECTED_STATE = Status(Status::State::UNEXPECTED_STATE);
    static const auto BATCHED_OK = Status(Status::State::BATCHED_OK);
}