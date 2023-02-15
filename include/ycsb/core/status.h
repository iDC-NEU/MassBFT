//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_STATUS_H
#define NEUCHAIN_PLUS_STATUS_H

#include <utility>
#include <string>

namespace ycsb::core {
    class Status {
    public:
        Status(std::string name, std::string description)
                : name(std::move(name)), description(std::move(description)){ }

        Status(const Status& rhs) = default;

        virtual ~Status() = default;
        [[nodiscard]] bool isOk () const {
            return this->name == "OK" || this->name == "BATCHED_OK";
        }
        [[nodiscard]] const auto& getName() const {
            return name;
        }
        [[nodiscard]] const auto& getDescription() const {
            return description;
        }
        [[nodiscard]] bool operator==(const Status& rhs) const {
            return this->name == rhs.name;
        }
    private:
        std::string name, description;
    };

    static const auto STATUS_OK = Status("OK", "The operation completed successfully.");
    static const auto ERROR = Status("ERROR", "The operation failed.");
    static const auto NOT_FOUND = Status("NOT_FOUND", "The requested record was not found.");
    static const auto NOT_IMPLEMENTED = Status("NOT_IMPLEMENTED", "The operation is not implemented for the current binding.");
    static const auto UNEXPECTED_STATE = Status("UNEXPECTED_STATE", "The operation reported success, but the result was not as expected.");
    static const auto BAD_REQUEST = Status("BAD_REQUEST", "The request was not valid.");
    static const auto FORBIDDEN = Status("FORBIDDEN", "The operation is forbidden.");
    static const auto SERVICE_UNAVAILABLE = Status("SERVICE_UNAVAILABLE", "Dependant service for the current binding is not available.");
    static const auto BATCHED_OK = Status("BATCHED_OK", "The operation has been batched by the binding to be executed later.");
}
#endif //NEUCHAIN_PLUS_STATUS_H
