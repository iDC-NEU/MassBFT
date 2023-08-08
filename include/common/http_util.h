//
// Created by user on 23-8-8.
//

#include "httplib.h"
#include "nlohmann/json.hpp"

namespace util {
    inline void setErrorWithMessage(httplib::Response &res, auto&& message) {
        nlohmann::json ret;
        ret["success"] = false;
        ret["message"] = message;
        res.status = 400;
        res.set_content(ret.dump(), "application/json");
    }

    inline void setSuccessWithMessage(httplib::Response &res, auto&& message) {
        nlohmann::json ret;
        ret["success"] = true;
        ret["message"] = message;
        res.set_content(ret.dump(), "application/json");
    }

    std::pair<bool, nlohmann::json> parseJson(auto&& raw) {
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(std::forward<decltype(raw)>(raw));
        } catch (const nlohmann::json::parse_error &e) {
            return {false, json};
        }
        return {true, json};
    }

    template<class T>
    std::pair<bool, std::vector<T>> getListFromJson(const nlohmann::json& json) {
        if (json.empty()) {
            return {true, std::vector<T>{}};
        }
        std::vector<T> tList;
        try {
            for (const auto& it: json) {
                tList.emplace_back(it);
            }
        } catch (const nlohmann::json::parse_error &e) {
            return {false, std::vector<T>{}};
        }
        return {true, tList};
    }

}