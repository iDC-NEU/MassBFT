//
// Created by user on 23-7-10.
//

#include <string>
#include <unordered_map>
#include <optional>
#include "glog/logging.h"

namespace util {
    class ArgParser {
    public:
        // Parse command line arguments
        ArgParser(int argc, char* argv[]) {
            for (int i = 1; i < argc; ++i) {
                std::string_view arg(argv[i]);
                if (arg.find('=') != std::string::npos) {
                    // Option with value in the format "option=value"
                    size_t pos = arg.find('=');
                    std::string option = std::string(arg.substr(0, pos));
                    std::string value = std::string(arg.substr(pos + 1));
                    _options[option] = value;
                } else if (isOption(arg)) {
                    // Option without value (flag)
                    _options[std::string(arg)] = "true";
                }
                LOG(WARNING) << "Invalid option: " << arg;
            }
        }

        // Add a command line option with a default value
        void addOption(const std::string& option, const std::string& defaultValue = {}) {
            _options[option] = defaultValue;
        }

        // Get the value of a command line option
        std::optional<std::string> getOption(const std::string& option) const {
            auto it = _options.find(option);
            if (it != _options.end()) {
                return it->second;
            }
            return std::nullopt;
        }

    private:
        // Check if a string is a valid option
        static inline bool isOption(auto&& str) {
            return !str.empty() && str[0] == '-';
        }

        std::unordered_map<std::string, std::string> _options;
    };
}