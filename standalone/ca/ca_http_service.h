//
// Created by user on 23-8-8.
//

#include "ca/config_initializer.h"
#include "common/phmap.h"

namespace ca {
    class ServiceBackend {
    public:
        static std::unique_ptr<ServiceBackend> NewServiceBackend(std::unique_ptr<ca::Dispatcher> dispatcher);

        ~ServiceBackend();

    public:
        void initNodes(const std::vector<int>& nodes);

        void setNodesIp(int groupId, int nodeId, const std::string &pub, const std::string &pri);

        void addNodeAsClient(int groupId, int nodeId, const std::string &pub);

    public:
        bool transmitFileToRemote() { return processParallel(&ca::Dispatcher::transmitFileToRemote); }

        bool transmitFileToRemote(const std::vector<std::string>& ipList) {
            return processParallel(&ca::Dispatcher::transmitFileToRemote, ipList);
        }

        bool updateRemoteSourcecode() { return processParallel(&ca::Dispatcher::updateRemoteSourcecode); }

        bool updateRemoteSourcecode(const std::vector<std::string>& ipList) {
            return processParallel(&ca::Dispatcher::updateRemoteSourcecode, ipList);
        }

        bool compileRemoteSourcecode() { return processParallel(&ca::Dispatcher::compileRemoteSourcecode); }

        bool compileRemoteSourcecode(const std::vector<std::string>& ipList) {
            return processParallel(&ca::Dispatcher::compileRemoteSourcecode, ipList);
        }

        bool updateProperties();

        bool updateProperties(const std::vector<std::string>& ipList);

        bool generateDatabase(const std::string& dbName) {
            return processParallelPeerOnly(&ca::Dispatcher::generateDatabase, dbName);
        }

        bool backupDatabase() { return processParallelPeerOnly(&ca::Dispatcher::backupRemoteDatabase); }

        bool restoreDatabase() { return processParallelPeerOnly(&ca::Dispatcher::restoreRemoteDatabase); }

    protected:
        ServiceBackend() = default;

        template <typename F, typename... A>
        bool processParallel(F&& task, const std::vector<std::string>& ips, A&&... args) {
            for (int i=0; i<3; i++) {
                if (_dispatcher->processParallel(std::forward<decltype(task)>(task), ips, std::forward<decltype(args)>(args)...)) {
                    return true;
                }
            }
            return false;
        }

        bool processParallel(auto&& func) {
            std::vector<std::string> ipList;
            for (const auto& it: _nodesList) {
                ipList.push_back(it.first);
            }
            return processParallel(std::forward<decltype(func)>(func), ipList);
        }

        template <typename F, typename... A>
        bool processParallelPeerOnly(F&& task, A&&... args) {
            std::vector<std::string> ipList;
            for (const auto& it: _nodesList) {
                if (it.second.isClient) {
                    continue;
                }
                ipList.push_back(it.first);
            }
            return processParallel(std::forward<decltype(task)>(task), ipList, std::forward<decltype(args)>(args)...);
        }

    private:
        struct NodeConfig {
            int groupId;
            int nodeId;
            int isClient;
        };

        util::MyNodeHashMap<std::string, NodeConfig, std::mutex> _nodesList;
        std::unique_ptr<ca::Initializer> _initializer;
        std::unique_ptr<ca::Dispatcher> _dispatcher;
    };
}
