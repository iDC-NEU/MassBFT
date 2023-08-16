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
        bool transmitFiles(const std::vector<std::string>& ipList) {
            if (ipList.empty()) {
                return processParallel(&ca::Dispatcher::transmitFileToRemote);
            }
            return processParallel(&ca::Dispatcher::transmitFileToRemote, ipList);
        }

        bool updateSourcecode(const std::vector<std::string>& ipList) {
            if (ipList.empty()) {
                return processParallel(&ca::Dispatcher::updateRemoteSourcecode);
            }
            return processParallel(&ca::Dispatcher::updateRemoteSourcecode, ipList);
        }

        bool compileSourcecode(const std::vector<std::string>& ipList) {
            if (ipList.empty()) {
                return processParallel(&ca::Dispatcher::compileRemoteSourcecode);
            }
            return processParallel(&ca::Dispatcher::compileRemoteSourcecode, ipList);
        }

        bool updatePBFTPack(const std::vector<std::string>& ipList) {
            if (ipList.empty()) {
                return processParallel(&ca::Dispatcher::updateRemoteBFTPack);
            }
            return processParallel(&ca::Dispatcher::updateRemoteBFTPack, ipList);
        }

    protected:
        void updateProperties();

    public:
        void updateProperties(const std::vector<std::string>& ipList);

        bool generateDatabase(const std::string& dbName) {
            return processParallelPeerOnly(&ca::Dispatcher::generateDatabase, dbName);
        }

        bool backupDatabase() { return processParallelPeerOnly(&ca::Dispatcher::backupRemoteDatabase); }

        bool restoreDatabase() { return processParallelPeerOnly(&ca::Dispatcher::restoreRemoteDatabase); }

        bool startPeer() { return processParallelPeerOnly(&ca::Dispatcher::startPeer); }

        bool stopPeer() { return processParallelPeerOnly(&ca::Dispatcher::stopPeer); }

        bool startUser(const std::string& dbName) {
            return processParallelClientOnly(&ca::Dispatcher::startUser, dbName);
        }

    protected:
        ServiceBackend() = default;

        template <typename F, typename... A>
        bool processParallel(F&& task, const std::vector<std::string>& ips, A&&... args) {
            std::stringstream ss;
            std::for_each(ips.begin(), ips.end(), [&](const auto& t) { ss << t << ", "; });
            LOG(INFO) << "Apply tasks to remote nodes: " << "{" << ss.str() << "}";
            if (!_dispatcher->processParallel(std::forward<decltype(task)>(task), ips, std::forward<decltype(args)>(args)...)) {
                LOG(INFO) << "Apply tasks failed, please retry later.";
                return false;
            }
            return true;
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

        template <typename F, typename... A>
        bool processParallelClientOnly(F&& task, A&&... args) {
            std::vector<std::string> ipList;
            for (const auto& it: _nodesList) {
                if (!it.second.isClient) {
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
