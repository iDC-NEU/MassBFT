#pragma once

#include <string>
#include <functional>
#include <memory>
#include <thread>
#include <mutex>
#include <queue>

class HotStuffApp;

namespace hotstuff {
    class HotStuffService {
    public:
        HotStuffService(int nodeId,
                        const std::function<void(const std::string& proposal, const std::string& hash)>& verifyCallback,
                        const std::function<void(const std::string& hash)>& commitCallback,
                        const std::function<std::string()>& pushProposalCallback = nullptr);

        void pushProposal(const std::string& proposal);

        ~HotStuffService();

    protected:
        int initHotStuffApp(int argc, char **argv);

    private:
        std::string nodeIdStr;
        std::string nodeIdConfigStr;
        std::unique_ptr<HotStuffApp> papp;

        std::function<void(const std::string& proposal, const std::string& hash)> verifyCallback;
        std::function<void(const std::string& hash)> commitCallback;
        std::function<std::string()> pushProposalCallback;

        std::thread pappThread;
        std::thread clientThread;
        std::mutex pendingRequestMutex;
        std::queue<std::string> pendingRequests;
    };

}