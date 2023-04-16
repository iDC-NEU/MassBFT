//
// Created by user on 23-4-15.
//

#include "common/ssh.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class SFTPTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

TEST_F(SFTPTest, TransTest) {
    auto session = util::SSHSession::NewSSHSession("127.0.0.1");
    ASSERT_TRUE(session != nullptr);
    auto ret = session->connect("user", "123456");
    ASSERT_TRUE(ret);
    std::string runningPath = "/home/user/nc_bft/";
    auto hostConfigPath = "/home/user/nc_bft/config/hosts.config";
    for (int i=0; i<4; i++) {
        auto remoteFilePath = "/tmp/hosts_" + std::to_string(i) + ".config";
        auto it = session->createSFTPSession();
        ASSERT_TRUE(it->putFile(remoteFilePath, true, hostConfigPath));
    }

    std::vector<std::unique_ptr<util::SSHChannel>> channelList(4);
    for (auto& it: channelList) {
        it = session->createChannel();
        ASSERT_TRUE(it != nullptr);
    }

    std::string jvmPath = "/home/user/.jdks/openjdk-20/bin/java ";
    std::string jvmOption = "-Dlogback.configurationFile=./config/logback.xml ";
    std::string classPath = "-classpath ./nc_bft.jar ";

    auto cb = [](std::string_view sv) {
        if (sv.empty()) {
            return true;
        }
        LOG(INFO) << sv;
        return true;
    };

    for (int i=0; i<(int)channelList.size(); i++) {
        ret = channelList[i]->execute("cd " + runningPath + "&&" + jvmPath.append(jvmOption).append(classPath) + "bftsmart.demo.neuchainplus.NeuChainServer " + std::to_string(i));
        ASSERT_TRUE(ret);
    }
    std::string out, error;
    channelList[0]->read(out, false, cb);
    channelList[0]->read(error, true, cb);
    sleep(3600);
}

TEST_F(SFTPTest, ReadTest){
    auto session = util::SSHSession::NewSSHSession("127.0.0.1");
    ASSERT_TRUE(session != nullptr);
    auto ret = session->connect("user", "123456");
    ASSERT_TRUE(ret);

    std::string remote_path = "/home/user/nc_bft/";
    std::string local_path = "/home/user/nc_bft/tmp/";

    for (int i=0; i<4; i++) {
        auto remoteFilePath = remote_path + "hosts_" + std::to_string(i) + ".config";
        auto localFilePath = local_path + "hosts_" + std::to_string(i) + ".config";

        auto it = session->createSFTPSession();
        ASSERT_TRUE(it != nullptr);
        ASSERT_TRUE(it->getFileToLocal(remoteFilePath, localFilePath, true));
    }
}

TEST_F(SFTPTest, StopTest){
    auto session = util::SSHSession::NewSSHSession("127.0.0.1");
    ASSERT_TRUE(session != nullptr);
    auto ret = session->connect("user", "123456");
    ASSERT_TRUE(ret);

    auto stop_channel = session->createChannel();
    ASSERT_TRUE(stop_channel != nullptr);
    stop_channel->execute("pkill -f nc_bft.jar");
    ASSERT_TRUE(stop_channel);
}