//
// Created by user on 23-3-30.
//

#include "common/ssh.h"
#include "common/timer.h"

#include "gtest/gtest.h"
#include "glog/logging.h"


class SSHTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    // return value is not null
    static auto NewSSHSession() {
        auto session = util::SSHSession::NewSSHSession(remoteIp);
        CHECK(session != nullptr);
        auto ret = session->connect(userName, passWord);
        CHECK(ret == true);
        return session;
    }

protected:
    constexpr static auto userName = "user";
    constexpr static auto passWord = "123456";
    constexpr static auto remoteIp = "127.0.0.1";
    inline static std::string runningPath = "/home/user/nc_bft/";

    static bool readChannelCallback(std::string_view sv) {
        util::Timer::sleep_sec(1);
        if (sv.empty()) {
            return false;
        }
        LOG(INFO) << sv;
        return true;
    };

};

TEST_F(SSHTest, IntrgrateTest) {
    auto session = NewSSHSession();
    auto channel = session->createChannel();
    ASSERT_TRUE(channel != nullptr);
    std::string out, err;
    auto ret = channel->execute("ls");
    ASSERT_TRUE(ret);
    auto cb = [](std::string_view sv) {
        if (sv.empty()) {
            return false;
        }
        LOG(INFO) << sv;
        return true;
    };
    channel->read(out, false, cb);
    channel->read(err, true, cb);
}

TEST_F(SSHTest, SendFileToRemote) {
    auto session = NewSSHSession();
    auto hostConfigPath = runningPath + "config/hosts.config";
    for (int i=0; i<4; i++) {
        auto remoteFilePath = runningPath + "config/hosts_" + std::to_string(i) + ".config";
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

    for (int i=0; i<(int)channelList.size(); i++) {
        auto ret = channelList[i]->execute("cd " + runningPath + "&&" + jvmPath.append(jvmOption).append(classPath) + "bftsmart.demo.neuchainplus.NeuChainServer " + std::to_string(i));
        ASSERT_TRUE(ret);
    }
    std::string out, error;
    channelList[0]->read(out, false, readChannelCallback);
    channelList[0]->read(error, true, readChannelCallback);
    sleep(10);
}

TEST_F(SSHTest, ReadTest) {
    auto session = NewSSHSession();
    for (int i=0; i<4; i++) {
        std::string data;
        auto remoteFilePath = runningPath + "config/hosts_" + std::to_string(i) + ".config";
        auto it = session->createSFTPSession();
        ASSERT_TRUE(it != nullptr);
        ASSERT_TRUE(it->getFileToBuffer(remoteFilePath, data));
        ASSERT_TRUE(!data.empty());
    }
}

TEST_F(SSHTest, StopTest) {
    auto session = NewSSHSession();
    auto stop_channel = session->createChannel();
    ASSERT_TRUE(stop_channel != nullptr);
    stop_channel->execute("pkill -f nc_bft.jar");
    std::string out, error;
    stop_channel->read(out, false, readChannelCallback);
    stop_channel->read(error, true, readChannelCallback);
}
