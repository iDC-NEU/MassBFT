//
// Created by user on 23-3-30.
//

#include "common/ssh.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class SSHTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

TEST_F(SSHTest, IntrgrateTest) {
    auto session = util::SSHSession::NewSSHSession("127.0.0.1");
    ASSERT_TRUE(session != nullptr);
    auto ret = session->connect("user", "123456");
    ASSERT_TRUE(ret);
    auto channel = session->createChannel();
    ASSERT_TRUE(channel != nullptr);

    std::string out, err;
    ret = channel->execute("/home/user/.jdks/openjdk-20/bin/java -Dlogback.configurationFile=/home/user/nc_bft/config/logback.xml -classpath /home/user/nc_bft/main.jar bftsmart.demo.neuchainplus.NeuChainServer 0");
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