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

    // create sftp_session
    auto sftp = session->createSFTPSession();
    ASSERT_TRUE(sftp != nullptr);
    auto hosts_config = sftp->readConfig("/home/user/nc_bft/config/hosts.config");
    ASSERT_TRUE(hosts_config.data());
    ret = sftp->writeConfig(hosts_config, "/tmp/hosts.config");
    ASSERT_TRUE(ret);
    auto system_config = sftp->readConfig("/home/user/nc_bft/config/system.config");
    ASSERT_TRUE(system_config.data());
    ret = sftp->writeConfig(system_config, "/tmp/system.config");
    ASSERT_TRUE(ret);


    std::string out, err;
//    ret = channel->execute("cp -r /home/user/nc_bft/config /tmp");
//    ASSERT_TRUE(ret);
    ret = channel->execute("/home/user/.jdks/openjdk-20/bin/java -Dlogback.configurationFile=/home/user/nc_bft/config/logback.xml -classpath /home/user/nc_bft/nc_bft.jar bftsmart.demo.neuchainplus.NeuChainServer 0");
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