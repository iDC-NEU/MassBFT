//
// Created by user on 23-3-30.
//

#include "common/ssh.h"
#include <libssh/libssh.h>
#include <libssh/sftp.h>
#include <fstream>
#include <fcntl.h>
#include "glog/logging.h"


std::unique_ptr<util::SSHChannel> util::SSHChannel::NewSSHChannel(ssh_session_struct *session) {
    auto channel = ssh_channel_new(session);
    if (channel == nullptr) {
        return nullptr;
    }
    // Assign value
    std::unique_ptr<util::SSHChannel> sshChannel(new util::SSHChannel());
    sshChannel->_channel = channel;
    if (ssh_channel_open_session(sshChannel->_channel) != SSH_OK) {
        LOG(ERROR) << "ssh channel open session error.";
        return nullptr;
    }
    return sshChannel;
}


util::SSHChannel::~SSHChannel() {
    if (_channel == nullptr) {
        return;
    }
    if (ssh_channel_is_eof(_channel) != 1) {
        ssh_channel_request_send_signal(_channel, "KILL");
        ssh_channel_send_eof(_channel);
    }
    if (ssh_channel_is_open(_channel) == 1) {
        ssh_channel_close(_channel);
    }
    ssh_channel_free(_channel);
}

bool util::SSHChannel::read(std::string& buf, int errFlag, const std::function<bool(std::string_view append)>& callback) {
    buf.clear();
    auto outBytes = 0;
    do {
        buf.resize(outBytes + 1024);
        auto currentOut = ssh_channel_read_timeout(_channel, buf.data() + outBytes, buf.size() - outBytes, errFlag, _timeout);
        if (callback) {
            if (!callback(std::string_view(buf.data() + outBytes, currentOut))) {
                return true;    // user cancel it
            }
        } else {
            if (currentOut == 0) {
                break;
            }
        }
        if (currentOut < 0) {
            LOG(ERROR) << "Read response error openssh.";
            break;
        }
        outBytes += currentOut;
    } while(true);
    buf.resize(outBytes);
    return !buf.empty();
}

bool util::SSHChannel::execute(const std::string &command) {
    if (ssh_channel_request_exec(_channel, command.data()) != SSH_OK) {
        return false;
    }
    return true;
}

std::unique_ptr<util::SFTPSession> util::SFTPSession::NewSFTPSession(ssh_session_struct* session) {
    auto sftp = sftp_new(session);
    if(sftp == nullptr) {
        LOG(ERROR) << "Error allocating SFTP session: " << ssh_get_error(session);
        return nullptr;
    }
    std::unique_ptr<util::SFTPSession> sftpSession(new util::SFTPSession);
    sftpSession->_session = session;
    sftpSession->_sftp = sftp;
    // SFTP protocol initialization
    if (sftp_init(sftpSession->_sftp) != SSH_OK) {
        // analyze sftpError
        sftpSession->printError();
        return nullptr;
    }
    return sftpSession;
}

util::SFTPSession::~SFTPSession() {
    if(_sftp != nullptr){
        sftp_free(_sftp);
    }
}

void util::SFTPSession::printError() const {
    // analyze the sftpError type when it occurs
    switch (sftp_get_error(_sftp)) {
        case SSH_FX_OK:
            break;
        case SSH_FX_EOF:
            LOG(ERROR) << " end-of-file encountered.";
            break;
        case SSH_FX_NO_SUCH_FILE:
            LOG(ERROR) << " file dos not exist.";
            break;
        case SSH_FX_PERMISSION_DENIED:
            LOG(ERROR) << " permission denied.";
            break;
        case SSH_FX_FAILURE:
            LOG(ERROR) << " generic failure.";
            break;
        case SSH_FX_BAD_MESSAGE:
            LOG(ERROR) << "garbage received from server.";
            break;
        case SSH_FX_NO_CONNECTION:
            LOG(ERROR) << "no connection has been set up.";
            break;
        case SSH_FX_CONNECTION_LOST:
            LOG(ERROR) << "there was a connection, but we lost it.";
            break;
        case SSH_FX_OP_UNSUPPORTED:
            LOG(ERROR) << " operation not supported by libssh yet.";
            break;
        case SSH_FX_INVALID_HANDLE:
            LOG(ERROR) << " invalid file handle.";
            break;
        case SSH_FX_NO_SUCH_PATH:
            LOG(ERROR) << " no such file or directory path exists.";
            break;
        case SSH_FX_FILE_ALREADY_EXISTS:
            LOG(ERROR) << " an attempt to create an already existing file or directory has been made.";
            break;
        case SSH_FX_WRITE_PROTECT:
            LOG(ERROR) << " write-protected filesystem.";
            break;
        case SSH_FX_NO_MEDIA:
            LOG(ERROR) << "  no media was in remote drive.";
            break;
    }
}

bool util::SFTPSession::putFile(const std::string &remoteFilePath, bool override, const std::string &localFilePath) {
    // then, read the file to memory
    std::ifstream fin(localFilePath, std::ios::binary);
    if (!fin) {
        LOG(ERROR) << "can't open file for reading: " << strerror(errno);
        return false;
    }
    // get file size in bytes
    fin.seekg(0, std::ios::end);
    auto size = fin.tellg();
    // allocate a buffer
    std::string buf;
    buf.resize(size);
    // rewind to beginning of file
    fin.seekg(0);
    // read file contents into buffer
    fin.read(buf.data(), size);
    return putFile(remoteFilePath, override, buf.data(), (int)buf.size());
}

bool util::SFTPSession::putFile(const std::string &remoteFilePath, bool override, void *data, int size) {
    // first, open a file
    int accessType = O_WRONLY | O_CREAT | O_TRUNC;
    if (!override) {
        accessType |= O_EXCL;   // if file exists, return false
    }
    std::shared_ptr<sftp_file_struct> file(sftp_open(this->_sftp, remoteFilePath.data(), accessType, S_IRWXU),
                                           [](auto* p) {
                                               if (p != nullptr) {
                                                   sftp_close(p);
                                               }
                                           });
    if (file == nullptr) {
        LOG(ERROR) << "Can't open file for writing: " << ssh_get_error(this->_session);
        return false;
    }
    // finally, write the file to sftp remote endpoint
    // write buffer to remote file
    if (sftp_write(file.get(), data, size) < 0) {
        LOG(ERROR) << "Can't send file: " << ssh_get_error(this->_session);
        return false;
    }
    return true;
}

bool util::SFTPSession::getFileToDisk(const std::string& remoteFilePath, const std::string &localFilePath, bool override) {
    // open local file
    int accessType = O_CREAT | O_WRONLY;
    if (!override) {
        accessType |= O_EXCL;   // if file exists, return false
    }
    auto fd = open(localFilePath.data(), accessType, 0777);
    if (fd < 0) {
        LOG(ERROR) << "Can't open file for writing: " << strerror(errno);
        return false;
    }
    auto callback = [&fd](void* data, int size) ->bool {
        auto ret = write(fd, data, size);
        if (ret != size) {
            LOG(ERROR) << "Error writing: " << strerror(errno);
            return false;
        }
        return true;
    };
    if (!getFileWithCallback(remoteFilePath, callback)) {
        return false;
    }
    if (close(fd) != 0) {
        return false;
    }
    return true;
}

bool util::SFTPSession::getFileToBuffer(const std::string &remoteFilePath, std::string &buffer) {
    auto callback = [&buffer](void* data, int size) ->bool {
        std::string_view str(static_cast<char*>(data), size);
        buffer += str;
        return true;
    };
    return getFileWithCallback(remoteFilePath, callback);
}

bool util::SFTPSession::getFileWithCallback(const std::string &remoteFilePath,
                                            const std::function<bool(void *, int)>& callback) {
    // open a remote file
    std::shared_ptr<sftp_file_struct> file(sftp_open(this->_sftp, remoteFilePath.data(), O_RDONLY, 0),
                                           [](auto* p) {
                                               if (p != nullptr) {
                                                   sftp_close(p);
                                               }
                                           });
    if (file == nullptr) {
        LOG(ERROR) << " Can't open file for reading: " << ssh_get_error(this->_session);
        return false;
    }
    // read the remote file into buffer
    constexpr static int maxSize = 65535;
    std::vector<char> buffer(maxSize);
    for (;;) {
        auto size = sftp_read(file.get(), buffer.data(), maxSize);
        if (size == 0) { // when EOF, break
            break;
        }
        if (size < 0) {
            LOG(ERROR) << "Error while reading file: " << ssh_get_error(this->_session);
            return false;
        }
        if (!callback(buffer.data(), (int)size)) {
            LOG(ERROR) << "Write file to local failed!";
            return false;
        }
    }
    return true;
}

std::unique_ptr<util::SSHSession> util::SSHSession::NewSSHSession(std::string ip, int port) {
    if (port <= 0 || port > 65535) {
        port = 22;
    }
    auto session = ssh_new();
    if (session == nullptr) {
        return nullptr;
    }
    int verbosity = SSH_LOG_WARNING;
    ssh_options_set(session, SSH_OPTIONS_HOST, ip.data());
    ssh_options_set(session, SSH_OPTIONS_LOG_VERBOSITY, &verbosity);
    ssh_options_set(session, SSH_OPTIONS_PORT, &port);

    // Assign value
    std::unique_ptr<util::SSHSession> sshSession(new util::SSHSession());
    sshSession->_session = session;
    auto ret = ssh_connect(sshSession->_session);
    if (ret != SSH_OK) {
        LOG(ERROR) << "Error connecting to " << ip << " : " << ssh_get_error(sshSession->_session);
        return nullptr;
    }
    sshSession->_ip = std::move(ip);
    sshSession->_port = port;
    if (!sshSession->verifyKnownHost()) {
        return nullptr;
    }
    return sshSession;
}

util::SSHSession::~SSHSession() {
    if (!_session) {
        return;
    }
    if (ssh_is_connected(_session) == 1) {
        ssh_disconnect(_session);
    }
    ssh_free(_session);
}

bool util::SSHSession::verifyKnownHost() {
    struct DeferDelete {
        void operator()(unsigned char **p) const {
            ssh_clean_pubkey_hash(p);
        }
    };
    // validate server cert
    ssh_key pub = nullptr;
    if (ssh_get_server_publickey(_session, &pub) < 0) {
        LOG(ERROR) << "Error get ssh remote server public key: " << _ip;
        return false;
    }

    unsigned char *hash = nullptr;
    size_t hashLen;
    auto ret = ssh_get_publickey_hash(pub,
                                      SSH_PUBLICKEY_HASH_SHA1,
                                      &hash,
                                      &hashLen);
    std::unique_ptr<unsigned char *, DeferDelete> defer(&hash);

    ssh_key_free(pub);
    if (ret < 0) {
        return false;
    }
    auto state = ssh_session_is_known_server(_session);
    switch (state) {
        case SSH_KNOWN_HOSTS_OK:
            /* OK */
            break;
        case SSH_KNOWN_HOSTS_CHANGED:
            LOG(ERROR) << "Host key for server changed: " << _ip << "\n"
                       << "For security reasons, connection will be stopped.";
            return false;
        case SSH_KNOWN_HOSTS_OTHER:
            LOG(ERROR) << "The host key for this server was not found but an other type of key exists: " << _ip << "\n"
                       << "An attacker might change the default server key to confuse your client into thinking the key does not exist.";
            return false;
        case SSH_KNOWN_HOSTS_NOT_FOUND:
            LOG(ERROR) << "Could not find known host file: " << _ip << "\n"
                       << "If you accept the host key here, the file will be automatically created.";
            /* FALL THROUGH to SSH_SERVER_NOT_KNOWN behavior */
        case SSH_KNOWN_HOSTS_UNKNOWN:
        {
            auto hexA = ssh_get_hexa(hash, hashLen);
            LOG(ERROR) << "The server is unknown. Auto add the host key into dictionary: " << _ip << "\n"
                       << "Public key hash:" << hexA;
            ssh_string_free_char(hexA);
            if (ssh_session_update_known_hosts(_session) < 0) {
                LOG(ERROR) << "Error " << strerror(errno);
                return false;
            }
            break;
        }
        case SSH_KNOWN_HOSTS_ERROR:
            LOG(ERROR) << "Error " << ssh_get_error(_session);
            return false;
    }
    return true;
}

bool util::SSHSession::connect(const std::string& user, const std::string &password) {
    auto ret = ssh_userauth_password(_session, user.data(), password.data());
    if (ret != SSH_AUTH_SUCCESS) {
        LOG(ERROR) << "Error authenticating with password " << ssh_get_error(_session);
        return false;
    }
    return true;
}
