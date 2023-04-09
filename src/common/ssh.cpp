//
// Created by user on 23-3-30.
//

#include "common/ssh.h"
#include <libssh/libssh.h>
#include <libssh/sftp.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "glog/logging.h"


std::unique_ptr<util::SFTPSession> util::SFTPSession::NewSFTPSession(ssh_session_struct* session){
    //open SFTPSession
    auto sftp = sftp_new(session);
    if(sftp == nullptr){
        LOG(ERROR) << "Error allocating SFTP session: %s\n" << ssh_get_error(session);
        return nullptr;
    }

    // Assign value
    std::unique_ptr<util::SFTPSession> sftpSession(new util::SFTPSession);

    // SFTP protocol initialization
    auto ret = sftp_init(sftp);
    if (ret != SSH_OK)
    {
        int sftp_error = sftp_get_error(sftp);
        LOG(ERROR) << "Error initializing SFTP session: code %d.\n" << sftp_error;

        // analyze sftpError
        //TODO: does the log error in this function right?
        sftpSession -> analyzeSftpError(sftp_error);

        sftp_free(sftp);
        return nullptr;
    }

    return sftpSession;
}

util::SFTPSession::~SFTPSession() {
    // close SFTPSession
    if(_sftp != nullptr){
        sftp_free(_sftp);
    }
}

void util::SFTPSession::analyzeSftpError(int sftp_error){
    // analyze the sftpError type when it occurs
    switch (sftp_error) {
        case SSH_FX_OK:
            // no error
            //TODO: does this situation need sftp_free()?
            break;

        case SSH_FX_EOF:
            LOG(ERROR) << " end-of-file encountered. " ;

        case SSH_FX_NO_SUCH_FILE:
            LOG(ERROR) << " file dos not exist. " ;

        case SSH_FX_PERMISSION_DENIED:
            LOG(ERROR) << " permission denied. " ;

        case SSH_FX_FAILURE:
            LOG(ERROR) << " generic failure. " ;

        case SSH_FX_BAD_MESSAGE:
            LOG(ERROR) << "garbage received from server. ";

        case SSH_FX_NO_CONNECTION:
            LOG(ERROR) << "no connection has been set up. ";

        case SSH_FX_CONNECTION_LOST:
            LOG(ERROR) << "there was a connection, but we lost it. ";

        case SSH_FX_OP_UNSUPPORTED:
            LOG(ERROR) << " operation not supported by libssh yet. ";

        case SSH_FX_INVALID_HANDLE:
            LOG(ERROR) << " invalid file handle. ";

        case SSH_FX_NO_SUCH_PATH:
            LOG(ERROR) << " no such file or directory path exists. ";

        case SSH_FX_FILE_ALREADY_EXISTS:
            LOG(ERROR) << " an attempt to create an already existing file or directory has been made. ";

        case SSH_FX_WRITE_PROTECT:
            LOG(ERROR) << " write-protected filesystem. ";

        case SSH_FX_NO_MEDIA:
            LOG(ERROR) << "  no media was in remote drive. ";

    };
}

int util::SFTPSession::writeConfig(ssh_session_struct *session, sftp_session_struct *sftp, const char* read_path,const char* write_path) {
    // write .config from read_path to write_path(/tmp)
    int access_type = O_CREAT;
    sftp_file file;
    const char *helloworld = "Hello, World!\n";
    int length = strlen(helloworld);
    int ret, nwritten;


    file = sftp_open(sftp, write_path,access_type, S_IRWXU);
    if (file == nullptr)
    {
        LOG(ERROR) << "Can't open file for writing: %s\n" << ssh_get_error(session);
        return SSH_ERROR;
    }

    nwritten = sftp_write(file, helloworld, length);
    if (nwritten != length)
    {
        LOG(ERROR) << "Can't write data to file: %s\n" << ssh_get_error(session);
        sftp_close(file);
        return SSH_ERROR;
    }

    ret = sftp_close(file);
    if (ret != SSH_OK){
        LOG(ERROR) << "Can't close the written file: %s\n" << ssh_get_error(session);
        return ret;
    }

    return SSH_OK;
}

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
