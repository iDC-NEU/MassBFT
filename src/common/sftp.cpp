//
// Created by user on 23-4-15.
//

#include "common/ssh.h"
#include <libssh/libssh.h>
#include <libssh/sftp.h>
#include <fstream>
#include <fcntl.h>
#include "glog/logging.h"

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
                                           [](auto* p) { sftp_close(p); });
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

std::optional<std::string> getFileToMemory(const std::string& remoteFilePath){
    return nullptr;
}

bool getFileToDisk(const std::string& remoteFilePath, const std::string& localPath){
    return false;
}