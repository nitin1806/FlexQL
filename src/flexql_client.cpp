#include "common.hpp"
#include "protocol.hpp"

#include "flexql.h"

#include <arpa/inet.h>
#include <netdb.h>

struct flexql_db {
    int fd = -1;
    std::string recv_buffer;
};

namespace {

char *dup_cstr(const std::string &text) {
    char *buffer = static_cast<char *>(std::malloc(text.size() + 1));
    if (buffer == nullptr) {
        return nullptr;
    }
    std::memcpy(buffer, text.c_str(), text.size() + 1);
    return buffer;
}

int connect_socket(const char *host, int port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo *result = nullptr;
    const std::string port_text = std::to_string(port);
    if (::getaddrinfo(host, port_text.c_str(), &hints, &result) != 0) {
        return -1;
    }

    int fd = -1;
    for (addrinfo *rp = result; rp != nullptr; rp = rp->ai_next) {
        fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (::connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(result);
    return fd;
}

}  // namespace

extern "C" int flexql_open(const char *host, int port, flexql_db **db) {
    if (host == nullptr || db == nullptr) {
        return FLEXQL_ERROR;
    }
    int fd = connect_socket(host, port);
    if (fd < 0) {
        return FLEXQL_ERROR;
    }
    *db = new flexql_db{};
    (*db)->fd = fd;
    return FLEXQL_OK;
}

extern "C" int flexql_close(flexql_db *db) {
    if (db == nullptr) {
        return FLEXQL_ERROR;
    }
    if (db->fd >= 0) {
        ::close(db->fd);
    }
    delete db;
    return FLEXQL_OK;
}

extern "C" int flexql_exec(
    flexql_db *db,
    const char *sql,
    flexql_callback callback,
    void *arg,
    char **errmsg) {
    if (db == nullptr || db->fd < 0 || sql == nullptr) {
        return FLEXQL_ERROR;
    }
    if (!flexql::send_all(db->fd, std::string(sql) + "\n")) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("failed to send query to server");
        }
        return FLEXQL_ERROR;
    }

    flexql::QueryResult result;
    std::string error_message;
    if (!flexql::decode_response(db->fd, db->recv_buffer, result, error_message) || !error_message.empty()) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr(error_message.empty() ? "failed to receive response" : error_message);
        }
        return FLEXQL_ERROR;
    }

    if (callback != nullptr) {
        std::vector<char *> column_names;
        column_names.reserve(result.columns.size());
        for (auto &column : result.columns) {
            column_names.push_back(column.data());
        }
        for (auto &row : result.rows) {
            std::vector<char *> values;
            values.reserve(row.size());
            for (auto &value : row) {
                values.push_back(value.data());
            }
            if (callback(arg, static_cast<int>(row.size()), values.data(), column_names.data()) != 0) {
                break;
            }
        }
    }
    return FLEXQL_OK;
}

extern "C" void flexql_free(void *ptr) {
    std::free(ptr);
}
