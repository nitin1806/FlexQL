#pragma once

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <thread>
#include <ctime>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>

namespace flexql {

inline std::string trim(std::string_view input) {
    std::size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    std::size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return std::string(input.substr(start, end - start));
}

inline std::string to_upper(std::string_view input) {
    std::string out(input);
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return out;
}

inline bool iequals(std::string_view a, std::string_view b) {
    return to_upper(a) == to_upper(b);
}

inline std::vector<std::string> split_csv(const std::string &input) {
    std::vector<std::string> parts;
    std::string current;
    bool in_quote = false;
    char quote = '\0';
    for (std::size_t i = 0; i < input.size(); ++i) {
        char c = input[i];
        if ((c == '\'' || c == '"') && (i == 0 || input[i - 1] != '\\')) {
            if (in_quote && c == quote) {
                in_quote = false;
            } else if (!in_quote) {
                in_quote = true;
                quote = c;
            }
            current.push_back(c);
            continue;
        }
        if (c == ',' && !in_quote) {
            parts.push_back(trim(current));
            current.clear();
            continue;
        }
        current.push_back(c);
    }
    if (!current.empty() || !input.empty()) {
        parts.push_back(trim(current));
    }
    return parts;
}

inline std::string unquote(std::string value) {
    if (value.size() >= 2 &&
        ((value.front() == '\'' && value.back() == '\'') || (value.front() == '"' && value.back() == '"'))) {
        std::string out;
        out.reserve(value.size() - 2);
        for (std::size_t i = 1; i + 1 < value.size(); ++i) {
            char c = value[i];
            if (c == '\\' && i + 1 < value.size() - 1) {
                ++i;
                out.push_back(value[i]);
            } else {
                out.push_back(c);
            }
        }
        return out;
    }
    return value;
}

inline std::string escape_field(const std::string &value) {
    std::string out;
    out.reserve(value.size());
    for (char c : value) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '\t': out += "\\t"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            default: out.push_back(c); break;
        }
    }
    return out;
}

inline std::string unescape_field(const std::string &value) {
    std::string out;
    out.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '\\' && i + 1 < value.size()) {
            ++i;
            switch (value[i]) {
                case 't': out.push_back('\t'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case '\\': out.push_back('\\'); break;
                default:
                    out.push_back('\\');
                    out.push_back(value[i]);
                    break;
            }
        } else {
            out.push_back(value[i]);
        }
    }
    return out;
}

inline bool send_all(int fd, const std::string &data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        ssize_t rc = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(rc);
    }
    return true;
}

inline bool recv_line(int fd, std::string &line) {
    line.clear();
    char c = '\0';
    while (true) {
        ssize_t rc = ::recv(fd, &c, 1, 0);
        if (rc <= 0) {
            return !line.empty();
        }
        if (c == '\n') {
            return true;
        }
        line.push_back(c);
    }
}

inline bool recv_line_buffered(int fd, std::string &buffer, std::string &line) {
    while (true) {
        const std::size_t newline_pos = buffer.find('\n');
        if (newline_pos != std::string::npos) {
            line = buffer.substr(0, newline_pos);
            buffer.erase(0, newline_pos + 1);
            return true;
        }

        char chunk[8192];
        const ssize_t rc = ::recv(fd, chunk, sizeof(chunk), 0);
        if (rc <= 0) {
            if (buffer.empty()) {
                return false;
            }
            line = buffer;
            buffer.clear();
            return true;
        }
        buffer.append(chunk, static_cast<std::size_t>(rc));
    }
}

inline void durable_fsync_file(const std::filesystem::path &path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("open failed for fsync: " + path.string());
    }
    if (::fsync(fd) != 0) {
        int err = errno;
        ::close(fd);
        throw std::runtime_error("fsync failed: " + std::string(std::strerror(err)));
    }
    ::close(fd);
}

inline void durable_fsync_dir(const std::filesystem::path &path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        throw std::runtime_error("open dir failed for fsync: " + path.string());
    }
    if (::fsync(fd) != 0) {
        int err = errno;
        ::close(fd);
        throw std::runtime_error("fsync dir failed: " + std::string(std::strerror(err)));
    }
    ::close(fd);
}

}  // namespace flexql
