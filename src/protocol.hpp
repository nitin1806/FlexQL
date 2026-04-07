#pragma once

#include "storage.hpp"

namespace flexql {

std::string encode_result(const QueryResult &result);
std::string encode_error(const std::string &message);
bool decode_response(
    int fd,
    std::string &recv_buffer,
    QueryResult &result,
    std::string &error_message);

}  // namespace flexql
