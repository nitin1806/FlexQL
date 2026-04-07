#include "protocol.hpp"

namespace flexql {

std::string encode_result(const QueryResult &result) {
    std::ostringstream out;
    out << "COLUMNS";
    for (const auto &column : result.columns) {
        out << '\t' << escape_field(column);
    }
    out << '\n';
    for (const auto &row : result.rows) {
        out << "ROW";
        for (const auto &value : row) {
            out << '\t' << escape_field(value);
        }
        out << '\n';
    }
    out << "END\n";
    return out.str();
}

std::string encode_error(const std::string &message) {
    return "ERROR\t" + escape_field(message) + "\nEND\n";
}

bool decode_response(int fd, std::string &recv_buffer, QueryResult &result, std::string &error_message) {
    result = QueryResult{};
    error_message.clear();
    std::string line;
    while (recv_line_buffered(fd, recv_buffer, line)) {
        if (line == "END") {
            return error_message.empty();
        }
        const auto tsv = [&] {
            std::vector<std::string> values;
            std::string current;
            for (char c : line) {
                if (c == '\t') {
                    values.push_back(current);
                    current.clear();
                } else {
                    current.push_back(c);
                }
            }
            values.push_back(current);
            return values;
        }();
        if (tsv.empty()) {
            continue;
        }
        if (tsv[0] == "ERROR") {
            error_message = tsv.size() > 1 ? unescape_field(tsv[1]) : "unknown server error";
        } else if (tsv[0] == "COLUMNS") {
            for (std::size_t i = 1; i < tsv.size(); ++i) {
                result.columns.push_back(unescape_field(tsv[i]));
            }
        } else if (tsv[0] == "ROW") {
            std::vector<std::string> row;
            for (std::size_t i = 1; i < tsv.size(); ++i) {
                row.push_back(unescape_field(tsv[i]));
            }
            result.rows.push_back(std::move(row));
        }
    }
    return false;
}

}  // namespace flexql
