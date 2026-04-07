#include "flexql.h"

#include <iostream>
#include <string>
#include <vector>

namespace {

int print_row(void *, int column_count, char **values, char **column_names) {
    for (int i = 0; i < column_count; ++i) {
        std::cout << column_names[i] << '=' << values[i];
        if (i + 1 != column_count) {
            std::cout << " | ";
        }
    }
    std::cout << '\n';
    return 0;
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    int port = argc > 2 ? std::stoi(argv[2]) : 9000;

    flexql_db *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "failed to connect to FlexQL server\n";
        return 1;
    }

    std::cout << "Connected to FlexQL at " << host << ':' << port << '\n';
    std::cout << "Type SQL statements and end them with ';'. Type exit to quit.\n";

    std::string line;
    std::string sql;
    while (std::cout << "flexql> ", std::getline(std::cin, line)) {
        if (line == "exit" || line == "quit") {
            break;
        }
        sql += line;
        if (sql.find(';') == std::string::npos) {
            sql.push_back(' ');
            continue;
        }
        char *errmsg = nullptr;
        const int rc = flexql_exec(db, sql.c_str(), print_row, nullptr, &errmsg);
        if (rc != FLEXQL_OK) {
            std::cerr << "error: " << (errmsg != nullptr ? errmsg : "unknown error") << '\n';
            flexql_free(errmsg);
        }
        sql.clear();
    }

    flexql_close(db);
    return 0;
}
