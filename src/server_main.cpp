#include "common.hpp"
#include "protocol.hpp"
#include "storage.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>

namespace {

void serve_client(int client_fd, flexql::StorageEngine &engine) {
    std::string sql;
    std::string recv_buffer;
    while (flexql::recv_line_buffered(client_fd, recv_buffer, sql)) {
        sql = flexql::trim(sql);
        if (sql.empty()) {
            continue;
        }
        try {
            const auto command = flexql::parse_sql(sql);
            const auto result = engine.execute(command);
            if (!flexql::send_all(client_fd, flexql::encode_result(result))) {
                break;
            }
        } catch (const std::exception &ex) {
            if (!flexql::send_all(client_fd, flexql::encode_error(ex.what()))) {
                break;
            }
        }
    }
    ::close(client_fd);
}

}  // namespace

int main(int argc, char **argv) {
    int port = 9000;
    std::filesystem::path data_dir = "data";
    if (argc > 1) {
        port = std::stoi(argv[1]);
    }
    if (argc > 2) {
        data_dir = argv[2];
    }

    flexql::StorageEngine engine(data_dir);
    engine.load();

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "failed to create server socket\n";
        return 1;
    }

    int opt = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        std::cerr << "failed to bind server socket\n";
        ::close(server_fd);
        return 1;
    }
    if (::listen(server_fd, 64) != 0) {
        std::cerr << "failed to listen on server socket\n";
        ::close(server_fd);
        return 1;
    }

    std::cout << "FlexQL server listening on port " << port
              << " with data dir " << data_dir.string() << '\n';

    while (true) {
        int client_fd = ::accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) {
            continue;
        }
        std::thread(serve_client, client_fd, std::ref(engine)).detach();
    }
}
