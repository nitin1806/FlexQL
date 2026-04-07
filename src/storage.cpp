#include "storage.hpp"

namespace flexql {

namespace {

std::string type_to_string(ColumnType type) {
    switch (type) {
        case ColumnType::Decimal: return "DECIMAL";
        case ColumnType::Varchar: return "VARCHAR";
        case ColumnType::Datetime: return "DATETIME";
    }
    throw std::runtime_error("unknown column type");
}

bool parse_decimal(const std::string &value, long double &out) {
    char *end = nullptr;
    errno = 0;
    out = std::strtold(value.c_str(), &end);
    return errno == 0 && end != value.c_str() && *end == '\0';
}

long long current_epoch_seconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

long long parse_epoch_string(const std::string &value) {
    long double numeric = 0.0L;
    if (parse_decimal(value, numeric)) {
        return static_cast<long long>(numeric);
    }
    std::tm tm{};
    std::istringstream iss(value);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (iss.fail()) {
        throw std::runtime_error("expiration must be epoch seconds or YYYY-MM-DD HH:MM:SS");
    }
    return static_cast<long long>(timegm(&tm));
}

std::vector<std::string> split_tsv(const std::string &line) {
    std::vector<std::string> parts;
    std::string current;
    for (char c : line) {
        if (c == '\t') {
            parts.push_back(current);
            current.clear();
        } else {
            current.push_back(c);
        }
    }
    parts.push_back(current);
    return parts;
}

std::string join_escaped_values(const std::vector<std::string> &values) {
    std::ostringstream out;
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i != 0) {
            out << '\t';
        }
        out << escape_field(values[i]);
    }
    return out.str();
}

}  // namespace

std::optional<QueryResult> StorageEngine::QueryCache::get(const std::string &key) {
    const auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }
    items_.splice(items_.begin(), items_, it->second);
    return it->second->value;
}

void StorageEngine::QueryCache::put(const std::string &key, const QueryResult &value) {
    const auto existing = index_.find(key);
    if (existing != index_.end()) {
        existing->second->value = value;
        items_.splice(items_.begin(), items_, existing->second);
        return;
    }
    items_.push_front({key, value});
    index_[key] = items_.begin();
    if (items_.size() > capacity_) {
        auto last = std::prev(items_.end());
        index_.erase(last->key);
        items_.pop_back();
    }
}

void StorageEngine::QueryCache::clear() {
    items_.clear();
    index_.clear();
}

StorageEngine::StorageEngine(std::filesystem::path root_dir) : root_dir_(std::move(root_dir)) {}

StorageEngine::~StorageEngine() {
    close_table_files();
    close_wal_file();
}

void StorageEngine::load() {
    std::unique_lock lock(mutex_);
    std::filesystem::create_directories(root_dir_);
    close_table_files();
    close_wal_file();
    tables_.clear();
    applied_txids_.clear();
    next_txid_ = 1;
    for (const auto &entry : std::filesystem::directory_iterator(root_dir_)) {
        if (entry.path().extension() == ".schema") {
            load_table(entry.path());
        }
    }
    load_wal();
    replay_wal();
    cache_.clear();
}

QueryResult StorageEngine::execute(const Command &command) {
    switch (command.type) {
        case CommandType::CreateTable:
            return create_table(command.create_table);
        case CommandType::Insert:
            return insert_row(command.insert);
        case CommandType::Select:
            return select_rows(command.select);
    }
    throw std::runtime_error("unsupported command");
}

QueryResult StorageEngine::create_table(const CreateTableCommand &command) {
    std::unique_lock lock(mutex_);
    if (tables_.contains(command.table_name)) {
        throw std::runtime_error("table already exists: " + command.table_name);
    }
    Table table;
    table.name = command.table_name;
    table.columns = command.columns;
    int primary_keys = 0;
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        table.column_index[table.columns[i].name] = i;
        if (table.columns[i].primary_key) {
            ++primary_keys;
            table.primary_key_column = i;
        }
    }
    if (primary_keys > 1) {
        throw std::runtime_error("only one PRIMARY KEY column is supported");
    }
    if (table.columns.empty()) {
        throw std::runtime_error("table must contain at least one column");
    }
    persist_schema(table);
    std::ofstream(table_data_path(table.name), std::ios::app).close();
    durable_fsync_file(table_data_path(table.name));
    durable_fsync_dir(root_dir_);
    ensure_data_file_open(table);
    tables_[table.name] = table;
    cache_.clear();
    return QueryResult{{"status"}, {{"table created"}}};
}

QueryResult StorageEngine::insert_row(const InsertCommand &command) {
    std::unique_lock lock(mutex_);
    auto it = tables_.find(command.table_name);
    if (it == tables_.end()) {
        throw std::runtime_error("unknown table: " + command.table_name);
    }
    Table &table = it->second;
    std::vector<RowRecord> rows_to_insert;
    rows_to_insert.reserve(command.rows.size());
    std::unordered_map<std::string, std::size_t> batch_primary_keys;
    for (const auto &input_row : command.rows) {
        RowRecord row = make_row_record(table, input_row, command.expires_at);
        if (table.primary_key_column) {
            const std::string &pk_value = row.values[*table.primary_key_column];
            if (table.primary_index.contains(pk_value) || batch_primary_keys.contains(pk_value)) {
                throw std::runtime_error("duplicate primary key value: " + pk_value);
            }
            batch_primary_keys[pk_value] = rows_to_insert.size();
        }
        rows_to_insert.push_back(std::move(row));
    }

    const long long txid = next_txid_++;
    append_wal_record(txid, table.name, rows_to_insert);
    append_rows_to_disk(table, txid, rows_to_insert);
    apply_rows_in_memory(table, rows_to_insert);
    applied_txids_.insert(txid);
    cache_.clear();
    return QueryResult{{"status"}, {{std::to_string(command.rows.size()) + " row(s) inserted"}}};
}

QueryResult StorageEngine::select_rows(const SelectCommand &command) {
    const std::string cache_key = [&] {
        std::ostringstream oss;
        oss << "SELECT:";
        for (const auto &column : command.columns) oss << column << ",";
        oss << "|FROM:" << command.left_table;
        if (command.right_table) oss << "|JOIN:" << *command.right_table;
        if (command.join_condition) oss << "|ON:" << command.join_condition->lhs << ":" << command.join_condition->rhs;
        if (command.where_condition) oss << "|WHERE:" << command.where_condition->lhs << ":" << command.where_condition->rhs;
        return oss.str();
    }();

    {
        std::unique_lock lock(mutex_);
        if (auto cached = cache_.get(cache_key)) {
            return *cached;
        }
    }

    std::unique_lock lock(mutex_);
    const auto [left_table, right_table] = resolve_tables(command);

    QueryResult result;
    if (command.columns.size() == 1 && trim(command.columns.front()) == "*") {
        for (const auto &column : left_table.columns) {
            result.columns.push_back(left_table.name + "." + column.name);
        }
        if (right_table != nullptr) {
            for (const auto &column : right_table->columns) {
                result.columns.push_back(right_table->name + "." + column.name);
            }
        }
    } else {
        for (const auto &column : command.columns) {
            result.columns.push_back(trim(column));
        }
    }

    const auto emit_row = [&](const RowRecord &left_row, const RowRecord *right_row) {
        if (is_expired(left_row) || (right_row != nullptr && is_expired(*right_row))) {
            return;
        }
        if (command.where_condition &&
            !evaluate_condition(*command.where_condition, left_table, left_row, right_table, right_row)) {
            return;
        }

        std::vector<std::string> output_row;
        if (command.columns.size() == 1 && trim(command.columns.front()) == "*") {
            output_row.insert(output_row.end(), left_row.values.begin(), left_row.values.end());
            if (right_row != nullptr) {
                output_row.insert(output_row.end(), right_row->values.begin(), right_row->values.end());
            }
        } else {
            for (const auto &expr : command.columns) {
                const std::string column = trim(expr);
                if (column.find('.') != std::string::npos) {
                    const std::string table_name = column.substr(0, column.find('.'));
                    if (table_name == left_table.name) {
                        output_row.push_back(get_value(left_table, left_row, column));
                    } else if (right_table != nullptr && table_name == right_table->name) {
                        output_row.push_back(get_value(*right_table, *right_row, column));
                    } else {
                        throw std::runtime_error("unknown qualified column: " + column);
                    }
                } else {
                    if (left_table.column_index.contains(column)) {
                        output_row.push_back(get_value(left_table, left_row, column));
                    } else if (right_table != nullptr && right_table->column_index.contains(column)) {
                        output_row.push_back(get_value(*right_table, *right_row, column));
                    } else {
                        throw std::runtime_error("unknown column: " + column);
                    }
                }
            }
        }
        result.rows.push_back(std::move(output_row));
    };

    if (right_table == nullptr) {
        bool used_primary_index = false;
        if (command.where_condition && command.where_condition->op == CompareOp::Eq && !command.where_condition->rhs_is_column) {
            const std::string column_name = trim(command.where_condition->lhs);
            const std::string plain_name = column_name.find('.') == std::string::npos
                ? column_name
                : column_name.substr(column_name.find('.') + 1);
            if ((column_name.find('.') == std::string::npos || column_name.rfind(left_table.name + ".", 0) == 0) &&
                left_table.primary_key_column &&
                left_table.columns[*left_table.primary_key_column].name == plain_name) {
                const auto pk_it = left_table.primary_index.find(command.where_condition->rhs);
                used_primary_index = true;
                if (pk_it != left_table.primary_index.end()) {
                    const auto &left_row = left_table.rows[pk_it->second];
                    if (!left_row.deleted) {
                        emit_row(left_row, nullptr);
                    }
                }
            }
        }
        if (!used_primary_index) {
            for (const auto &left_row : left_table.rows) {
                if (!left_row.deleted) {
                    emit_row(left_row, nullptr);
                }
            }
        }
    } else {
        if (!command.join_condition) {
            throw std::runtime_error("JOIN requires a join condition");
        }
        for (const auto &left_row : left_table.rows) {
            if (left_row.deleted) {
                continue;
            }
            for (const auto &right_row : right_table->rows) {
                if (right_row.deleted) {
                    continue;
                }
                if (evaluate_condition(*command.join_condition, left_table, left_row, right_table, &right_row)) {
                    emit_row(left_row, &right_row);
                }
            }
        }
    }

    cache_.put(cache_key, result);
    return result;
}

bool StorageEngine::is_expired(const RowRecord &row) const {
    return row.expires_at_epoch <= current_epoch_seconds();
}

std::optional<std::size_t> StorageEngine::find_primary_key_column(const Table &table) const {
    return table.primary_key_column;
}

std::size_t StorageEngine::require_column(const Table &table, const std::string &name) const {
    const std::string plain_name = name.find('.') == std::string::npos ? name : name.substr(name.find('.') + 1);
    const auto it = table.column_index.find(plain_name);
    if (it == table.column_index.end()) {
        throw std::runtime_error("unknown column: " + name);
    }
    return it->second;
}

std::pair<const StorageEngine::Table&, const StorageEngine::Table*> StorageEngine::resolve_tables(
    const SelectCommand &command) const {
    const auto left_it = tables_.find(command.left_table);
    if (left_it == tables_.end()) {
        throw std::runtime_error("unknown table: " + command.left_table);
    }
    if (!command.right_table) {
        return {left_it->second, nullptr};
    }
    const auto right_it = tables_.find(*command.right_table);
    if (right_it == tables_.end()) {
        throw std::runtime_error("unknown table: " + *command.right_table);
    }
    return {left_it->second, &right_it->second};
}

std::string StorageEngine::get_value(const Table &table, const RowRecord &row, const std::string &column) const {
    return row.values.at(require_column(table, column));
}

bool StorageEngine::compare_values(const std::string &lhs, CompareOp op, const std::string &rhs) const {
    long double left_num = 0.0L;
    long double right_num = 0.0L;
    const bool numeric = parse_decimal(lhs, left_num) && parse_decimal(rhs, right_num);
    if (numeric) {
        switch (op) {
            case CompareOp::Eq: return left_num == right_num;
            case CompareOp::Ne: return left_num != right_num;
            case CompareOp::Lt: return left_num < right_num;
            case CompareOp::Le: return left_num <= right_num;
            case CompareOp::Gt: return left_num > right_num;
            case CompareOp::Ge: return left_num >= right_num;
        }
    }
    switch (op) {
        case CompareOp::Eq: return lhs == rhs;
        case CompareOp::Ne: return lhs != rhs;
        case CompareOp::Lt: return lhs < rhs;
        case CompareOp::Le: return lhs <= rhs;
        case CompareOp::Gt: return lhs > rhs;
        case CompareOp::Ge: return lhs >= rhs;
    }
    return false;
}

bool StorageEngine::evaluate_condition(
    const Condition &cond,
    const Table &left_table,
    const RowRecord &left_row,
    const Table *right_table,
    const RowRecord *right_row) const {
    auto resolve = [&](const std::string &expr) -> std::string {
        const std::string column = trim(expr);
        if (column.find('.') != std::string::npos) {
            const std::string table_name = column.substr(0, column.find('.'));
            if (table_name == left_table.name) {
                return get_value(left_table, left_row, column);
            }
            if (right_table != nullptr && right_row != nullptr && table_name == right_table->name) {
                return get_value(*right_table, *right_row, column);
            }
            throw std::runtime_error("unknown qualified column: " + column);
        }
        if (left_table.column_index.contains(column)) {
            return get_value(left_table, left_row, column);
        }
        if (right_table != nullptr && right_row != nullptr && right_table->column_index.contains(column)) {
            return get_value(*right_table, *right_row, column);
        }
        throw std::runtime_error("unknown column: " + column);
    };

    const std::string lhs = resolve(cond.lhs);
    const std::string rhs = cond.rhs_is_column ? resolve(cond.rhs) : cond.rhs;
    return compare_values(lhs, cond.op, rhs);
}

StorageEngine::RowRecord StorageEngine::make_row_record(
    const Table &table,
    const std::vector<std::string> &values,
    const std::optional<std::string> &expires_at) const {
    if (values.size() != table.columns.size()) {
        throw std::runtime_error("value count does not match schema");
    }
    for (std::size_t i = 0; i < values.size(); ++i) {
        const auto &value = values[i];
        const auto &column = table.columns[i];
        if (column.type == ColumnType::Decimal) {
            long double parsed = 0.0L;
            if (!parse_decimal(value, parsed)) {
                throw std::runtime_error("invalid DECIMAL value for column " + column.name);
            }
        }
    }

    RowRecord row;
    row.values = values;
    row.expires_at = parse_expiry(expires_at);
    row.expires_at_epoch = parse_epoch_string(row.expires_at);
    return row;
}

std::string StorageEngine::parse_expiry(const std::optional<std::string> &expires_at) const {
    if (!expires_at || trim(*expires_at).empty()) {
        return "32503680000";
    }
    const std::string trimmed = trim(*expires_at);
    parse_epoch_string(trimmed);
    return trimmed;
}

std::string StorageEngine::build_transaction_payload(
    long long txid,
    const std::vector<RowRecord> &rows,
    bool include_table_name,
    const std::string &table_name) const {
    std::ostringstream payload_stream;
    payload_stream << "BEGIN\t" << txid;
    if (include_table_name) {
        payload_stream << '\t' << escape_field(table_name);
    }
    payload_stream << '\t' << rows.size() << '\n';
    for (const auto &row : rows) {
        payload_stream << "ROW\t" << txid << '\t' << escape_field(row.expires_at);
        if (!row.values.empty()) {
            payload_stream << '\t' << join_escaped_values(row.values);
        }
        payload_stream << '\n';
    }
    payload_stream << "COMMIT\t" << txid << '\n';
    return payload_stream.str();
}

void StorageEngine::append_wal_record(long long txid, const std::string &table_name, const std::vector<RowRecord> &rows) {
    if (wal_fd_ < 0) {
        wal_fd_ = ::open(wal_path().c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (wal_fd_ < 0) {
            throw std::runtime_error("failed to open WAL file");
        }
    }
    const std::string payload = build_transaction_payload(txid, rows, true, table_name);
    ssize_t written = ::write(wal_fd_, payload.data(), payload.size());
    if (written != static_cast<ssize_t>(payload.size())) {
        int err = errno;
        throw std::runtime_error("failed to write WAL: " + std::string(std::strerror(err)));
    }
    if (::fdatasync(wal_fd_) != 0) {
        int err = errno;
        throw std::runtime_error("failed to sync WAL: " + std::string(std::strerror(err)));
    }
}

void StorageEngine::append_rows_to_disk(const Table &table, long long txid, const std::vector<RowRecord> &rows) {
    const std::string payload = build_transaction_payload(txid, rows, false, "");
    ssize_t written = ::write(table.data_fd, payload.data(), payload.size());
    if (written != static_cast<ssize_t>(payload.size())) {
        int err = errno;
        throw std::runtime_error("failed to write row: " + std::string(std::strerror(err)));
    }
    if (::fdatasync(table.data_fd) != 0) {
        int err = errno;
        throw std::runtime_error("failed to sync row write: " + std::string(std::strerror(err)));
    }
}

void StorageEngine::apply_rows_in_memory(Table &table, const std::vector<RowRecord> &rows) {
    const std::size_t base_index = table.rows.size();
    for (std::size_t i = 0; i < rows.size(); ++i) {
        if (table.primary_key_column && !is_expired(rows[i])) {
            table.primary_index[rows[i].values[*table.primary_key_column]] = base_index + i;
        }
        table.rows.push_back(rows[i]);
    }
}

void StorageEngine::persist_schema(const Table &table) {
    const auto path = table_schema_path(table.name);
    const auto temp_path = path.string() + ".tmp";
    {
        std::ofstream out(temp_path, std::ios::trunc);
        if (!out) {
            throw std::runtime_error("failed to write schema file");
        }
        for (const auto &column : table.columns) {
            out << column.name << '\t' << type_to_string(column.type) << '\t'
                << (column.primary_key ? "PK" : "") << '\n';
        }
    }
    durable_fsync_file(temp_path);
    std::filesystem::rename(temp_path, path);
    durable_fsync_dir(root_dir_);
}

void StorageEngine::load_table(const std::filesystem::path &schema_path) {
    Table table;
    table.name = schema_path.stem().string();

    {
        std::ifstream in(schema_path);
        std::string line;
        while (std::getline(in, line)) {
            if (trim(line).empty()) {
                continue;
            }
            const auto parts = split_tsv(line);
            if (parts.size() < 2) {
                throw std::runtime_error("invalid schema line for table " + table.name);
            }
            ColumnDef column;
            column.name = parts[0];
            column.type = type_to_string(ColumnType::Decimal) == parts[1] ? ColumnType::Decimal
                         : type_to_string(ColumnType::Varchar) == parts[1] ? ColumnType::Varchar
                                                                           : ColumnType::Datetime;
            column.primary_key = parts.size() > 2 && parts[2] == "PK";
            table.column_index[column.name] = table.columns.size();
            table.columns.push_back(column);
            if (column.primary_key) {
                table.primary_key_column = table.columns.size() - 1;
            }
        }
    }

    const auto data_path = table_data_path(table.name);
    if (std::filesystem::exists(data_path)) {
        std::ifstream in(data_path);
        std::string line;
        long long current_txid = -1;
        std::size_t expected_rows = 0;
        std::vector<RowRecord> pending_rows;
        while (std::getline(in, line)) {
            if (trim(line).empty()) {
                continue;
            }
            const auto parts = split_tsv(line);
            if (parts.empty()) {
                continue;
            }
            if (parts[0] == "BEGIN") {
                if (parts.size() != 3) {
                    pending_rows.clear();
                    current_txid = -1;
                    continue;
                }
                current_txid = std::stoll(parts[1]);
                expected_rows = static_cast<std::size_t>(std::stoull(parts[2]));
                pending_rows.clear();
                next_txid_ = std::max(next_txid_, current_txid + 1);
            } else if (parts[0] == "ROW") {
                if (current_txid < 0 || parts.size() != table.columns.size() + 3) {
                    continue;
                }
                if (std::stoll(parts[1]) != current_txid) {
                    pending_rows.clear();
                    current_txid = -1;
                    continue;
                }
                RowRecord row;
                row.expires_at = unescape_field(parts[2]);
                row.expires_at_epoch = parse_epoch_string(row.expires_at);
                for (std::size_t i = 3; i < parts.size(); ++i) {
                    row.values.push_back(unescape_field(parts[i]));
                }
                pending_rows.push_back(std::move(row));
            } else if (parts[0] == "COMMIT") {
                if (current_txid < 0 || parts.size() != 2 || std::stoll(parts[1]) != current_txid) {
                    pending_rows.clear();
                    current_txid = -1;
                    continue;
                }
                if (pending_rows.size() == expected_rows) {
                    apply_rows_in_memory(table, pending_rows);
                    applied_txids_.insert(current_txid);
                }
                pending_rows.clear();
                current_txid = -1;
            }
        }
    }
    ensure_data_file_open(table);
    tables_[table.name] = std::move(table);
}

void StorageEngine::rebuild_primary_index(Table &table) {
    table.primary_index.clear();
    if (!table.primary_key_column) {
        return;
    }
    const std::size_t pk_index = *table.primary_key_column;
    for (std::size_t i = 0; i < table.rows.size(); ++i) {
        if (!table.rows[i].deleted && !is_expired(table.rows[i])) {
            table.primary_index[table.rows[i].values[pk_index]] = i;
        }
    }
}

void StorageEngine::close_table_files() {
    for (auto &[name, table] : tables_) {
        (void)name;
        if (table.data_fd >= 0) {
            ::close(table.data_fd);
            table.data_fd = -1;
        }
    }
}

void StorageEngine::close_wal_file() {
    if (wal_fd_ >= 0) {
        ::close(wal_fd_);
        wal_fd_ = -1;
    }
}

void StorageEngine::ensure_data_file_open(Table &table) {
    if (table.data_fd >= 0) {
        return;
    }
    const auto path = table_data_path(table.name);
    table.data_fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (table.data_fd < 0) {
        throw std::runtime_error("failed to open data file for append");
    }
}

std::filesystem::path StorageEngine::wal_path() const {
    return root_dir_ / "flexql.wal";
}

std::vector<StorageEngine::WalTransaction> StorageEngine::parse_wal_stream(std::istream &in) const {
    std::vector<WalTransaction> transactions;
    std::string line;
    WalTransaction current;
    std::size_t expected_rows = 0;
    bool in_tx = false;
    while (std::getline(in, line)) {
        if (trim(line).empty()) {
            continue;
        }
        const auto parts = split_tsv(line);
        if (parts.empty()) {
            continue;
        }
        if (parts[0] == "BEGIN") {
            if (parts.size() != 4) {
                in_tx = false;
                current = WalTransaction{};
                continue;
            }
            current = WalTransaction{};
            current.txid = std::stoll(parts[1]);
            current.table_name = unescape_field(parts[2]);
            expected_rows = static_cast<std::size_t>(std::stoull(parts[3]));
            in_tx = true;
        } else if (parts[0] == "ROW" && in_tx) {
            if (parts.size() < 3 || std::stoll(parts[1]) != current.txid) {
                in_tx = false;
                current = WalTransaction{};
                continue;
            }
            RowRecord row;
            row.expires_at = unescape_field(parts[2]);
            row.expires_at_epoch = parse_epoch_string(row.expires_at);
            for (std::size_t i = 3; i < parts.size(); ++i) {
                row.values.push_back(unescape_field(parts[i]));
            }
            current.rows.push_back(std::move(row));
        } else if (parts[0] == "COMMIT" && in_tx) {
            if (parts.size() == 2 && std::stoll(parts[1]) == current.txid && current.rows.size() == expected_rows) {
                transactions.push_back(current);
            }
            in_tx = false;
            current = WalTransaction{};
        }
    }
    return transactions;
}

void StorageEngine::load_wal() {
    if (!std::filesystem::exists(wal_path())) {
        wal_fd_ = ::open(wal_path().c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (wal_fd_ < 0) {
            throw std::runtime_error("failed to create WAL file");
        }
        durable_fsync_file(wal_path());
        durable_fsync_dir(root_dir_);
        return;
    }
    std::ifstream in(wal_path());
    for (const auto &tx : parse_wal_stream(in)) {
        next_txid_ = std::max(next_txid_, tx.txid + 1);
    }
    wal_fd_ = ::open(wal_path().c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (wal_fd_ < 0) {
        throw std::runtime_error("failed to open WAL file");
    }
}

void StorageEngine::replay_wal() {
    std::ifstream in(wal_path());
    for (const auto &tx : parse_wal_stream(in)) {
        if (applied_txids_.contains(tx.txid)) {
            continue;
        }
        auto it = tables_.find(tx.table_name);
        if (it == tables_.end()) {
            continue;
        }
        Table &table = it->second;
        if (table.primary_key_column) {
            for (const auto &row : tx.rows) {
                const std::string &pk_value = row.values[*table.primary_key_column];
                if (table.primary_index.contains(pk_value)) {
                    throw std::runtime_error("WAL replay found duplicate primary key value: " + pk_value);
                }
            }
        }
        append_rows_to_disk(table, tx.txid, tx.rows);
        apply_rows_in_memory(table, tx.rows);
        applied_txids_.insert(tx.txid);
    }
}


std::filesystem::path StorageEngine::table_schema_path(const std::string &table_name) const {
    return root_dir_ / (table_name + ".schema");
}

std::filesystem::path StorageEngine::table_data_path(const std::string &table_name) const {
    return root_dir_ / (table_name + ".data");
}

}  // namespace flexql
