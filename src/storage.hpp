#pragma once

#include "sql.hpp"

namespace flexql {

struct QueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

class StorageEngine {
public:
    explicit StorageEngine(std::filesystem::path root_dir);
    ~StorageEngine();

    void load();
    QueryResult execute(const Command &command);

private:
    struct RowRecord {
        std::vector<std::string> values;
        std::string expires_at;
        long long expires_at_epoch = 0;
        bool deleted = false;
    };

    struct Table {
        std::string name;
        std::vector<ColumnDef> columns;
        std::unordered_map<std::string, std::size_t> column_index;
        std::vector<RowRecord> rows;
        std::unordered_map<std::string, std::size_t> primary_index;
        std::optional<std::size_t> primary_key_column;
        int data_fd = -1;
    };

    struct WalTransaction {
        long long txid = 0;
        std::string table_name;
        std::vector<RowRecord> rows;
    };

    struct CacheEntry {
        std::string key;
        QueryResult value;
    };

    class QueryCache {
    public:
        explicit QueryCache(std::size_t capacity) : capacity_(capacity) {}
        std::optional<QueryResult> get(const std::string &key);
        void put(const std::string &key, const QueryResult &value);
        void clear();

    private:
        std::size_t capacity_;
        std::list<CacheEntry> items_;
        std::unordered_map<std::string, std::list<CacheEntry>::iterator> index_;
    };

    QueryResult create_table(const CreateTableCommand &command);
    QueryResult insert_row(const InsertCommand &command);
    QueryResult select_rows(const SelectCommand &command);

    bool is_expired(const RowRecord &row) const;
    std::optional<std::size_t> find_primary_key_column(const Table &table) const;
    std::size_t require_column(const Table &table, const std::string &name) const;
    std::pair<const Table&, const Table*> resolve_tables(const SelectCommand &command) const;
    std::string get_value(const Table &table, const RowRecord &row, const std::string &column) const;
    bool compare_values(const std::string &lhs, CompareOp op, const std::string &rhs) const;
    bool evaluate_condition(
        const Condition &cond,
        const Table &left_table,
        const RowRecord &left_row,
        const Table *right_table,
        const RowRecord *right_row) const;
    std::string normalize_column_name(const std::string &expr, const std::string &default_table) const;
    RowRecord make_row_record(const Table &table, const std::vector<std::string> &values, const std::optional<std::string> &expires_at) const;
    std::string parse_expiry(const std::optional<std::string> &expires_at) const;
    std::string build_transaction_payload(long long txid, const std::vector<RowRecord> &rows, bool include_table_name, const std::string &table_name) const;
    void append_wal_record(long long txid, const std::string &table_name, const std::vector<RowRecord> &rows);
    void append_rows_to_disk(const Table &table, long long txid, const std::vector<RowRecord> &rows);
    void apply_rows_in_memory(Table &table, const std::vector<RowRecord> &rows);
    void replay_wal();
    void load_wal();
    std::vector<WalTransaction> parse_wal_stream(std::istream &in) const;
    void close_wal_file();
    void persist_schema(const Table &table);
    void load_table(const std::filesystem::path &schema_path);
    void rebuild_primary_index(Table &table);
    void close_table_files();
    void ensure_data_file_open(Table &table);
    std::filesystem::path table_schema_path(const std::string &table_name) const;
    std::filesystem::path table_data_path(const std::string &table_name) const;
    std::filesystem::path wal_path() const;

    std::filesystem::path root_dir_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, Table> tables_;
    QueryCache cache_{128};
    std::unordered_set<long long> applied_txids_;
    int wal_fd_ = -1;
    long long next_txid_ = 1;
};

}  // namespace flexql
