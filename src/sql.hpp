#pragma once

#include "common.hpp"

namespace flexql {

enum class ColumnType {
    Decimal,
    Varchar,
    Datetime
};

struct ColumnDef {
    std::string name;
    ColumnType type;
    bool primary_key = false;
};

enum class CommandType {
    CreateTable,
    Insert,
    Select
};

enum class CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge
};

struct Condition {
    std::string lhs;
    CompareOp op = CompareOp::Eq;
    std::string rhs;
    bool rhs_is_column = false;
};

struct CreateTableCommand {
    std::string table_name;
    std::vector<ColumnDef> columns;
};

struct InsertCommand {
    std::string table_name;
    std::vector<std::vector<std::string>> rows;
    std::optional<std::string> expires_at;
};

struct SelectCommand {
    std::vector<std::string> columns;
    std::string left_table;
    std::optional<std::string> right_table;
    std::optional<Condition> join_condition;
    std::optional<Condition> where_condition;
};

struct Command {
    CommandType type;
    CreateTableCommand create_table;
    InsertCommand insert;
    SelectCommand select;
};

Command parse_sql(const std::string &sql);

}  // namespace flexql
