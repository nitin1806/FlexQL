#include "sql.hpp"

namespace flexql {

namespace {

bool starts_with_ci(std::string_view text, std::string_view prefix) {
    if (text.size() < prefix.size()) {
        return false;
    }
    return iequals(text.substr(0, prefix.size()), prefix);
}

std::size_t find_ci(std::string_view text, std::string_view needle, std::size_t start = 0) {
    if (needle.empty() || start >= text.size()) {
        return std::string::npos;
    }
    const std::string text_upper = to_upper(text);
    const std::string needle_upper = to_upper(needle);
    return text_upper.find(needle_upper, start);
}

ColumnType parse_type(const std::string &token) {
    std::string upper = to_upper(trim(token));
    const std::size_t paren_pos = upper.find('(');
    if (paren_pos != std::string::npos) {
        upper = trim(upper.substr(0, paren_pos));
    }
    if (upper == "DECIMAL" || upper == "INT" || upper == "INTEGER") {
        return ColumnType::Decimal;
    }
    if (upper == "VARCHAR" || upper == "TEXT") {
        return ColumnType::Varchar;
    }
    if (upper == "DATETIME") {
        return ColumnType::Datetime;
    }
    throw std::runtime_error("unsupported column type: " + token);
}

CompareOp parse_compare_op(const std::string &token) {
    if (token == "=") return CompareOp::Eq;
    if (token == "!=") return CompareOp::Ne;
    if (token == "<") return CompareOp::Lt;
    if (token == "<=") return CompareOp::Le;
    if (token == ">") return CompareOp::Gt;
    if (token == ">=") return CompareOp::Ge;
    throw std::runtime_error("unsupported operator: " + token);
}

Condition parse_condition(const std::string &text, bool rhs_is_column) {
    static const std::vector<std::string> operators = {"<=", ">=", "!=", "=", "<", ">"};
    for (const auto &op : operators) {
        const std::size_t pos = text.find(op);
        if (pos != std::string::npos) {
            Condition cond;
            cond.lhs = trim(text.substr(0, pos));
            cond.op = parse_compare_op(op);
            cond.rhs = trim(text.substr(pos + op.size()));
            cond.rhs_is_column = rhs_is_column;
            if (!rhs_is_column) {
                cond.rhs = unquote(cond.rhs);
            }
            return cond;
        }
    }
    throw std::runtime_error("invalid WHERE/JOIN condition");
}

CreateTableCommand parse_create_table(const std::string &sql) {
    const std::size_t open_paren = sql.find('(');
    const std::size_t close_paren = sql.rfind(')');
    if (open_paren == std::string::npos || close_paren == std::string::npos || close_paren <= open_paren) {
        throw std::runtime_error("invalid CREATE TABLE syntax");
    }
    const std::string header = trim(sql.substr(0, open_paren));
    const std::string body = sql.substr(open_paren + 1, close_paren - open_paren - 1);

    const std::size_t name_pos = find_ci(header, "CREATE TABLE");
    const std::string table_name = trim(header.substr(name_pos + std::string("CREATE TABLE").size()));
    if (table_name.empty()) {
        throw std::runtime_error("table name is required");
    }

    CreateTableCommand command;
    command.table_name = table_name;
    const auto parts = split_csv(body);
    for (const auto &part : parts) {
        std::istringstream iss(part);
        ColumnDef column;
        std::string type_name;
        std::string extra;
        if (!(iss >> column.name >> type_name)) {
            throw std::runtime_error("invalid column definition: " + part);
        }
        column.type = parse_type(type_name);
        std::string remaining;
        std::getline(iss, remaining);
        remaining = to_upper(trim(remaining));
        column.primary_key = remaining == "PRIMARY KEY";
        command.columns.push_back(column);
    }
    return command;
}

InsertCommand parse_insert(const std::string &sql) {
    const std::size_t values_pos = find_ci(sql, "VALUES");
    if (values_pos == std::string::npos) {
        throw std::runtime_error("INSERT requires VALUES");
    }
    const std::string header = trim(sql.substr(0, values_pos));
    const std::size_t table_pos = find_ci(header, "INSERT INTO");
    const std::string table_name = trim(header.substr(table_pos + std::string("INSERT INTO").size()));
    const std::size_t open_paren = sql.find('(', values_pos);
    if (open_paren == std::string::npos) {
        throw std::runtime_error("invalid INSERT syntax");
    }
    InsertCommand command;
    command.table_name = table_name;
    const std::size_t expires_pos = find_ci(sql, "EXPIRES AT", open_paren);
    const std::size_t values_end = expires_pos == std::string::npos ? sql.size() : expires_pos;
    const std::string values_part = trim(sql.substr(open_paren, values_end - open_paren));

    std::size_t pos = 0;
    while (pos < values_part.size()) {
        while (pos < values_part.size() &&
               (std::isspace(static_cast<unsigned char>(values_part[pos])) || values_part[pos] == ',')) {
            ++pos;
        }
        if (pos >= values_part.size()) {
            break;
        }
        if (values_part[pos] != '(') {
            throw std::runtime_error("invalid INSERT tuple list");
        }
        std::size_t depth = 0;
        bool in_quote = false;
        char quote = '\0';
        std::size_t end = pos;
        for (; end < values_part.size(); ++end) {
            const char c = values_part[end];
            if ((c == '\'' || c == '"') && (end == 0 || values_part[end - 1] != '\\')) {
                if (in_quote && c == quote) {
                    in_quote = false;
                } else if (!in_quote) {
                    in_quote = true;
                    quote = c;
                }
            } else if (!in_quote && c == '(') {
                ++depth;
            } else if (!in_quote && c == ')') {
                --depth;
                if (depth == 0) {
                    break;
                }
            }
        }
        if (end >= values_part.size() || depth != 0) {
            throw std::runtime_error("unterminated INSERT tuple");
        }
        std::vector<std::string> row;
        for (auto &value : split_csv(values_part.substr(pos + 1, end - pos - 1))) {
            row.push_back(unquote(trim(value)));
        }
        command.rows.push_back(std::move(row));
        pos = end + 1;
    }
    if (command.rows.empty()) {
        throw std::runtime_error("INSERT requires at least one row");
    }

    if (expires_pos != std::string::npos) {
        command.expires_at = unquote(trim(sql.substr(expires_pos + std::string("EXPIRES AT").size())));
    }
    return command;
}

SelectCommand parse_select(const std::string &sql) {
    const std::size_t from_pos = find_ci(sql, " FROM ");
    if (from_pos == std::string::npos) {
        throw std::runtime_error("SELECT requires FROM");
    }

    SelectCommand command;
    const std::string column_part = trim(sql.substr(std::string("SELECT").size(), from_pos - std::string("SELECT").size()));
    command.columns = split_csv(column_part);

    std::string tail = trim(sql.substr(from_pos + std::string(" FROM ").size()));
    const std::size_t join_pos = find_ci(tail, " INNER JOIN ");
    const std::size_t where_pos = find_ci(tail, " WHERE ");

    if (join_pos == std::string::npos) {
        command.left_table = trim(where_pos == std::string::npos ? tail : tail.substr(0, where_pos));
    } else {
        command.left_table = trim(tail.substr(0, join_pos));
        std::string join_tail = tail.substr(join_pos + std::string(" INNER JOIN ").size());
        const std::size_t on_pos = find_ci(join_tail, " ON ");
        if (on_pos == std::string::npos) {
            throw std::runtime_error("INNER JOIN requires ON");
        }
        command.right_table = trim(join_tail.substr(0, on_pos));
        const std::size_t where_pos_join = find_ci(join_tail, " WHERE ", on_pos);
        const std::string on_expr = trim(
            where_pos_join == std::string::npos
                ? join_tail.substr(on_pos + std::string(" ON ").size())
                : join_tail.substr(on_pos + std::string(" ON ").size(),
                                   where_pos_join - (on_pos + std::string(" ON ").size())));
        command.join_condition = parse_condition(on_expr, true);
    }

    if (where_pos != std::string::npos) {
        command.where_condition = parse_condition(trim(tail.substr(where_pos + std::string(" WHERE ").size())), false);
    } else if (join_pos != std::string::npos) {
        std::string join_tail = tail.substr(join_pos + std::string(" INNER JOIN ").size());
        const std::size_t where_pos_join = find_ci(join_tail, " WHERE ");
        if (where_pos_join != std::string::npos) {
            command.where_condition = parse_condition(trim(join_tail.substr(where_pos_join + std::string(" WHERE ").size())), false);
        }
    }
    return command;
}

}  // namespace

Command parse_sql(const std::string &sql_text) {
    std::string sql = trim(sql_text);
    if (!sql.empty() && sql.back() == ';') {
        sql.pop_back();
        sql = trim(sql);
    }
    if (starts_with_ci(sql, "CREATE TABLE")) {
        Command command;
        command.type = CommandType::CreateTable;
        command.create_table = parse_create_table(sql);
        return command;
    }
    if (starts_with_ci(sql, "INSERT INTO")) {
        Command command;
        command.type = CommandType::Insert;
        command.insert = parse_insert(sql);
        return command;
    }
    if (starts_with_ci(sql, "SELECT")) {
        Command command;
        command.type = CommandType::Select;
        command.select = parse_select(sql);
        return command;
    }
    throw std::runtime_error("unsupported SQL statement");
}

}  // namespace flexql
