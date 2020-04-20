#pragma once
#include <string>
#include <fstream>
#include <vector>

using TableItem = std::pair<std::string, std::string>;

class TableReader {
public:
    explicit TableReader(const std::string& table_path);

    bool Next();

    bool HasNext();

    bool Empty();

    const std::string& GetKey() const;

    const std::string& GetValue() const;

    std::string GetRow() const;

    TableItem GetItem() const;

    std::vector<TableItem> ReadAllItems();

private:
    bool empty_ = false;
    std::string key_;
    std::string value_;
    std::ifstream table_stream_;
};

class TableWriter {
public:
    explicit TableWriter(const std::string& table_path);

    void Write(const std::string& key, const std::string& value);

    void Write(const std::string& row);

    void Write(const TableItem & item);

    void Write(const std::vector<TableItem>& items);

    void Append(TableReader& reader, size_t max_count = -1);

    void Append(const std::string& source_path, size_t max_conut = -1);

private:
    std::ofstream table_stream_;
};