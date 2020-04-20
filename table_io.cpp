#include "table_io.h"

TableReader::TableReader(const std::string& table_path)
    : table_stream_(table_path) {
}

bool TableReader::Next() {
    if (HasNext()) {
        std::getline(table_stream_, key_, '\t');
        std::getline(table_stream_, value_);
        return true;
    } else {
        empty_ = true;
        return false;
    }
}

bool TableReader::HasNext() {
    return table_stream_.peek() != EOF;
}

bool TableReader::Empty() {
    return empty_;
}

const std::string& TableReader::GetKey() const {
    return key_;
}

const std::string& TableReader::GetValue() const {
    return value_;
}

std::string TableReader::GetRow() const {
    return key_ + '\t' + value_;
}

std::pair<std::string, std::string> TableReader::GetItem() const {
    return std::make_pair(key_, value_);
}

std::vector<std::pair<std::string, std::string>> TableReader::ReadAllItems() {
    std::vector<std::pair<std::string, std::string>> result;
    while (Next()) {
        result.push_back(GetItem());
    }

    return result;
}

TableWriter::TableWriter(const std::string& table_path)
    : table_stream_(table_path) {
}

void TableWriter::Write(const std::string& key, const std::string& value) {
    table_stream_ << key << "\t" << value << "\n";
}

void TableWriter::Write(const std::string& row) {
    table_stream_ << row << '\n';
}

void TableWriter::Write(const std::pair<std::string, std::string>& item) {
    Write(item.first, item.second);
}

void TableWriter::Write(const std::vector<TableItem>& items) {
    for (const auto& item : items) {
        Write(item);
    }
}

void TableWriter::Append(TableReader& reader, size_t max_count) {
    size_t count = 0;
    while (count++ < max_count && reader.Next()) {
        Write(reader.GetRow());
    }
}

void TableWriter::Append(const std::string& source_path, size_t max_count) {
    TableReader reader(source_path);
    Append(reader, max_count);
}
