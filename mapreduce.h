#pragma once
#include "executor.h"
#include <boost/process.hpp>
#include <fstream>
#include <random>
#include <string>

namespace bp = boost::process;

class Joiner : public Future<std::string> {
public:
    Joiner(const FuturePtr<std::vector<std::string>>& source_paths_future,
           const std::string& result_path, bool remove_source = false);
};

class Mapper : public Future<std::string> {
public:
    Mapper(std::shared_ptr<Executor> executor, const std::string& script_path,
           const std::string& source_path, const std::string& result_path, size_t block_size = 1);

    void Run();

protected:
    std::shared_ptr<Executor> executor_;
    size_t processes_count_ = 0;
    const std::string id_;
    const std::string& script_path_;
    const std::string source_path_;
    const std::string result_path_;
    const size_t block_size_;
};

class NaiveSorter : public Future<std::string> {
public:
    NaiveSorter(const std::string& source_path, const std::string& result_path, bool remove_source = false);
};

class Merger : public Future<std::string> {
public:
    Merger(FuturePtr<std::string> first_source_path, FuturePtr<std::string> second_source_path,
           const std::string& result_path, bool remove_sources = false);
};

class Sorter : public Future<std::string> {
public:
    Sorter(std::shared_ptr<Executor> executor, const std::string& source_path,
        const std::string& result_path, size_t block_size = 1);

    void Run();

protected:
    std::shared_ptr<Executor> executor_;
    size_t processes_count = 0;
    const std::string id_;
    const std::string source_path_;
    const std::string result_path_;
    const size_t block_size_;

    FuturePtr<std::string> RecursiveMergeSort(size_t begin, size_t end, std::vector<FuturePtr<std::string>> futures);
};

FuturePtr<std::string> Sort(std::shared_ptr<Executor> executor,
                            const std::string& source_path,
                            const std::string& result_path);

FuturePtr<std::string> Map(std::shared_ptr<Executor> executor,
                           const std::string& script_path,
                           const std::string& source_path,
                           const std::string& result_path);
