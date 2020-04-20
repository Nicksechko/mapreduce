#pragma once
#include "executor.h"
#include "table_io.h"
#include <boost/process.hpp>
#include <fstream>
#include <random>
#include <string>

namespace bp = boost::process;

class TableTask : public Future<std::string> {
public:
    explicit TableTask(std::string name);

    TableTask(std::string name,
              ExecutorPtr executor,
              std::string source_path,
              std::string result_path,
              bool remove_source = false);

    virtual void Submit() = 0;

    std::string GetNewProcessName();

    ~TableTask() override;

protected:
    ExecutorPtr executor_;
    std::string source_path_;
    std::string result_path_;
    size_t processes_count_ = 0;
    static size_t tasks_count;
    const std::string id_;
    const bool remove_source_ = false;
    const std::string name_;
};

using TableTaskPtr = std::shared_ptr<TableTask>;

template<class Task, class ...Targs>
TableTaskPtr Run(ExecutorPtr executor, Targs ...args);

class Joiner : public TableTask {
public:
    Joiner(ExecutorPtr executor,
           std::vector<TableTaskPtr> source_path_tasks,
           std::string result_path,
           bool remove_source = false);

    void Submit() override;

    ~Joiner() override;

protected:
    std::vector<TableTaskPtr> source_path_tasks_;
};

TableTaskPtr Join(std::shared_ptr<Executor> executor,
                  std::vector<TableTaskPtr> source_path_tasks,
                  std::string result_path,
                  bool remove_source = false);

class NaiveMapper : public TableTask {
public:
    NaiveMapper(ExecutorPtr executor,
                std::string source_path,
                std::string result_path,
                std::string script_path,
                bool remove_source = false);

    void Submit() override;

protected:
    const std::string script_path_;
};

TableTaskPtr NaiveMap(ExecutorPtr executor,
                      std::string source_path,
                      std::string result_path,
                      std::string script_path,
                      bool remove_source = false);

class Mapper : public TableTask {
public:
    Mapper(std::shared_ptr<Executor> executor,
           std::string source_path,
           std::string result_path,
           std::string script_path,
           size_t block_size = 1,
           bool remove_source = false);

    void Submit() override;

protected:
    const std::string script_path_;
    const size_t block_size_;
};

TableTaskPtr Map(ExecutorPtr executor,
                 std::string source_path,
                 std::string result_path,
                 std::string script_path,
                 size_t block_size = 1,
                 bool remove_source = false);

class NaiveSorter : public TableTask {
public:
    NaiveSorter(ExecutorPtr executor,
                std::string source_path,
                std::string result_path,
                bool remove_source = false);

    void Submit() override;
};

TableTaskPtr NaiveSort(ExecutorPtr executor,
                      std::string source_path,
                      std::string result_path,
                      bool remove_source = false);

class Merger : public TableTask {
public:
    Merger(ExecutorPtr executor,
           TableTaskPtr first_source_path,
           TableTaskPtr second_source_path,
           std::string result_path,
           bool remove_source = false);

    void Submit() override;

    ~Merger() override;

protected:
    TableTaskPtr first_source_task_;
    TableTaskPtr second_source_task_;
};

TableTaskPtr Merge(ExecutorPtr executor,
                   TableTaskPtr first_source_path,
                   TableTaskPtr second_source_path,
                   std::string result_path,
                   bool remove_source = false);

class Sorter : public TableTask {
public:
    Sorter(ExecutorPtr executor,
           std::string source_path,
           std::string result_path,
           size_t block_size = 1,
           bool remove_source = false);

    void Submit() override;

protected:
    const size_t block_size_;

    TableTaskPtr RecursiveMergeSort(size_t begin, size_t end, const std::vector<TableTaskPtr>& futures);
};


TableTaskPtr Sort(ExecutorPtr executor,
                  std::string source_path,
                  std::string result_path,
                  size_t block_size = 1,
                  bool remove_source = false);

//class Reducer : public TableTask {
//public:
//    Reducer(std::shared_ptr<Executor> executor, const std::string& script_path,
//           const std::string& source_path, const std::string& result_path);
//
//    void Run();
//
//protected:
//    std::shared_ptr<Executor> executor_;
//    size_t processes_count_ = 0;
//    const std::string id_;
//    const std::string& script_path_;
//    const std::string source_path_;
//    const std::string result_path_;
//    const size_t block_size_;
//};
