#pragma once
#include "executor.h"
#include "table_io.h"
#include <boost/process.hpp>
#include <fstream>
#include <random>
#include <string>
#include <atomic>

namespace bp = boost::process;

namespace {
std::atomic<size_t> tasks_count{0};

const size_t default_block_size = 100;
}

template<class TIn, class TOut>
class ITableTask : public Task {
public:
    explicit ITableTask(std::string name)
        : name_(std::move(name)),
          id_(std::to_string(tasks_count++)) {
    }

    ITableTask(std::string name,
               ExecutorPtr executor,
               TIn source_path,
               bool remove_source = false)
        : name_(std::move(name)),
          executor_(std::move(executor)),
          source_path_(std::move(source_path)),
          id_(std::to_string(tasks_count++)),
          remove_source_(remove_source) {
    }

    TOut GetResult() {
        return result_path_;
    }

    std::string GetNewFileName() {
        return name_ + "_" + id_ + "_" + std::to_string(processes_count_++);
    }

    ~ITableTask() override {
        if (remove_source_) {
            if constexpr(std::is_same_v<TIn, std::string>) {
                bp::system("rm " + source_path_);
            } else if constexpr(std::is_same_v<TIn, std::vector<std::string>>) {
                for (const auto& source_path : source_path_) {
                    bp::system("rm " + source_path);
                }
            }
        }
    }

protected:
    ExecutorPtr executor_;
    TIn source_path_;
    TOut result_path_;
    size_t processes_count_ = 0;
    const std::string id_;
    const bool remove_source_ = false;
    const std::string name_;
};

using TableFuturePtr = FuturePtr<std::string>;
using MultiTableFuturePtr = FuturePtr<std::vector<std::string>>;

template<class T>
FuturePtr<T> DummyFuture(const T& data) {
    auto future = std::make_shared<Future<T>>();
    future->setResult(data);
    return future;
}

template<class Operator, class TOut, class TIn, class ...TArgs>
FuturePtr<TOut> Run(ExecutorPtr executor, FuturePtr<TIn> source_path, TArgs ...args) {
    auto future = std::make_shared<Future<TOut>>();
    executor->then<Unit>(source_path, [=] {
        auto task = std::make_shared<Operator>(executor, source_path->get(), args...);
        executor->submit(task);
        future->addDependency(task);
        future->setFunc([task] {
            return task->GetResult();
        });
        executor->submit(future);
        return Unit{};
    });

    return future;
}

template<class Operator, class TOut, class ...TArgs>
FuturePtr<std::vector<TOut>> RunForAll(ExecutorPtr executor, MultiTableFuturePtr source_paths, TArgs ...args) {
    auto double_future = executor->then<FuturePtr<std::vector<TOut>>>(source_paths, [=] {
        std::vector<FuturePtr<TOut>> futures;
        for (const auto& source_path : source_paths->get()) {
            futures.push_back(Run<Operator, TOut>(executor, DummyFuture(source_path), args...));
        }
        return executor->whenAll(futures);
    });

    return executor->redirect(double_future);
}

class Concatenater : public ITableTask<std::vector<std::string>, std::string> {
public:
    Concatenater(ExecutorPtr executor,
                 std::vector<std::string> source_path_,
                 bool remove_source = false);

    void run() override;
};

class Performer : public ITableTask<std::string, std::string> {
public:
    Performer(ExecutorPtr executor,
              std::string source_path,
              std::string script_command,
              bool remove_source = false);

    void run() override;

protected:
    const std::string script_command_;
};

class Splitter : public ITableTask<std::string, std::vector<std::string>> {
public:
    Splitter(ExecutorPtr executor,
             std::string source_path,
             bool remove_source = false,
             size_t block_size = default_block_size,
             bool by_key = false);

    void run() override;

protected:
    const size_t block_size_;
    const bool by_key_;
};

class NaiveSorter : public ITableTask<std::string, std::string> {
public:
    NaiveSorter(ExecutorPtr executor,
                std::string source_path,
                bool remove_source = false);

    void run() override;
};

class Merger : public ITableTask<std::vector<std::string>, std::string> {
public:
    Merger(ExecutorPtr executor,
           std::vector<std::string> first_source_path,
           bool remove_source = false);

    void run() override;
};

class ListMerger : public ITableTask<std::vector<std::string>, TableFuturePtr> {
public:
    ListMerger(ExecutorPtr executor,
               std::vector<std::string> source_paths,
               bool remove_source = false);

    void run() override;

protected:
    std::vector<std::string> source_links_;

    TableFuturePtr RecursiveMerge(size_t begin, size_t end);
};

TableFuturePtr Concatenate(ExecutorPtr executor,
                           MultiTableFuturePtr source_paths,
                           bool remove_source = false);


TableFuturePtr Perform(ExecutorPtr executor,
                       TableFuturePtr source_path,
                       std::string script_command,
                       bool remove_source = false);


MultiTableFuturePtr Perform(ExecutorPtr executor,
                            MultiTableFuturePtr source_path,
                            std::string script_command,
                            bool remove_source = false);

MultiTableFuturePtr Split(ExecutorPtr executor,
                          TableFuturePtr source_path,
                          bool remove_source = false,
                          size_t block_size = default_block_size,
                          bool by_key = false);

TableFuturePtr Map(ExecutorPtr executor,
                   TableFuturePtr source_path,
                   std::string script_command,
                   bool remove_source = false,
                   size_t block_size = default_block_size);


TableFuturePtr NaiveSort(ExecutorPtr executor,
                         TableFuturePtr source_path,
                         bool remove_source = false);

MultiTableFuturePtr NaiveSort(ExecutorPtr executor,
                              MultiTableFuturePtr source_path,
                              bool remove_source = false);

TableFuturePtr Merge(ExecutorPtr executor,
                     TableFuturePtr first_source_path,
                     TableFuturePtr second_source_path,
                     bool remove_source = false);

TableFuturePtr Merge(ExecutorPtr executor,
                     MultiTableFuturePtr source_paths,
                     bool remove_source = false);

TableFuturePtr Sort(ExecutorPtr executor,
                    TableFuturePtr source_path,
                    bool remove_source = false,
                    size_t block_size = default_block_size);

TableFuturePtr Reduce(ExecutorPtr executor,
                      TableFuturePtr source_path,
                      std::string script_command,
                      bool remove_source = false,
                      size_t block_size = default_block_size);

TableFuturePtr MapReduce(ExecutorPtr executor,
                         TableFuturePtr source_path,
                         std::string map_script_command,
                         std::string reduce_script_command,
                         bool remove_source = false,
                         size_t block_size = default_block_size);
