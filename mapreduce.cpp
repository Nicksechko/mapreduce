#include "mapreduce.h"

Concatenater::Concatenater(std::shared_ptr<Executor> executor,
                           std::vector<std::string> source_path,
                           bool remove_source)
    : ITableTask("concatenate", std::move(executor), std::move(source_path), remove_source) {
}

void Concatenater::run() {
    result_path_ = GetNewFileName();
    TableWriter result(result_path_);
    for (const auto& source_path : source_path_) {
        result.Append(source_path);
    }
}

Performer::Performer(std::shared_ptr<Executor> executor,
                     std::string source_path,
                     std::string script_command,
                     bool remove_source)
    : ITableTask("perform", std::move(executor), std::move(source_path), remove_source),
      script_command_(std::move(script_command)) {
}

void Performer::run() {
    result_path_ = GetNewFileName();
    bp::system(script_command_, bp::std_out > result_path_, bp::std_in < source_path_);
}

Splitter::Splitter(ExecutorPtr executor,
                   std::string source_path,
                   bool remove_source,
                   size_t block_size,
                   bool by_key)
    : ITableTask("split", std::move(executor), std::move(source_path), remove_source),
      block_size_(block_size), by_key_(by_key) {
}

void Splitter::run() {
    TableReader source(source_path_);
    while (!source.Empty()) {
        std::string chunk_name = GetNewFileName();
        TableWriter chunk(chunk_name);

        if (by_key_) {
            chunk.WriteKeyBlock(source);
        } else {
            chunk.Append(source, block_size_);
        }

        result_path_.push_back(chunk_name);
    }
}

NaiveSorter::NaiveSorter(ExecutorPtr executor,
                         std::string source_path,
                         bool remove_source)
    : ITableTask("naive_sort", std::move(executor), std::move(source_path), remove_source) {
}

void NaiveSorter::run() {
    auto items = TableReader(source_path_).ReadAllItems();

    std::sort(items.begin(), items.end());

    result_path_ = GetNewFileName();
    TableWriter result(result_path_);
    result.Write(items);
}

Merger::Merger(ExecutorPtr executor,
               std::vector<std::string> source_path,
               bool remove_sources)
    : ITableTask("merge", std::move(executor), std::move(source_path), remove_sources) {
    if(source_path_.size() != 2) {
        throw std::runtime_error("Merge accept exactly 2 tables");
    }
}

void Merger::run() {
    result_path_ = GetNewFileName();
    TableReader first_source(source_path_[0]);
    TableReader second_source(source_path_[1]);
    TableWriter result(result_path_);
    while (!first_source.Empty() || !second_source.Empty()) {
        if (second_source.Empty() || (!first_source.Empty() && first_source.GetKey() < second_source.GetKey())) {
            result.Write(first_source.GetItem());
            first_source.Next();
        } else {
            result.Write(second_source.GetItem());
            second_source.Next();
        }
    }
}

ListMerger::ListMerger(ExecutorPtr executor,
                       std::vector<std::string> source_paths,
                       bool remove_source)
    : ITableTask("list_merge", std::move(executor), std::move(source_paths), remove_source) {
}

void ListMerger::run() {
    for (const auto& source_path : source_path_) {
        source_links_.push_back(GetNewFileName());
        bp::system("ln " + source_path + " " + source_links_.back());
    }
    result_path_ = RecursiveMerge(0, source_links_.size());
}

TableFuturePtr ListMerger::RecursiveMerge(size_t begin, size_t end) {
    if (begin + 1 == end) {
        return DummyFuture(source_links_[begin]);
    }

    size_t mid = begin + (end - begin) / 2;
    auto left = RecursiveMerge(begin, mid);
    auto right = RecursiveMerge(mid, end);

    return Merge(executor_, left, right, true);
}

TableFuturePtr Concatenate(ExecutorPtr executor,
                           MultiTableFuturePtr source_paths,
                           bool remove_source) {
    return Run<Concatenater, std::string>(std::move(executor), std::move(source_paths), remove_source);
}

TableFuturePtr Perform(ExecutorPtr executor,
                       TableFuturePtr source_path,
                       std::string script_command,
                       bool remove_source) {
    return Run<Performer, std::string>(std::move(executor), std::move(source_path), std::move(script_command),
                                       remove_source);
}

MultiTableFuturePtr Perform(ExecutorPtr executor,
                            MultiTableFuturePtr source_paths,
                            std::string script_command,
                            bool remove_source) {
    return RunForAll<Performer, std::string>(std::move(executor), std::move(source_paths), std::move(script_command),
                                             remove_source);
}

MultiTableFuturePtr Split(ExecutorPtr executor,
                          TableFuturePtr source_path,
                          bool remove_source,
                          size_t block_size,
                          bool by_key) {
    return Run<Splitter, std::vector<std::string>>(std::move(executor), std::move(source_path), remove_source,
                                                   block_size, by_key);
}

TableFuturePtr Map(ExecutorPtr executor,
                   TableFuturePtr source_path,
                   std::string script_command,
                   bool remove_source,
                   size_t block_size) {
    auto split_result = Split(executor, std::move(source_path), remove_source, block_size);
    auto perform_result = Perform(executor, split_result, std::move(script_command), true);
    return Concatenate(executor, perform_result, true);
}

TableFuturePtr NaiveSort(ExecutorPtr executor,
                         TableFuturePtr source_path,
                         bool remove_source) {
    return Run<NaiveSorter, std::string>(std::move(executor), std::move(source_path), remove_source);
}

MultiTableFuturePtr NaiveSort(ExecutorPtr executor,
                              MultiTableFuturePtr source_paths,
                              bool remove_source) {
    return RunForAll<NaiveSorter, std::string>(std::move(executor), std::move(source_paths), remove_source);
}

TableFuturePtr Merge(ExecutorPtr executor,
                     TableFuturePtr first_source_path,
                     TableFuturePtr second_source_path,
                     bool remove_source) {
    auto source_paths = executor->whenAll<std::string>({std::move(first_source_path), std::move(second_source_path)});
    return Run<Merger, std::string>(std::move(executor), source_paths, remove_source);
}

TableFuturePtr Merge(ExecutorPtr executor,
                     MultiTableFuturePtr source_paths,
                     bool remove_source) {
    auto double_future = Run<ListMerger, TableFuturePtr>(executor, std::move(source_paths), remove_source);
    return executor->redirect(double_future);
}

TableFuturePtr Sort(ExecutorPtr executor,
                    TableFuturePtr source_path,
                    bool remove_source,
                    size_t block_size) {
    auto split_result = Split(executor, std::move(source_path), remove_source, block_size);
    auto naive_sort_result = NaiveSort(executor, std::move(split_result), true);
    return Merge(executor, naive_sort_result, true);
}

TableFuturePtr Reduce(ExecutorPtr executor,
                      TableFuturePtr source_path,
                      std::string script_command,
                      bool remove_source,
                      size_t block_size) {
    auto split_result = Split(executor, std::move(source_path), remove_source, block_size, true);
    auto perform_result = Perform(executor, std::move(split_result), std::move(script_command), true);
    return Concatenate(executor, std::move(perform_result), true);
}

TableFuturePtr MapReduce(ExecutorPtr executor,
                         TableFuturePtr source_path,
                         std::string map_script_command,
                         std::string reduce_script_command,
                         bool remove_source,
                         size_t block_size) {
    auto map_result = Map(executor, std::move(source_path), std::move(map_script_command), remove_source, block_size);
    auto sort_result = Sort(executor, std::move(map_result), true, block_size);
    return Reduce(executor, std::move(sort_result), std::move(reduce_script_command), true, block_size);
}