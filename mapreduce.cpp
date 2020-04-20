#include "mapreduce.h"

size_t TableTask::tasks_count = 0;

TableTask::TableTask(std::string name) : name_(std::move(name)), id_(std::to_string(tasks_count++)) {
}

TableTask::TableTask(std::string name,
                     ExecutorPtr executor,
                     std::string source_path,
                     std::string result_path,
                     bool remove_source)
    : name_(std::move(name)),
      executor_(std::move(executor)),
      source_path_(std::move(source_path)),
      result_path_(std::move(result_path)),
      id_(std::to_string(tasks_count++)),
      remove_source_(remove_source) {
}

std::string TableTask::GetNewProcessName() {
    return name_ + "_" + id_ + "_" + std::to_string(processes_count_++);
}

TableTask::~TableTask() {
    if (!source_path_.empty() && remove_source_) {
        bp::system("rm " + source_path_);
    }
}

template<class Operation, class ...Args>
TableTaskPtr Run(ExecutorPtr executor, Args ...args) {
    TableTaskPtr task = std::make_shared<Operation>(executor, args...);
    task->Submit();
    executor->submit(task);
    return task;
}

Joiner::Joiner(std::shared_ptr<Executor> executor,
               std::vector<TableTaskPtr> source_path_tasks,
               std::string result_path,
               bool remove_source)
    : TableTask("join", std::move(executor), "", std::move(result_path), remove_source),
      source_path_tasks_(std::move(source_path_tasks)) {
}

void Joiner::Submit() {
    auto all_future = executor_->whenAll<TableTaskPtr, std::string>(source_path_tasks_);
    addDependency(all_future);
    function_ = [this, all_future] {
        TableWriter result(result_path_);
        std::vector<std::string> source_paths = all_future->get();
        for (const auto& source_path : source_paths) {
            result.Append(source_path);
        }

        return result_path_;
    };
}

Joiner::~Joiner() {
    if (remove_source_) {
        for (const auto& task : source_path_tasks_) {
            std::string source_path = task->get();
            bp::system("rm " + source_path);
        }
    }
}

TableTaskPtr Join(std::shared_ptr<Executor> executor,
                  std::vector<TableTaskPtr> source_path_tasks,
                  std::string result_path,
                  bool remove_source) {
    return Run<Joiner>(std::move(executor), std::move(source_path_tasks), std::move(result_path), remove_source);
}

Performer::Performer(std::shared_ptr<Executor> executor,
                     std::string source_path,
                     std::string result_path,
                     std::string script_path,
                     bool remove_source)
    : TableTask("perform", std::move(executor), std::move(source_path), std::move(result_path), remove_source),
      script_path_(std::move(script_path)) {
}

void Performer::Submit() {
    function_ = [this] {
        bp::system("./" + script_path_, bp::std_out > result_path_, bp::std_in < source_path_);
        return result_path_;
    };
}

TableTaskPtr Perform(ExecutorPtr executor,
                     std::string source_path,
                     std::string result_path,
                     std::string script_path,
                     bool remove_source) {
    return Run<Performer>(std::move(executor), std::move(source_path), std::move(result_path),
                          std::move(script_path), remove_source);
}

Mapper::Mapper(std::shared_ptr<Executor> executor,
               std::string source_path,
               std::string result_path,
               std::string script_path,
               size_t block_size,
               bool remove_source)
    : TableTask("map", std::move(executor), std::move(source_path), std::move(result_path), remove_source),
      script_path_(std::move(script_path)),
      block_size_(block_size) {
}

void Mapper::Submit() {
    TableReader source(source_path_);
    std::vector<TableTaskPtr> processes;
    while (!source.Empty()) {
        std::string process_name = GetNewProcessName();
        std::string process_data_filename = process_name + "_data";
        std::string process_result_filename = process_name + "_result";
        TableWriter process_data(process_data_filename);

        process_data.Append(source, block_size_);

        processes.push_back(Perform(executor_, process_data_filename, process_result_filename,
                                    script_path_, true));
    }

    auto joiner = Join(executor_, processes, result_path_, true);

    addDependency(joiner);

    function_ = [joiner] {
        return joiner->get();
    };
}

TableTaskPtr Map(ExecutorPtr executor,
                 std::string source_path,
                 std::string result_path,
                 std::string script_path,
                 size_t block_size,
                 bool remove_source) {
    return Run<Mapper>(std::move(executor), std::move(source_path), std::move(result_path),
                       std::move(script_path), block_size, remove_source);
}

NaiveSorter::NaiveSorter(ExecutorPtr executor,
                         std::string source_path,
                         std::string result_path,
                         bool remove_source)
    : TableTask("naive_sort", std::move(executor), std::move(source_path), std::move(result_path), remove_source) {
}

void NaiveSorter::Submit() {
    function_ = [=] {
        auto items = TableReader(source_path_).ReadAllItems();

        std::sort(items.begin(), items.end());

        TableWriter result(result_path_);
        result.Write(items);

        return result_path_;
    };
}

TableTaskPtr NaiveSort(ExecutorPtr executor,
                       std::string source_path,
                       std::string result_path,
                       bool remove_source) {
    return Run<NaiveSorter>(std::move(executor), std::move(source_path), std::move(result_path), remove_source);
}

Merger::Merger(ExecutorPtr executor,
               TableTaskPtr first_source_task,
               TableTaskPtr second_source_task,
               std::string result_path,
               bool remove_sources)
    : TableTask("merge", std::move(executor), "", std::move(result_path), remove_sources),
      first_source_task_(std::move(first_source_task)),
      second_source_task_(std::move(second_source_task)) {
}

void Merger::Submit() {
    addDependency(first_source_task_);
    addDependency(second_source_task_);

    function_ = [=] {
        TableReader first_source(first_source_task_->get());
        TableReader second_source(second_source_task_->get());
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

        return result_path_;
    };
}

Merger::~Merger() {
    if (remove_source_) {
        bp::system("rm " + first_source_task_->get());
        bp::system("rm " + second_source_task_->get());
    }
}

TableTaskPtr Merge(ExecutorPtr executor,
                   TableTaskPtr first_source_path,
                   TableTaskPtr second_source_path,
                   std::string result_path,
                   bool remove_source) {
    return Run<Merger>(std::move(executor), std::move(first_source_path), std::move(second_source_path),
                       std::move(result_path), remove_source);
}

Sorter::Sorter(ExecutorPtr executor,
               std::string source_path,
               std::string result_path,
               size_t block_size,
               bool remove_source)
    : TableTask("sort", std::move(executor), std::move(source_path), std::move(result_path), remove_source),
      block_size_(block_size) {
}

void Sorter::Submit() {
    TableReader source(source_path_);
    std::vector<TableTaskPtr> processes;
    while (!source.Empty()) {
        std::string process_filename = GetNewProcessName();
        std::string process_data_filename = process_filename + "_data";
        std::string process_result_filename = process_filename + "_result";
        TableWriter process_data(process_data_filename);
        process_data.Append(source, block_size_);

        processes.push_back(Run<NaiveSorter>(executor_, process_data_filename, process_result_filename, true));
    }

    auto merged_future = RecursiveMergeSort(0, processes.size(), processes);

    addDependency(merged_future);

    function_ = [merged_future] {
        return merged_future->get();
    };
}

TableTaskPtr Sorter::RecursiveMergeSort(size_t begin, size_t end, const std::vector<TableTaskPtr>& tasks) {
    if (begin + 1 == end) {
        return tasks[begin];
    }
    size_t mid = begin + (end - begin) / 2;
    auto left = RecursiveMergeSort(begin, mid, tasks);
    auto right = RecursiveMergeSort(mid, end, tasks);
    std::string result_path = GetNewProcessName();
    if (end - begin == tasks.size()) {
        result_path = result_path_;
    }
    auto merged = Run<Merger>(executor_, left, right, result_path, true);

    return merged;
}

TableTaskPtr Sort(ExecutorPtr executor,
                  std::string source_path,
                  std::string result_path,
                  size_t block_size,
                  bool remove_source) {
    return Run<Sorter>(std::move(executor), std::move(source_path), std::move(result_path), block_size, remove_source);
}

Reducer::Reducer(ExecutorPtr executor,
                 std::string source_path,
                 std::string result_path,
                 std::string script_path,
                 bool remove_source)
    : TableTask("reduce", std::move(executor), std::move(source_path), std::move(result_path), remove_source),
      script_path_(std::move(script_path)) {
}

void Reducer::Submit() {
    TableReader source(source_path_);
    std::vector<TableTaskPtr> processes;
    while (!source.Empty()) {
        std::string process_name = GetNewProcessName();
        std::string process_data_filename = process_name + "_data";
        std::string process_result_filename = process_name + "_result";

        TableWriter process_data(process_data_filename);
        process_data.WriteKeyBlock(source);

        processes.push_back(Perform(executor_, process_data_filename, process_result_filename,
                                    script_path_, true));
    }

    auto joiner = Join(executor_, processes, result_path_, true);

    addDependency(joiner);

    function_ = [joiner] {
        return joiner->get();
    };
}

TableTaskPtr Reduce(ExecutorPtr executor,
                    std::string source_path,
                    std::string result_path,
                    std::string script_path,
                    bool remove_source) {
    return Run<Reducer>(std::move(executor), std::move(source_path), std::move(result_path),
                        std::move(script_path), remove_source);
}
