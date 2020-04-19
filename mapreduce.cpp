#include "mapreduce.h"

Mapper::Mapper(std::shared_ptr<Executor> executor,
               const std::string& script_path,
               const std::string& source_path,
               const std::string& result_path,
               size_t block_size)
    : executor_(executor), id_(std::to_string(std::random_device()())), script_path_(script_path),
    source_path_(source_path), result_path_(result_path), block_size_(block_size) {
}

void Mapper::Run() {
    std::ifstream source(source_path_);
    size_t process_no = 0;
    std::vector<FuturePtr<std::string>> processes;
    while (source.peek() != EOF) {
        std::string tmp_filename = "map_" + id_ + std::to_string(process_no++);
        std::ofstream tmp(tmp_filename + "_data");
        std::string buf;
        size_t cnt_lines = 0;
        while (cnt_lines++ < block_size_ && std::getline(source, buf)) {
            tmp << buf << '\n';
        }

        processes.push_back(std::make_shared<Future<std::string>>([this, tmp_filename] {
          bp::system("./" + script_path_, bp::std_out > (tmp_filename + "_result"),
                     bp::std_in < (tmp_filename + "_data"));
          bp::system("rm " + tmp_filename + "_data");
          return tmp_filename + "_result";
        }));

        executor_->submit(processes.back());
    }

    auto all_future = executor_->whenAll(processes);
    auto joiner = std::make_shared<Joiner>(all_future, result_path_, true);
    joiner->addDependency(all_future);
    executor_->submit(joiner);

    addDependency(joiner);

    function_ = [joiner] {
      return joiner->get();
    };
}

Joiner::Joiner(const FuturePtr<std::vector<std::string>>& source_paths_future,
    const std::string& result_path, bool remove_sources) {
    function_ = [=] {
        std::ofstream result(result_path);
        std::vector<std::string> source_paths = source_paths_future->get();
        for (auto& source_path : source_paths) {
            std::ifstream source(source_path);
            std::string buf;
            while (std::getline(source, buf)) {
                result << buf << '\n';
            }
            if (remove_sources) {
                bp::system("rm " + source_path);
            }
        }

        return result_path;
    };
}

NaiveSorter::NaiveSorter(const std::string& source_path, const std::string& result_path, bool remove_source) {
    function_ = [=] {
      std::ifstream source(source_path);
      std::vector<std::pair<std::string, std::string>> items;
      std::string key;
      std::string value;
      while (std::getline(source, key, '\t')) {
          std::getline(source, value);
          items.emplace_back(key, value);
      }
      std::sort(items.begin(), items.end());
      std::ofstream result(result_path);
      for (const auto& [key, value] : items) {
          result << key << '\t' << value << '\n';
      }

      if (remove_source) {
          bp::system("rm " + source_path);
      }

      return result_path;
    };
}

Merger::Merger(FuturePtr<std::string> first_source_path, FuturePtr<std::string> second_source_path,
               const std::string& result_path, bool remove_sources) {
    function_ = [=] {
      std::ifstream first_source(first_source_path->get());
      std::ifstream second_source(second_source_path->get());
      std::ofstream result(result_path);
      std::string first_key;
      std::string first_value;
      std::string second_key;
      std::string second_value;
      std::getline(first_source, first_key, '\t');
      std::getline(second_source, second_key, '\t');
      while (first_source.peek() != EOF || second_source.peek() != EOF) {
          if (second_source.peek() == EOF || (first_source.peek() != EOF && first_key < second_key)) {
              std::getline(first_source, first_value);
              result << first_key << '\t' << first_value << '\n';
              std::getline(first_source, first_key, '\t');
          } else {
              std::getline(second_source, second_value);
              result << second_key << '\t' << second_value << '\n';
              std::getline(second_source, second_key, '\t');
          }
      }

      if (remove_sources) {
          bp::system("rm " + first_source_path->get());
          bp::system("rm " + second_source_path->get());
      }

      return result_path;
    };
}

Sorter::Sorter(std::shared_ptr<Executor> executor,
               const std::string& source_path,
               const std::string& result_path,
               size_t block_size)
    : executor_(executor), id_(std::to_string(std::random_device()())),
      source_path_(source_path), result_path_(result_path), block_size_(block_size) {
}

void Sorter::Run() {
    std::ifstream source(source_path_);
    std::vector<FuturePtr<std::string>> processes;
    while (source.peek() != EOF) {
      std::string tmp_filename = "sort_" + id_ + std::to_string(processes_count++);
      std::ofstream tmp(tmp_filename + "_data");
      std::string buf;
      size_t cnt_lines = 0;
      while (cnt_lines++ < block_size_ && std::getline(source, buf)) {
          tmp << buf << '\n';
      }

      processes.push_back(std::make_shared<NaiveSorter>(tmp_filename + "_data", tmp_filename + "_result", true));

      executor_->submit(processes.back());
    }

    auto merged_future = RecursiveMergeSort(0, processes.size(), processes);

    addDependency(merged_future);

    function_ = [merged_future] {
      return merged_future->get();
    };
}

FuturePtr<std::string> Sorter::RecursiveMergeSort(size_t begin, size_t end, std::vector<FuturePtr<std::string>> futures) {
    if (begin + 1 == end) {
        return futures[begin];
    }
    size_t mid = begin + (end - begin) / 2;
    auto left = RecursiveMergeSort(begin, mid, futures);
    auto right = RecursiveMergeSort(mid, end, futures);
    std::string result_path = "recursive_merge_" + id_ + std::to_string(processes_count++);
    if (end - begin == futures.size()) {
        result_path = result_path_;
    }
    auto merged = std::make_shared<Merger>(left, right, result_path, true);
    merged->addDependency(left);
    merged->addDependency(right);
    executor_->submit(merged);
    return merged;
}

FuturePtr<std::string> Sort(std::shared_ptr<Executor> executor,
                            const std::string& source_path,
                            const std::string& result_path) {
    auto sorter = std::make_shared<Sorter>(executor, source_path, result_path);
    sorter->Run();
    return sorter;
}

FuturePtr<std::string> Map(std::shared_ptr<Executor> executor,
                           const std::string& script_path,
                           const std::string& source_path,
                           const std::string& result_path) {
    auto mapper = std::make_shared<Mapper>(executor, script_path, source_path, result_path);
    mapper->Run();
    return mapper;
}
