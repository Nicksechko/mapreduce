#include <memory>
#include <chrono>
#include <vector>
#include <functional>
#include <thread>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include <iostream>
#include <optional>
#include <shared_mutex>
#include <unordered_set>

class Executor;

class Task : public std::enable_shared_from_this<Task> {
public:
    virtual ~Task() {
    }

    virtual void run() = 0;

    void addDependency(std::shared_ptr<Task> dep);

    void addTrigger(std::shared_ptr<Task> dep);

    void setTimeTrigger(std::chrono::system_clock::time_point at);

    bool isCompleted() const;

    bool isFailed() const;

    bool isCanceled() const;

    bool isFinished() const;

    std::exception_ptr getError() const;

    void cancel();

    void wait();

protected:
    enum class TaskStatus {
        Created = -2,
        Timered = -1,
        Pending = 0,
        InProgress = 1,
        Completed = 2,
        Failed = 3,
        Canceled = 4
    };

    std::weak_ptr<Executor> executor_;

    mutable std::shared_mutex mutex_;
    mutable std::condition_variable_any finished_cv_;

    TaskStatus status_ = TaskStatus::Created;
    std::exception_ptr err_ = nullptr;

    std::optional<size_t> dependency_count_ = std::nullopt;
    std::vector<std::weak_ptr<Task>> dependants_;

    std::optional<bool> trigger_ = std::nullopt;
    std::vector<std::weak_ptr<Task>> triggers_;

    std::optional<std::chrono::system_clock::time_point> time_trigger_ = std::nullopt;

    bool isTaskCompleted() const;

    bool isTaskFailed() const;

    bool isTaskCanceled() const;

    bool isTaskFinished() const;

    bool isTaskStarted() const;

    bool canSubmit() const;

    bool setInProgress();

    void setFailed(std::exception_ptr err);

    void setCompleted();

    virtual void finish(std::unique_lock<std::shared_mutex>& lock);

    void submit(std::unique_lock<std::shared_mutex>& lock);

    void submitTimer(std::unique_lock<std::shared_mutex>& lock);

    bool addDependant(std::shared_ptr<Task> dependant);

    bool addTriggered(std::shared_ptr<Task> triggered);

    void removeDependency();

    void trigger();

    bool setExecutor(std::shared_ptr<Executor> executor);

    friend class Executor;
};

template <class T>
using MinPriorityQueue = std::priority_queue<T, std::vector<T>, std::greater<>>;

class TimerHeap {
public:
    void push(std::chrono::system_clock::time_point timer, std::shared_ptr<Task> task);

    std::shared_ptr<Task> pop();

    void stop();

private:
    bool stopped_ = false;
    mutable std::mutex mutex_;
    mutable std::condition_variable timer_cv_;
    MinPriorityQueue<std::pair<std::chrono::system_clock::time_point, std::shared_ptr<Task>>>
        timers_;
};

template <class T>
class Future;

template <class T>
using FuturePtr = std::shared_ptr<Future<T>>;

struct Unit {};

class Executor : public std::enable_shared_from_this<Executor> {
public:
    explicit Executor(size_t concurrency_);

    virtual ~Executor();

    virtual void submit(std::shared_ptr<Task> task);

    virtual void startShutdown();

    void waitShutdown();

    template <class T>
    FuturePtr<T> invoke(std::function<T()> fn) {
        auto future = std::make_shared<Future<T>>(fn);
        submit(future);
        return future;
    }

    template <class Y, class T>
    FuturePtr<Y> then(FuturePtr<T> input, std::function<Y()> fn) {
        auto future = std::make_shared<Future<Y>>(fn);
        future->addDependency(input);
        submit(future);
        return future;
    }

    template <class T>
    FuturePtr<std::vector<T>> whenAll(std::vector<FuturePtr<T>> all) {
        auto future = std::make_shared<Future<std::vector<T>>>([all] {
          std::vector<T> result;
          for (auto item : all) {
              result.push_back(static_cast<T>(item->get()));
          }
          return result;
        });
        for (auto item : all) {
            future->addDependency(item);
        }
        submit(future);
        return future;
    }

    template <class T>
    FuturePtr<T> whenFirst(std::vector<FuturePtr<T>> all) {
        auto future = std::make_shared<Future<T>>([all] {
          for (auto item : all) {
              if (item->isFinished()) {
                  return item->get();
              }
          }
          throw std::runtime_error("Bad trigger");
        });
        for (auto item : all) {
            future->addTrigger(item);
        }
        submit(future);
        return future;
    }

    template <class T>
    FuturePtr<std::vector<T>> whenAllBeforeDeadline(
        std::vector<FuturePtr<T>> all, std::chrono::system_clock::time_point deadline) {
        auto future = std::make_shared<Future<std::vector<T>>>([all] {
          std::vector<T> result;
          for (auto item : all) {
              if (item->isFinished()) {
                  result.push_back(item->get());
              }
          }
          return result;
        });
        future->setTimeTrigger(deadline);
        submit(future);
        return future;
    }

private:
    enum class ExecutorStatus {
        Running = 0,
        Joining = 1,
        Finished = 2,
    };

    std::atomic_flag in_shut_down_{false};
    std::atomic_flag in_join_{false};
    ExecutorStatus status_ = ExecutorStatus::Running;
    std::atomic<bool> shut_down_{false};
    mutable std::mutex shut_down_mutex_;
    mutable std::condition_variable shut_down_cv_;

    mutable std::mutex mutex_;
    mutable std::condition_variable queue_cv_;

    std::vector<std::thread> threads_;
    std::queue<std::shared_ptr<Task>> to_do_list_;

    std::unordered_set<std::shared_ptr<Task>> submited_tasks_;

    TimerHeap timer_heap_;

    void setShutDown();

    void joinThreads();

    bool addToDo(std::shared_ptr<Task> task);

    void addTimerTask(std::chrono::system_clock::time_point timer, std::shared_ptr<Task> task);

    friend class Task;
};

std::shared_ptr<Executor> MakeThreadPoolExecutor(int num_threads);

template <class T>
class Future : public Task {
public:
    Future() = default;

    explicit Future(std::function<T()> function) : function_(function) {
    }

    void run() override {
        T result = function_();
        std::unique_lock<std::shared_mutex> lock(Task::mutex_);
        result_ = result;
    }

    T get() const {
        std::shared_lock<std::shared_mutex> lock(Task::mutex_);
        result_cv_.wait(lock, [this] { return Task::isTaskFinished(); });
        if (Task::isTaskCompleted()) {
            return result_.value();
        } else if (Task::isTaskFailed()) {
            std::rethrow_exception(Task::getError());
        } else {
            throw std::runtime_error("Task was canceled");
        }
    }

protected:
    mutable std::condition_variable_any result_cv_;
    std::function<T()> function_;
    std::optional<T> result_ = std::nullopt;

    void finish(std::unique_lock<std::shared_mutex>& lock) override {
        Task::finish(lock);
        result_cv_.notify_all();
    }
};


