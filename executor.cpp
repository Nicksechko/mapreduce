#include "executor.h"
#include <cassert>

void Task::addDependency(std::shared_ptr<Task> dep) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    assert(!isTaskStarted());
    if (dep->addDependant(shared_from_this())) {
        dependency_count_ = dependency_count_.value_or(0) + 1;
    }
}

void Task::addTrigger(std::shared_ptr<Task> dep) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    assert(!isTaskStarted());
    trigger_ = trigger_.value_or(false);
    if (!dep->addTriggered(shared_from_this())) {
        trigger_ = true;
        submit(lock);
    }
}

void Task::setTimeTrigger(std::chrono::system_clock::time_point at) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    assert(!isTaskStarted());
    time_trigger_ = at;
}

bool Task::isCompleted() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return isTaskCompleted();
}

bool Task::isFailed() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return isTaskFailed();
}

bool Task::isCanceled() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return isTaskCanceled();
}

bool Task::isFinished() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return isTaskFinished();
}

std::exception_ptr Task::getError() const {
    return err_;
}

void Task::cancel() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!isTaskStarted()) {
        status_ = TaskStatus::Canceled;
        finish(lock);
    }
}

void Task::wait() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    finished_cv_.wait(lock, [this] { return isTaskFinished(); });
}

bool Task::isTaskCompleted() const {
    return status_ == TaskStatus::Completed;
}

bool Task::isTaskFailed() const {
    return status_ == TaskStatus::Failed;
}

bool Task::isTaskCanceled() const {
    return status_ == TaskStatus::Canceled;
}

bool Task::isTaskFinished() const {
    return static_cast<int>(status_) > 1;
}

bool Task::isTaskStarted() const {
    return status_ != TaskStatus::Created;
}

bool Task::canSubmit() const {
    return static_cast<int>(status_) < 0 && !executor_.expired() &&
        dependency_count_.value_or(0) == 0 && trigger_.value_or(true) &&
        !(!dependency_count_.has_value() && !trigger_.has_value() && time_trigger_.has_value());
}

bool Task::setInProgress() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (status_ == TaskStatus::Pending || status_ == TaskStatus::Timered) {
        status_ = TaskStatus::InProgress;
        return true;
    } else {
        return false;
    }
}

void Task::setFailed(std::exception_ptr err) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    err_ = std::move(err);
    status_ = TaskStatus::Failed;
    finish(lock);
}

void Task::setCompleted() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    status_ = TaskStatus::Completed;
    finish(lock);
}

void Task::finish(std::unique_lock<std::shared_mutex>& lock) {
    finished_cv_.notify_all();
    lock.unlock();
    for (const auto& weak_dependant : dependants_) {
        if (!weak_dependant.expired()) {
            auto dependant = weak_dependant.lock();
            dependant->removeDependency();
        }
    }
    for (const auto& weak_triggered : triggers_) {
        if (!weak_triggered.expired()) {
            auto triggered = weak_triggered.lock();
            triggered->trigger();
        }
    }
    lock.lock();
}

void Task::submit(std::unique_lock<std::shared_mutex>& lock) {
    if (!canSubmit()) {
        return;
    }
    status_ = TaskStatus::Pending;
    lock.unlock();
    std::shared_ptr<Executor> executor = executor_.lock();
    bool accepted = executor->addToDo(shared_from_this());
    lock.lock();
    if (!accepted) {
        status_ = TaskStatus::Canceled;
    }
}

void Task::submitTimer(std::unique_lock<std::shared_mutex>& lock) {
    if (time_trigger_.has_value()) {
        status_ = TaskStatus::Timered;
        lock.unlock();
        std::shared_ptr<Executor> executor = executor_.lock();
        executor->addTimerTask(time_trigger_.value(), shared_from_this());
        lock.lock();
    }
}

bool Task::addDependant(std::shared_ptr<Task> dependant) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!isTaskFinished()) {
        dependants_.push_back(dependant);
        return true;
    } else {
        return false;
    }
}

bool Task::addTriggered(std::shared_ptr<Task> triggered) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!isTaskFinished()) {
        triggers_.push_back(triggered);
        return true;
    } else {
        return false;
    }
}

void Task::removeDependency() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    assert(dependency_count_.has_value());
    dependency_count_ = dependency_count_.value() - 1;
    if (dependency_count_ == 0) {
        submit(lock);
    }
}

void Task::trigger() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    trigger_ = true;
    submit(lock);
}

bool Task::setExecutor(std::shared_ptr<Executor> executor) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (executor_.expired()) {
        executor_ = executor;
        submitTimer(lock);
        submit(lock);
        return true;
    } else {
        return false;
    }
}

void TimerHeap::push(std::chrono::system_clock::time_point timer, std::shared_ptr<Task> task) {
    std::unique_lock<std::mutex> lock(mutex_);
    timers_.emplace(timer, task);
    timer_cv_.notify_one();
}

std::shared_ptr<Task> TimerHeap::pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    timer_cv_.wait(lock, [this] { return !timers_.empty() || stopped_; });
    if (stopped_) {
        return nullptr;
    }
    auto item = timers_.top();
    timers_.pop();
    while (std::chrono::system_clock::now() < item.first) {
        timer_cv_.wait_until(lock, item.first, [this] { return !timers_.empty() || stopped_; });
        timers_.push(item);
        if (stopped_) {
            return nullptr;
        }
        item = timers_.top();
        timers_.pop();
    }
    return item.second;
}

void TimerHeap::stop() {
    std::unique_lock<std::mutex> lock(mutex_);
    stopped_ = true;
    timer_cv_.notify_all();
}

Executor::Executor(size_t concurrency_) {
    for (size_t i = 0; i < concurrency_; ++i) {
        threads_.emplace_back([this] {
          std::unique_lock<std::mutex> lock(mutex_);
          while (true) {
              queue_cv_.wait(lock, [this] { return !to_do_list_.empty() || shut_down_; });
              if (shut_down_) {
                  break;
              }
              while (!to_do_list_.empty() && !to_do_list_.front()->setInProgress()) {
                  to_do_list_.pop();
              }
              if (to_do_list_.empty()) {
                  continue;
              }
              std::shared_ptr<Task> task = to_do_list_.front();
              to_do_list_.pop();

              lock.unlock();
              try {
                  task->run();
                  task->setCompleted();
              } catch (...) {
                  task->setFailed(std::current_exception());
              }
              lock.lock();

              submited_tasks_.erase(task);
          }
        });
        threads_.emplace_back([this] {
          while (!shut_down_) {
              std::shared_ptr<Task> task = timer_heap_.pop();
              if (task) {
                  addToDo(task);
              }
          }
        });
    }
}

Executor::~Executor() {
    setShutDown();
    joinThreads();
}

void Executor::submit(std::shared_ptr<Task> task) {
    if (task->setExecutor(shared_from_this())) {
        std::unique_lock<std::mutex> lock(mutex_);
        submited_tasks_.insert(task);
    }
}

void Executor::startShutdown() {
    setShutDown();
}

void Executor::waitShutdown() {
    joinThreads();
}

void Executor::setShutDown() {
    if (in_shut_down_.test_and_set()) {
        return;
    }
    std::unique_lock<std::mutex> queue_lock(mutex_);
    std::unique_lock<std::mutex> shut_down_lock(shut_down_mutex_);
    shut_down_ = true;
    timer_heap_.stop();
    queue_cv_.notify_all();
    shut_down_cv_.notify_one();
}

void Executor::joinThreads() {
    if (in_join_.test_and_set()) {
        return;
    }
    std::unique_lock<std::mutex> lock(shut_down_mutex_);
    shut_down_cv_.wait(lock, [this] { return shut_down_.load(); });
    status_ = ExecutorStatus::Joining;
    for (auto& thread : threads_) {
        thread.join();
    }
    while (!to_do_list_.empty()) {
        to_do_list_.front()->cancel();
        to_do_list_.pop();
    }
    submited_tasks_.clear();
    status_ = ExecutorStatus::Finished;
    shut_down_cv_.notify_all();
}

bool Executor::addToDo(std::shared_ptr<Task> task) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!shut_down_) {
        to_do_list_.push(std::move(task));
        queue_cv_.notify_one();
        return true;
    } else {
        task->cancel();
        return false;
    }
}

void Executor::addTimerTask(std::chrono::system_clock::time_point timer,
                            std::shared_ptr<Task> task) {
    timer_heap_.push(timer, std::move(task));
}

std::shared_ptr<Executor> MakeThreadPoolExecutor(int num_threads) {
    return std::make_shared<Executor>(num_threads);
}
