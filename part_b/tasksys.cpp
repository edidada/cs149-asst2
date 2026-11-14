#include "tasksys.h"
#include <algorithm>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

class DependencyPool {
    struct Group {
        IRunnable* runnable;
        int total, next = 0, remaining, waiting = 0;
        bool done = false;
        std::vector<TaskID> children;
        Group(IRunnable* r, int n): runnable(r), total(n), remaining(n) {}
    };
    std::mutex mutex;
    std::condition_variable work, completed;
    std::vector<std::thread> workers;
    std::vector<std::unique_ptr<Group>> groups;
    std::deque<TaskID> ready;
    size_t outstanding = 0;
    bool stopping = false;

    // Called with mutex held. Iterative propagation also handles empty chains.
    void activate(TaskID id) {
        std::vector<TaskID> pending(1, id);
        while (!pending.empty()) {
            TaskID current = pending.back();
            pending.pop_back();
            Group& g = *groups[current];
            if (g.remaining != 0) {
                ready.push_back(current);
                continue;
            }
            g.done = true;
            --outstanding;
            for (TaskID child : g.children)
                if (--groups[child]->waiting == 0) pending.push_back(child);
            g.children.clear();
        }
        completed.notify_all();
        work.notify_all();
    }
    void worker() {
        std::unique_lock<std::mutex> lock(mutex);
        for (;;) {
            work.wait(lock, [&] { return stopping || !ready.empty(); });
            if (stopping) return;
            TaskID id = ready.front();
            ready.pop_front();
            Group& g = *groups[id];
            int task = g.next++;
            if (g.next < g.total) ready.push_back(id);
            lock.unlock();
            g.runnable->runTask(task, g.total);
            lock.lock();
            if (--g.remaining == 0) activate(id);
        }
    }
public:
    explicit DependencyPool(int threads) {
        try {
            for (int i = 0; i < std::max(1, threads); ++i)
                workers.emplace_back([this] { worker(); });
        } catch (...) {
            { std::lock_guard<std::mutex> lock(mutex); stopping = true; }
            work.notify_all();
            for (auto& thread : workers) thread.join();
            throw;
        }
    }
    ~DependencyPool() {
        sync();
        { std::lock_guard<std::mutex> lock(mutex); stopping = true; }
        work.notify_all();
        for (auto& thread : workers) thread.join();
    }
    TaskID submit(IRunnable* runnable, int total, const std::vector<TaskID>& deps) {
        std::lock_guard<std::mutex> lock(mutex);
        TaskID id = static_cast<TaskID>(groups.size());
        std::vector<TaskID> unique = deps;
        std::sort(unique.begin(), unique.end());
        unique.erase(std::unique(unique.begin(), unique.end()), unique.end());
        for (TaskID dep : unique)
            if (dep < 0 || dep >= id) throw std::invalid_argument("invalid dependency");
        if (total < 0) throw std::invalid_argument("negative task count");
        groups.emplace_back(new Group(runnable, total));
        ++outstanding;
        for (TaskID dep : unique) {
            if (!groups[dep]->done) {
                groups[dep]->children.push_back(id);
                ++groups[id]->waiting;
            }
        }
        if (groups[id]->waiting == 0) activate(id);
        return id;
    }
    void sync() {
        std::unique_lock<std::mutex> lock(mutex);
        completed.wait(lock, [&] { return outstanding == 0; });
    }
};

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), pool_(new DependencyPool(num_threads)) {}
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() { delete pool_; }
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    return pool_->submit(runnable, num_total_tasks, deps);
}
void TaskSystemParallelThreadPoolSleeping::sync() { pool_->sync(); }
