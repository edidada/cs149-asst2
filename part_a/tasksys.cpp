#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <algorithm>
#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */


// One bulk launch is published under lock. Completion counts finished calls,
// not merely claimed IDs, so run() is a barrier for task output writes.
class BulkPool {
    std::vector<std::thread> workers;
    std::mutex mutex, caller;
    std::condition_variable work, done;
    bool stop=false, sleeping;
    IRunnable *job=nullptr;
    int count=0, next=0, finished=0;
    void worker(){
        std::unique_lock<std::mutex> lock(mutex);
        for(;;){
            if(sleeping) work.wait(lock,[this]{return stop || next<count;});
            if(stop)return;
            if(next>=count){
                lock.unlock(); std::this_thread::yield(); lock.lock();
                continue;
            }
            int id=next++, total=count;
            IRunnable *task=job;
            lock.unlock();
            task->runTask(id,total);
            lock.lock();
            if(++finished==count)done.notify_one();
        }
    }
public:
    BulkPool(int n,bool sleep):sleeping(sleep){
        for(int i=0;i<std::max(1,n);i++) workers.emplace_back([this]{worker();});
    }
    ~BulkPool(){
        {std::lock_guard<std::mutex> lock(mutex);stop=true;}
        work.notify_all();
        for(auto &thread:workers)thread.join();
    }
    void run(IRunnable *task,int n){
        std::lock_guard<std::mutex> serial(caller);
        if(n<=0)return;
        std::unique_lock<std::mutex> lock(mutex);
        job=task;count=n;next=finished=0;
        work.notify_all();
        if(sleeping) done.wait(lock,[this]{return finished==count;});
        else while(finished<count){
            lock.unlock();std::this_thread::yield();lock.lock();
        }
    }
};
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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    threads_=std::max(1,num_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    if(num_total_tasks<=0)return;
    std::atomic<int> next(0);
    std::vector<std::thread> workers;
    for(int i=0;i<std::min(threads_,num_total_tasks);i++)
        workers.emplace_back([&]{
            for(;;){int id=next.fetch_add(1);if(id>=num_total_tasks)return;
                runnable->runTask(id,num_total_tasks);}
        });
    for(auto &thread:workers)thread.join();

}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    pool_=new BulkPool(num_threads,false);
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() { delete pool_; }

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    pool_->run(runnable,num_total_tasks);

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    pool_=new BulkPool(num_threads,true);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() { delete pool_; }

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    pool_->run(runnable,num_total_tasks);

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
