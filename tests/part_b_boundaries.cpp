#include "tasksys.h"
#include <atomic>
#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <iostream>
#include <memory>
#include <vector>
struct Node : IRunnable {
    std::vector<Node*> parents;
    std::vector<int> values;
    std::atomic<int> calls{0};
    explicit Node(int count): values(count, 0) {}
    void runTask(int id, int total) override {
        if (total != int(values.size())) std::abort();
        for (Node* parent : parents)
            for (int value : parent->values) if (value != 1) std::abort();
        if (values[id] != 0) std::abort();
        values[id] = 1;
        ++calls;
    }
};
struct Overlap : IRunnable {
    std::mutex mutex;
    std::condition_variable cv;
    int arrived = 0;
    bool returned = false;
    void runTask(int, int) override {
        std::unique_lock<std::mutex> lock(mutex);
        ++arrived;
        cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds(5), [&] {
                return returned && arrived == 2;
            })) std::abort();
    }
};
int main() {
    for (int threads : {2, 4, 8}) {
        Overlap overlap;
        TaskSystemParallelThreadPoolSleeping system(threads);
        system.runAsyncWithDeps(&overlap, 1, {});
        system.runAsyncWithDeps(&overlap, 1, {});
        {
            std::lock_guard<std::mutex> lock(overlap.mutex);
            overlap.returned = true;
        }
        overlap.cv.notify_all();
        system.sync();
        if (overlap.arrived != 2) std::abort();
    }
    std::cout << "asynchronous independent-group overlap checks=3" << std::endl;
    int checked = 0;
    for (int threads : {1, 2, 4, 8}) {
        std::vector<std::unique_ptr<Node>> nodes;
        {
            TaskSystemParallelThreadPoolSleeping system(threads);
            std::vector<TaskID> ids;
            for (int i = 0; i < 400; ++i) {
                int count = i % 11 == 0 ? 0 : 1 + i % 37;
                nodes.emplace_back(new Node(count));
                std::vector<TaskID> deps;
                for (int distance : {1, 3, 17}) if (i >= distance) {
                    deps.push_back(ids[i-distance]);
                    nodes.back()->parents.push_back(nodes[i-distance].get());
                }
                if (!deps.empty()) deps.push_back(deps.front());
                TaskID id = system.runAsyncWithDeps(nodes.back().get(), count, deps);
                if (!ids.empty() && id <= ids.back()) std::abort();
                ids.push_back(id);
                if (i % 79 == 0) { system.sync(); system.sync(); }
            }
            // Destructor must drain the final submissions while runnables live.
        }
        for (auto& node : nodes) {
            if (node->calls != int(node->values.size())) std::abort();
            for (int value : node->values) if (value != 1) std::abort();
            ++checked;
        }
    }
    std::cout << "DAG groups checked=" << checked << std::endl;
}
