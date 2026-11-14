#include "tasksys.h"
#include "tests.h"
#include <iostream>
int main(int argc, char** argv) {
    TestResults (*tests[])(ITaskSystem*) = {
        simpleTestSync,
        simpleTestAsync,
        pingPongEqualTest,
        pingPongUnequalTest,
        superLightTest,
        superSuperLightTest,
        recursiveFibonacciTest,
        mathOperationsInTightForLoopTest,
        mathOperationsInTightForLoopFewerTasksTest,
        mathOperationsInTightForLoopFanInTest,
        mathOperationsInTightForLoopReductionTreeTest,
        spinBetweenRunCallsTest,
        mandelbrotChunkedTest,
        pingPongEqualAsyncTest,
        pingPongUnequalAsyncTest,
        superLightAsyncTest,
        superSuperLightAsyncTest,
        recursiveFibonacciAsyncTest,
        mathOperationsInTightForLoopAsyncTest,
        mathOperationsInTightForLoopFewerTasksAsyncTest,
        mathOperationsInTightForLoopFanInAsyncTest,
        mathOperationsInTightForLoopReductionTreeAsyncTest,
        mandelbrotChunkedAsyncTest,
        spinBetweenRunCallsAsyncTest,
        simpleRunDepsTest,
        strictDiamondDepsTest,
        strictGraphDepsSmall,
        strictGraphDepsMedium,
        strictGraphDepsLarge,
    };
    int index = argc > 1 ? std::atoi(argv[1]) : -1;
    if (index < 0 || index >= int(sizeof(tests)/sizeof(tests[0]))) return 2;
    TaskSystemParallelThreadPoolSleeping system(4);
    TestResults result = tests[index](&system);
    std::cout << "original_case=" << index << " passed=" << result.passed
              << " seconds=" << result.time << std::endl;
    return result.passed ? 0 : 1;
}
