# CS149 Assignment2 Part A

基线e2dfbb7b88144fd9e09dc4fdd9e16cd3afa367cf。三个同步实现：spawn每run创建线程，原子领取ID并join；spinning持久线程无条件变量等待工作/完成，yield轮询；sleeping持久线程使用predicate条件变量休眠/完成等待。任务回调在池锁外运行，finished仅在回调返回后增加，run以mutex建立结果可见性。析构停止/唤醒并join，连续run重置计数，非正数任务立即返回。PartA async接口按原要求不实现。

原Makefile/itasksys/测试不改。待远端原测试、独立exactly-once/屏障/连续run/析构及sanitizer验证，尚未验收。GitHub x86不替代规定AWS c7g ARM性能评分。PartB依赖图未实现。无本地编译执行，无新增Python源文件。
