#include "tasksys.h"
#include <atomic>
#include <vector>
#include <cstdio>
#include <cstdlib>
struct Task:IRunnable{
 int total;
 std::vector<std::atomic<int>> visits;
 Task(int n):total(n),visits(n){for(auto &v:visits)v=0;}
 void runTask(int id,int count){if(count!=total || id<0 || id>=total)std::abort();visits[id]++;}
 void check(){for(auto &v:visits)if(v.load()!=1)std::abort();}
};
int main(){
 int cases=0;
 for(int threads : {1,2,4,8}) for(int type=0;type<3;type++){
  ITaskSystem *s=type==0 ? static_cast<ITaskSystem*>(new TaskSystemParallelSpawn(threads)) :
   type==1 ? static_cast<ITaskSystem*>(new TaskSystemParallelThreadPoolSpinning(threads)) :
   static_cast<ITaskSystem*>(new TaskSystemParallelThreadPoolSleeping(threads));
  for(int round=0;round<8;round++)for(int n : {0,1,2,7,31,257}){
   Task task(n);s->run(&task,n);task.check();cases++;
  }
  delete s;
 }
 printf("PASS: %d exactly-once synchronous bulk launches and pool shutdowns\n",cases);
}
