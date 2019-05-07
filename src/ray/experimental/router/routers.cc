#include "routers.h"
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>
#include <deque>
#include <unordered_map>
#include <mutex>

std::mutex mtx;
using namespace std;

double fib(int n) {
    int i;
    double a=0.0, b=1.0, tmp;
    for (i=0; i<n; ++i) {
        tmp = a; a = a + b; b = tmp;
    }
    return a;
}

std::string run_monitor_thread(){
   return "monitor";
}

// can send just a map with float and count and keep it updated in pyx
float check_arrival_curve_exceeded(std::unordered_map<float, float> current_arrival_counts_,
    std::unordered_map<float, int> arrival_curve_max_counts_){
   // Returns number of buckets whose max count is exceeded

  // move this up
  std::unique_lock<std::mutex> arrival_time_lock(mtx);
  auto current_time = std::chrono::system_clock::now();
  int buckets_exceeded = 0;
  float max_lambda = 0.0;
  int max_lambda_bucket = -1;
  //check arrival curve here
  for (auto delta_t_entry: current_arrival_counts_) {
    int max_count = arrival_curve_max_counts_[delta_t_entry.first];
    int cur_count = delta_t_entry.second;
    if (cur_count > max_count) {
      std::cout << "Bucket " << delta_t_entry.first << " exceeded. Cur count: " << cur_count
        << ", max count: " << max_count << std::endl;
      buckets_exceeded += 1;
      float cur_lambda = float(cur_count) / float(delta_t_entry.first) * 1000.0;
      if (cur_lambda > max_lambda) {
        max_lambda = cur_lambda;
        max_lambda_bucket = delta_t_entry.first;
      }
    }
  }
  if (max_lambda > 0.0) {
    std::cout << "Setting max lambda to " << max_lambda << " from bucket "
      << max_lambda_bucket << std::endl;
  }
  return max_lambda;
}

/*bool check_add_replicas_max(std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas,
    float arrival_curve_max_lambda){
   return true;
}

bool check_remove_replicas(std::vector<std::pair<float,std::chrono::system_clock::time_point>>& prev_lambdas){
   return true;
}
*/
