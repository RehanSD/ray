#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <mutex>
#include <iostream>
#include <deque>
#include <unordered_map>

using namespace std::chrono;
using namespace std;

double fib(int);

std::string run_monitor_thread();
float check_arrival_curve_exceeded(std::unordered_map<float, float> current_arrival_counts_,
    std::unordered_map<float, int> arrival_curve_max_counts_);
//bool check_add_replicas_max(std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas,
//    float arrival_curve_max_lambda);
//bool check_remove_replicas(std::vector<std::pair<float,std::chrono::system_clock::time_point>>& prev_lambdas);