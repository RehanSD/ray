#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <mutex>
#include <iostream>
#include <deque>
#include <unordered_map>
#include <set>
#include <queue>

using namespace std::chrono;
using namespace std;

double fib(int);

std::string run_monitor_thread();
float check_arrival_curve_exceeded(std::unordered_map<float, float> current_arrival_counts_,
    std::unordered_map<float, int> arrival_curve_max_counts_);
void register_arrival(std::string model, int query_id, std::unordered_map<float, float> current_arrival_counts_);

static constexpr int arrival_times_smoothing_window_ = 5;  // in seconds
static constexpr int arrival_times_optimizer_rerun_window_ = 60;  // in seconds

extern std::set<int> inflight_queries_;
extern std::atomic<int> total_snapshot_queries_;
extern std::unordered_map<std::string, std::atomic<int>> model_counts_;
extern std::deque<std::chrono::system_clock::time_point> arrival_times_for_replica_add_;
extern std::deque<std::chrono::system_clock::time_point> arrival_times_for_optimizer_rerun_;
extern std::unordered_map<float, std::queue<std::chrono::system_clock::time_point>>
    current_arrival_curve_;
//bool check_add_replicas_max(std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas,
//    float arrival_curve_max_lambda);
//bool check_remove_replicas(std::vector<std::pair<float,std::chrono::system_clock::time_point>>& prev_lambdas);