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

static constexpr int arrival_times_smoothing_window_ = 5;  // in seconds
static constexpr int arrival_times_optimizer_rerun_window_ = 60;  // in seconds
static constexpr int max_lambda_window_ = 30;  // in seconds
static constexpr int reactive2_wait_time_secs_ = 15;      // in seconds

// make sure all of these get declared
extern float max_load_;
extern std::set<int> inflight_queries_;
extern std::atomic<int> total_snapshot_queries_;
extern std::unordered_map<std::string, std::atomic<int>> model_counts_;
extern std::deque<std::chrono::system_clock::time_point> arrival_times_for_replica_add_;
extern std::deque<std::chrono::system_clock::time_point> arrival_times_for_optimizer_rerun_;
extern std::unordered_map<float, std::queue<std::chrono::system_clock::time_point>>
    current_arrival_curve_;
extern std::atomic<bool> active_;

// prev_lambdas should be cleared at stop
extern std::vector<std::pair<float, std::chrono::system_clock::time_point>> prev_lambdas;

double fib(int);

std::string run_monitor_thread(std::unordered_map<std::string, float> model_throughputs_,
  std::unordered_map<std::string, float> last_model_scale_snapshot_,
  std::unordered_map<std::string, int> model_num_replicas_,
  std::unordered_map<std::string, float> model_max_loads_,
  std::unordered_map<float, float> current_arrival_counts_,
  std::unordered_map<float, int> arrival_curve_max_counts);
float check_arrival_curve_exceeded(std::unordered_map<float, float> current_arrival_counts_,
    std::unordered_map<float, int> arrival_curve_max_counts_);
void register_arrival(std::string model, int query_id);
bool check_remove_replicas(std::unordered_map<std::string, float> model_throughputs_,
  std::unordered_map<std::string, float> last_model_scale_snapshot_,
  std::unordered_map<std::string, int> model_num_replicas_,
  std::unordered_map<std::string, float> model_max_loads_);
bool check_add_replicas_max(std::unordered_map<std::string, float> model_throughputs_,
  std::unordered_map<std::string, float> last_model_scale_snapshot_,
  std::unordered_map<std::string, int> model_num_replicas_,
  std::unordered_map<std::string, float> model_max_loads_,
   float arrival_curve_max_lambda);


