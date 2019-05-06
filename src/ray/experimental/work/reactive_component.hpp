#include <chrono>
#include <deque>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <vector>
// client_metrics must go
//#include "client_metrics.hpp"
#include "json.hpp"
#include "zmq_client.hpp"

#include <assert.h>

#include <mutex>
#include <thread>

using json = nlohmann::json;

using namespace std::chrono;
using namespace std;
using namespace clipper;

constexpr int MAX_IDLE_TIME_SECS = 15;

class ReactiveController {
 public:
  ReactiveController(
      std::vector<std::shared_ptr<ClipperReplica>> initial_replica_set, int actions_port,
      std::shared_ptr<ClientMetrics> metrics, long long slo_micros, bool take_reactive_action,
      std::map<std::string, std::tuple<int, float, float, float>> model_info, float max_load,
      std::unordered_map<float, int> arrival_curve_max_counts);
  void start();
  void stop();

  // returns replica address to send to
  std::shared_ptr<FrontendRPCClient> get_replica(std::string model, int query_id);

  void complete_query(int query_id);

 private:

  float check_arrival_curve_exceeded();
  bool check_add_replicas_max(
      zmq::socket_t& socket,
      std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas,
      float arrival_curve_max_lambda);
  bool check_remove_replicas(zmq::socket_t& socket,
      std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas);
  // bool start_optimizer(zmq::socket_t& socket, int run_number);
  // bool apply_optimizer_config(zmq::socket_t& socket, float cur_lambda);
  void run_monitor_thread();
  long long compute_p99_latency(std::vector<std::pair<long long, long long>> e2e_lats);

  static constexpr float slo_perc_upper_bound_ = 0.9;
  static constexpr float slo_perc_lower_bound_ = 0.4;
  // static constexpr int iter_sleep_time_ = 1;
  static constexpr int model_count_snapshot_window_ = 45;  // in seconds
  static constexpr int p99_collection_window_ = 10;        // in seconds
  static constexpr int optimizer_wait_time_secs_ = 15;      // in seconds
  static constexpr int arrival_times_lookback_delta_ = 2;  // in seconds
  static constexpr int arrival_times_lookahead_delta_ = 10;  // in seconds
  static constexpr int arrival_times_optimizer_rerun_window_ = 60;  // in seconds

  static constexpr int reactive2_wait_time_secs_ = 15;      // in seconds
  static constexpr int arrival_times_smoothing_window_ = 5;  // in seconds
  static constexpr int max_lambda_window_ = 30;  // in seconds
  static constexpr int max_arrival_curve_buckets_violated_ = 1;



  // Map from model name to corresponding Clipper replica in the case that we have the models
  // partitioned across different queues. This map doesn't need to be locked because it is
  // immutable.
  std::unordered_map<std::string, std::shared_ptr<ClipperReplica>> model_partition_map_;
  int actions_port_;
  std::thread monitor_thread_;
  std::atomic<bool> active_;
  std::atomic<int> total_snapshot_queries_;

  std::shared_ptr<ClientMetrics> metrics_;
  std::mutex arrival_time_mutex_;
  std::set<int> inflight_queries_;
  std::deque<std::chrono::system_clock::time_point> arrival_times_for_replica_add_;
  std::deque<std::chrono::system_clock::time_point> arrival_times_for_optimizer_rerun_;
  std::unordered_map<std::string, std::atomic<int>> model_counts_;
  std::unordered_map<std::string, float> last_model_scale_snapshot_;
  std::unordered_map<std::string, int> model_num_replicas_;
  std::unordered_map<std::string, float> model_throughputs_;
  std::unordered_map<std::string, float> model_max_loads_;
  bool take_reactive_action_;
  long long slo_micros_;
  float max_load_;
  std::unordered_map<float, int> arrival_curve_max_counts_;
  std::unordered_map<float, std::queue<std::chrono::system_clock::time_point>>
    current_arrival_curve_;
};


 private:
  std::string policy_;
  std::shared_ptr<FineGrainedReactiveComponentNoOpt> fg_reactive_no_opt_;
};

#endif
