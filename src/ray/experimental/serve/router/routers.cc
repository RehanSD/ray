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

float max_load_;
std::set<int> inflight_queries_;
std::atomic<int> total_snapshot_queries_;
std::unordered_map<std::string, std::atomic<int>> model_counts_;
std::deque<std::chrono::system_clock::time_point> arrival_times_for_replica_add_;
std::deque<std::chrono::system_clock::time_point> arrival_times_for_optimizer_rerun_;
std::unordered_map<float, std::queue<std::chrono::system_clock::time_point>>
    current_arrival_curve_;
std::atomic<bool> active_;
std::vector<std::pair<float, std::chrono::system_clock::time_point>> prev_lambdas;

FineGrainedReactiveComponentNoOpt::FineGrainedReactiveComponentNoOpt(long long slo_micros, bool take_reactive_action,
    std::map<std::string, std::tuple<int, float, float, float>> model_info, float max_load,
    std::unordered_map<float, int> arrival_curve_max_counts)
    : actions_port_(actions_port),
      active_(true),
      total_snapshot_queries_(0),
      metrics_(metrics),
      take_reactive_action_(take_reactive_action),
      slo_micros_(slo_micros),
      max_load_(max_load),
      arrival_curve_max_counts_(arrival_curve_max_counts) {
  metrics_->set_max_collection_winodw(p99_collection_window_);

  for (auto model : model_info) {
    model_counts_.emplace(std::piecewise_construct, std::forward_as_tuple(model.first),
                          std::forward_as_tuple(0));
  }
  for (auto entry : model_info) {
    model_num_replicas_.emplace(entry.first, std::get<0>(entry.second));
    float scale = std::get<1>(entry.second);
    last_model_scale_snapshot_.emplace(entry.first, scale);
    /* std::cout << entry.first << " -> " << last_model_scale_snapshot_.at(entry.first) << "   " */
    /*           << scale << std::endl; */
    model_throughputs_.emplace(entry.first, std::get<2>(entry.second));
    model_max_loads_.emplace(entry.first, std::get<3>(entry.second));
    std::cout << "Max load: " << entry.first << " -> " << model_max_loads_[entry.first] << std::endl;
  }

  std::cout << "Arrival curve: ";
  for (auto entry: arrival_curve_max_counts_) {
    current_arrival_curve_.emplace(entry.first, std::queue<std::chrono::system_clock::time_point>{});

    std::cout << entry.first << ":" << entry.second << ", ";
  }
  std::cout << std::endl;
}

std::string run_monitor_thread(std::unordered_map<std::string, float> model_throughputs_,
  std::unordered_map<std::string, float> last_model_scale_snapshot_,
  std::unordered_map<std::string, int> model_num_replicas_,
  std::unordered_map<std::string, float> model_max_loads_,
  std::unordered_map<float, float> current_arrival_counts_,
  std::unordered_map<float, int> arrival_curve_max_counts){
     int iter_number = 0;

  // bool lambda_is_initialized = false;
  int iter_sleep_time_ms = 1000;
  auto last_add_replica_timestamp = std::chrono::system_clock::now();
  bool optimizer_running = false;
  int num_optimizer_runs = 0;
  int last_iter_time_ms = 0.0;
  bool reactive_comp_activated = false;

  /* std::this_thread::sleep_for(std::chrono::seconds(30)); */

  while (active_) {
    iter_number += 1;
    int sleep_time = iter_sleep_time_ms - last_iter_time_ms;
    if (sleep_time > 100) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
    }

    auto iter_start = std::chrono::system_clock::now();

    if (total_snapshot_queries_ > 5000) {
      /* std::cout << "Updating last_model_scale_snapshot_ with " << total_snapshot_queries_ << " queries" << std::endl; */
      for (auto& entry : model_counts_) {
        last_model_scale_snapshot_[entry.first] =
            (float)(entry.second) / (float)(total_snapshot_queries_);
        /* std::cout << entry.first << " -> " << last_model_scale_snapshot_[entry.first] << std::endl; */
        entry.second = 0;
      }
      total_snapshot_queries_ = 0;
    }

    float arrival_curve_max_lambda = check_arrival_curve_exceeded(current_arrival_counts_,arrival_curve_max_counts);
    if (arrival_curve_max_lambda > 0.0 && iter_number > 10) {
      // First check if we need to add any replicas
      bool replicas_added = check_add_replicas_max(model_throughputs_, last_model_scale_snapshot_,
         model_num_replicas_, model_max_loads_, arrival_curve_max_lambda);
      if (replicas_added) {
        last_add_replica_timestamp = std::chrono::system_clock::now();
        optimizer_running = false;  // Throw away the current optimizer run if it exists
      }
    } else {
      // TODO: Should this be in the else statement or outside?
      int secs_since_last_replica_add =
          duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() -
                                              last_add_replica_timestamp)
              .count();
      if (secs_since_last_replica_add >= reactive2_wait_time_secs_ && iter_number > 10) {
        bool replicas_removed = check_remove_replicas(model_throughputs_, last_model_scale_snapshot_,
           model_num_replicas_, model_max_loads_);
        if (replicas_removed) {
        }
      }
    }

  auto iter_end = std::chrono::system_clock::now();
  last_iter_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      iter_end - iter_start).count();
  }
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

void register_arrival(std::string model, int query_id) {
  model_counts_[model] += 1;
  {  // Acquire arrival_time_lock
    std::unique_lock<std::mutex> arrival_time_lock(mtx);
    auto query_find = inflight_queries_.find(query_id);
    if (query_find == inflight_queries_.end()) {
      inflight_queries_.insert(query_id);
      total_snapshot_queries_ += 1;  // Tracks total number of arrived queries in current snapshot
      auto current_time = std::chrono::system_clock::now();
      arrival_times_for_replica_add_.push_back(current_time);
      arrival_times_for_optimizer_rerun_.push_back(current_time);
      while (std::chrono::duration_cast<std::chrono::seconds>(
                 current_time - *arrival_times_for_replica_add_.begin())
                 .count() > arrival_times_smoothing_window_) {
        arrival_times_for_replica_add_.pop_front();
      }
      while (std::chrono::duration_cast<std::chrono::seconds>(
                 current_time - *arrival_times_for_optimizer_rerun_.begin())
                 .count() > arrival_times_optimizer_rerun_window_) {
        arrival_times_for_optimizer_rerun_.pop_front();
      }

      for (auto& delta_t_entry: current_arrival_curve_) {
        // Add current time to each bucket
        delta_t_entry.second.push(current_time);
        // Remove expired timestamps
        auto cur_delta = std::chrono::duration_cast<std::chrono::milliseconds>(
              current_time - delta_t_entry.second.front()).count();
        while (delta_t_entry.second.size() > 0 && cur_delta > delta_t_entry.first) {
          delta_t_entry.second.pop();
          cur_delta = std::chrono::duration_cast<std::chrono::milliseconds>(
                        current_time - delta_t_entry.second.front()).count();
        }
      }
    }
  }// Release arrival_time_lock
}


bool check_add_replicas_max(std::unordered_map<std::string, float> model_throughputs_,
  std::unordered_map<std::string, float> last_model_scale_snapshot_,
  std::unordered_map<std::string, int> model_num_replicas_,
  std::unordered_map<std::string, float> model_max_loads_,
  float arrival_curve_max_lambda){
     float cur_lambda;
  {
    std::unique_lock<std::mutex> arrival_time_lock(mtx);
    float arrival_times_window =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            *(arrival_times_for_replica_add_.end() - 1) - *arrival_times_for_replica_add_.begin())
            .count() / 1000.0;
    if (arrival_times_for_replica_add_.size() < 100) {
      return false;
    }
    cur_lambda = (float)(arrival_times_for_replica_add_.size()) / arrival_times_window;
  }
  prev_lambdas.emplace_back(cur_lambda, std::chrono::system_clock::now());

  float max_lambda = arrival_curve_max_lambda;

  if (max_lambda == -1.0) {
    return false;
  }

  // Round up
  max_lambda = std::ceil(max_lambda);
  std::cout << "Max lambda for adding replicas: " << max_lambda << std::endl;
  std::unordered_map<std::string, int> model_extra_replicas_needed;
  bool additional_reps_needed = false;
  for (auto& entry : model_throughputs_) {
    float single_replica_scaled_throughput =
        model_throughputs_[entry.first] / last_model_scale_snapshot_[entry.first];
    float max_scaled_throughput =
        std::round(single_replica_scaled_throughput * (float)(model_num_replicas_[entry.first]));
    std::cout << "Model " << entry.first
              << ": model_throughput=" << model_throughputs_[entry.first]
              << " num_replicas=" << model_num_replicas_[entry.first]
              << " scale_factor=" << last_model_scale_snapshot_[entry.first] << std::endl;
    /* float k_hat_m = max_lambda / (single_replica_scaled_throughput * model_max_loads_[entry.first]); */
    float k_hat_m = max_lambda / (single_replica_scaled_throughput * max_load_);
    int extra_replicas_needed = int(std::ceil(k_hat_m)) - model_num_replicas_[entry.first];
    std::cout << "Extra replicas needed " << entry.first << ": " << extra_replicas_needed << std::endl;
    if (extra_replicas_needed <= 0) {
      extra_replicas_needed = 0;
    } else {
      additional_reps_needed = true;
    }
    model_extra_replicas_needed[entry.first] = extra_replicas_needed;
  }

  return additional_reps_needed;
}

bool check_remove_replicas(std::unordered_map<std::string, float> model_throughputs_,
  std::unordered_map<std::string, float> last_model_scale_snapshot_,
  std::unordered_map<std::string, int> model_num_replicas_,
  std::unordered_map<std::string, float> model_max_loads_){

  // Compute max lambda over max_lambda_window_
  float max_lambda = -1.0;
  for (auto lambda_entry = prev_lambdas.rbegin(); lambda_entry != prev_lambdas.rend(); ++lambda_entry) {
    float time_delta = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now() - lambda_entry->second)
                             .count();
    if (time_delta >= max_lambda_window_) {
      break;
    } else {
      if (lambda_entry->first > max_lambda) {
        max_lambda = lambda_entry->first;
      }
    }
  }
  if (max_lambda == -1.0) {
    return false;
  }

  // Round up
  max_lambda = std::ceil(max_lambda);
  std::cout << "Max lambda for removing replicas: " << max_lambda << std::endl;

  std::unordered_map<std::string, int> model_replicas_to_remove;
  bool change_needed = false;
  for (auto& entry : model_throughputs_) {
    float single_replica_scaled_throughput =
        model_throughputs_[entry.first] / last_model_scale_snapshot_[entry.first];
    float max_scaled_throughput =
        std::round(single_replica_scaled_throughput * (float)(model_num_replicas_[entry.first]));

    float k_hat_m = max_lambda / (single_replica_scaled_throughput * model_max_loads_[entry.first]);
    /* float k_hat_m = max_lambda / (single_replica_scaled_throughput * max_load_); */
    int replicas_to_remove = model_num_replicas_[entry.first] - int(std::ceil(k_hat_m));
    if (replicas_to_remove <= 0) {
      replicas_to_remove = 0;
    } else {
      std::cout << "Replicas to remove " << entry.first << ": " << replicas_to_remove <<
        ", k_hat_m " << k_hat_m << std::endl;
      change_needed = true;

    }
    model_replicas_to_remove[entry.first] = replicas_to_remove;
  }
  return change_needed;
}

