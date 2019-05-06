#include <assert.h>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <zmq.hpp>

// #include <netinet/in.h>
// #include <sys/socket.h>
#include "json.hpp"
#include "reactive_component.hpp"

using json = nlohmann::json;

using namespace std::chrono;
// using namespace std;

void signal_close_connection(zmq::socket_t& socket) {
  std::cout << "Sending shutdown request" << std::endl;
  json j;
  j["action"] = "shutdown";
  std::string s = j.dump() + "\n";
  socket.send(s.data(), s.size());
}

FineGrainedReactiveComponentNoOpt::FineGrainedReactiveComponentNoOpt(
    std::vector<std::shared_ptr<ClipperReplica>> initial_replica_set, int actions_port,
    std::shared_ptr<ClientMetrics> metrics, long long slo_micros, bool take_reactive_action,
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
  for (auto rep : initial_replica_set) {
    for (auto model : rep->models_) {
      // Ensure that each model is registered with exactly one replica
      auto find = model_partition_map_.find(model);
      if (find == model_partition_map_.end()) {
        model_partition_map_.emplace(model, rep);
      } else {
        throw runtime_error("Error: model " + model + " registered with multiple Clipper replicas");
      }
    }
  }
  for (auto model : model_partition_map_) {
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

void FineGrainedReactiveComponentNoOpt::start() {
  if (take_reactive_action_) {
    monitor_thread_ = std::thread([this]() { run_monitor_thread(); });
  }
}

void FineGrainedReactiveComponentNoOpt::stop() {
  active_ = false;
  for (auto entry : model_partition_map_) {
    if (entry.second->active_) {
      entry.second->active_ = false;
      entry.second->zmq_client_->stop();
    }
  }
  if (take_reactive_action_) {
    monitor_thread_.join();
    std::cout << "Reactive monitoring terminated" << std::endl;
  }
}

// returns replica address to send to
std::shared_ptr<FrontendRPCClient> FineGrainedReactiveComponentNoOpt::get_replica(std::string model,
                                                                                  int query_id) {
  model_counts_[model] += 1;
  {  // Acquire arrival_time_lock
    std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
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
  }  // Release arrival_time_lock

  auto find = model_partition_map_.find(model);
  if (find != model_partition_map_.end()) {
    return find->second->zmq_client_;
  } else {
    throw runtime_error("Error: model " + model + " not found in partion map");
  }
}

float FineGrainedReactiveComponentNoOpt::check_arrival_curve_exceeded() {
  // Returns number of buckets whose max count is exceeded
  std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
  auto current_time = std::chrono::system_clock::now();
  int buckets_exceeded = 0;
  float max_lambda = 0.0;
  int max_lambda_bucket = -1;
  for (auto delta_t_entry: current_arrival_curve_) {
    int max_count = arrival_curve_max_counts_[delta_t_entry.first];
    int cur_count = delta_t_entry.second.size();
    if (cur_count > max_count) {
      std::cout << "Bucket " << delta_t_entry.first << " exceeded. Cur count: " << cur_count
        << ", max count: " << max_count << std::endl;
      buckets_exceeded += 1;
      float cur_lambda = float(cur_count) / float(delta_t_entry.first) * 1000.0;
      if (cur_lambda > max_lambda) {
        max_lambda = cur_lambda;
        max_lambda_bucket = delta_t_entry.first;
      }
    } else {
      /* std::cout << "Bucket " << delta_t_entry.first << " cur count: " << cur_count */
      /*   << ", max count: " << max_count << std::endl; */
    }
  }
  if (max_lambda > 0.0) {
    std::cout << "Setting max lambda to " << max_lambda << " from bucket "
      << max_lambda_bucket << std::endl;
  }
  // TODO: how to remove replicas?
  return max_lambda;
}

void FineGrainedReactiveComponentNoOpt::complete_query(int query_id) {
  std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
  inflight_queries_.erase(query_id);
}

// Driver function to sort the vector elements
// by second element of pairs
bool sortbysec(const pair<long long, long long>& a, const pair<long long, long long>& b) {
  return (a.second < b.second);
}

bool FineGrainedReactiveComponentNoOpt::check_add_replicas_predict(
    zmq::socket_t& socket,
    std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas) {
  float cur_lambda;
  {
    std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
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

  float prev_lambda = -1.0;
  float tc_minus_tprev;
  for (auto lambda_entry = prev_lambdas.rbegin(); lambda_entry != prev_lambdas.rend(); ++lambda_entry) {
    tc_minus_tprev = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now() - lambda_entry->second)
                             .count();
    if (tc_minus_tprev >= arrival_times_lookback_delta_) {
      prev_lambda = lambda_entry->first;
      break;
    }
  }
  if (prev_lambda == -1.0) {
    return false;
  } 

  // Linearly extrapolate what lambda will be in arrival_times_smoothing_window_secs;
  // The time associated with prev_lambda (tc_minus_tprev secs ago) is x=0
  float slope = (cur_lambda - prev_lambda) / tc_minus_tprev;
  float y_offset = prev_lambda;
  float delta_t_secs = tc_minus_tprev + arrival_times_lookahead_delta_;

  float projected_lambda = slope * delta_t_secs + y_offset;
  std::cout << "Prev lambda: " << prev_lambda << ", Cur lambda: " << cur_lambda
            << ", Projected lambda: " << projected_lambda << ", Slope: " << slope << std::endl;

  // Compute max lambda over max_lambda_window_

  // NOTE: We are ROUNDING HERE
  if (std::round(projected_lambda) > std::round(cur_lambda)) {
    // iterate through models to find the model with the lowest effective throughput, as well as
    // the number of extra replicas it needs
    std::unordered_map<std::string, int> model_extra_replicas_needed;
    bool additional_reps_needed = false;
    for (auto& entry : model_partition_map_) {
      float single_replica_scaled_throughput =
          model_throughputs_[entry.first] / last_model_scale_snapshot_[entry.first];
      float max_scaled_throughput =
          std::round(single_replica_scaled_throughput * (float)(model_num_replicas_[entry.first]));
      std::cout << "Model " << entry.first
                << ": model_throughput=" << model_throughputs_[entry.first]
                << " num_replicas=" << model_num_replicas_[entry.first]
                << " scale_factor=" << last_model_scale_snapshot_[entry.first] << std::endl;
      float k_hat_m = projected_lambda / (single_replica_scaled_throughput * model_max_loads_[entry.first]);
      /* float k_hat_m = projected_lambda / (single_replica_scaled_throughput * max_load_); */
      int extra_replicas_needed = int(std::ceil(k_hat_m)) - model_num_replicas_[entry.first];
      std::cout << "Extra replicas needed " << entry.first << ": " << extra_replicas_needed << std::endl;
      if (extra_replicas_needed <= 0) {
        extra_replicas_needed = 0;
      } else {
        additional_reps_needed = true;
      }
      model_extra_replicas_needed[entry.first] = extra_replicas_needed;
    }

    if (additional_reps_needed) {
      auto add_rep_start = std::chrono::system_clock::now();
      json j;
      j["action"] = "add";
      j["models"] = model_extra_replicas_needed;
      std::string s = j.dump() + "\n";
      socket.send(s.data(), s.size());

      zmq::message_t msg_response;
      socket.recv(&msg_response);
      std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
      json json_response = json::parse(response_str);
      if (json_response["result"] != "success") {
        throw runtime_error("Error yweiurhiuhewei");
      }
      for (auto entry : model_extra_replicas_needed) {
        model_num_replicas_[entry.first] += entry.second;
      }
      auto add_rep_end = std::chrono::system_clock::now();
      float add_rep_dur = std::chrono::duration_cast<std::chrono::milliseconds>(
          add_rep_end - add_rep_start).count() / 1000.0;
      /* std::cout << "ADD REP DURATION: " << add_rep_dur << " seconds" << std::endl; */
      return true;
    }
  }
  return false;
}

bool FineGrainedReactiveComponentNoOpt::check_add_replicas_max(
    zmq::socket_t& socket,
    std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas,
    float arrival_curve_max_lambda) {
  float cur_lambda;
  {
    std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
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

  /* // Compute max lambda over max_lambda_window_ */
  /* #<{(| float max_lambda = -1.0; |)}># */
  /* float max_lambda = cur_lambda; */
  /* for (auto lambda_entry = prev_lambdas.rbegin(); lambda_entry != prev_lambdas.rend(); ++lambda_entry) { */
  /*   float time_delta = std::chrono::duration_cast<std::chrono::seconds>( */
  /*                            std::chrono::system_clock::now() - lambda_entry->second) */
  /*                            .count(); */
  /*   if (time_delta >= max_lambda_window_) { */
  /*     break; */
  /*   } else { */
  /*     if (lambda_entry->first > max_lambda) { */
  /*       max_lambda = lambda_entry->first; */
  /*     } */
  /*   } */
  /* } */

  float max_lambda = arrival_curve_max_lambda;

  /* if (arrival_curve_max_lambda > max_lambda) { */
  /*   std::cout << "Provisioning for arrival curve max lambda: " << arrival_curve_max_lambda */
  /*     << " over smoothed max lambda: " << max_lambda << std::endl; */
  /*   max_lambda = arrival_curve_max_lambda; */
  /* } else { */
  /*   std::cout << "Provisioning for smoothed max lambda: " << max_lambda */
  /*     << " over arrival curve max lambda: " << arrival_curve_max_lambda << std::endl; */
  /* } */

  /* if (arrival_curve_max_lambda > cur_lambda) { */
  /*   prev_lambdas.emplace_back(arrival_curve_max_lambda, std::chrono::system_clock::now()); */
  /* } else { */
  // prev_lambdas.emplace_back(cur_lambda, std::chrono::system_clock::now());
  // }

  if (max_lambda == -1.0) {
    return false;
  } 


  // Round up
  max_lambda = std::ceil(max_lambda);
  std::cout << "Max lambda for adding replicas: " << max_lambda << std::endl;
  std::unordered_map<std::string, int> model_extra_replicas_needed;
  bool additional_reps_needed = false;
  for (auto& entry : model_partition_map_) {
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

  if (additional_reps_needed) {
    auto add_rep_start = std::chrono::system_clock::now();
    json j;
    j["action"] = "add";
    j["models"] = model_extra_replicas_needed;
    std::string s = j.dump() + "\n";
    socket.send(s.data(), s.size());

    zmq::message_t msg_response;
    socket.recv(&msg_response);
    std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
    json json_response = json::parse(response_str);
    if (json_response["result"] != "success") {
      throw runtime_error("Error yweiurhiuhewei");
    }
    for (auto entry : model_extra_replicas_needed) {
      model_num_replicas_[entry.first] += entry.second;
    }
    auto add_rep_end = std::chrono::system_clock::now();
    float add_rep_dur = std::chrono::duration_cast<std::chrono::milliseconds>(
        add_rep_end - add_rep_start).count() / 1000.0;
    return true;
  }
  return false;
}


bool FineGrainedReactiveComponentNoOpt::check_remove_replicas(
    zmq::socket_t& socket,
    std::vector<std::pair<float, std::chrono::system_clock::time_point>>& prev_lambdas) {

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
  for (auto& entry : model_partition_map_) {
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

  if (change_needed) {
    auto remove_rep_start = std::chrono::system_clock::now();
    json j;
    j["action"] = "remove";
    j["models"] = model_replicas_to_remove;
    std::string s = j.dump() + "\n";
    socket.send(s.data(), s.size());

    zmq::message_t msg_response;
    socket.recv(&msg_response);
    std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
    json json_response = json::parse(response_str);
    if (json_response["result"] != "success") {
      throw runtime_error("Error kjhkjewrhrjkwehrjkwehrjk");
    }
    for (auto entry : model_replicas_to_remove) {
      model_num_replicas_[entry.first] -= entry.second;
    }
    auto remove_rep_end = std::chrono::system_clock::now();
    float remove_rep_dur = std::chrono::duration_cast<std::chrono::milliseconds>(
        remove_rep_end - remove_rep_start).count() / 1000.0;
    return true;
  }
  return false;
}


// bool FineGrainedReactiveComponentNoOpt::start_optimizer(zmq::socket_t& socket, int run_number) {
//   std::vector<double> deltas;
//   {  // Acquire arrival_time_lock
//     // First collect arrival trace workload for optimizer and write to delta file
//     std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
//
//     float arrival_times_window =
//         std::chrono::duration_cast<std::chrono::milliseconds>(
//             *(arrival_times_for_optimizer_rerun_.end() - 1) - *arrival_times_for_optimizer_rerun_.begin())
//             .count() / 1000.0;
//
//     // Don't re-run if we have less than 45 seconds of arrival data
//     if (arrival_times_window < 45) {
//       return false;
//     }
//     for (auto it_first = arrival_times_for_optimizer_rerun_.begin(),
//               it_second = arrival_times_for_optimizer_rerun_.begin() + 1;
//          it_second != arrival_times_for_optimizer_rerun_.end(); it_first++, it_second++) {
//       long delta_micros =
//           std::chrono::duration_cast<std::chrono::microseconds>(*it_second - *it_first).count();
//       double delta_millis = delta_micros / 1000.0;
//       deltas.emplace_back(delta_millis);
//     }
//   }  // Release arrival_time_lock
//   #<{(| std::cout << "Starting optimizer run " << run_number << std::endl; |)}>#
//
//   // Write the file outside the lock
//   std::string deltas_fname =
//       "/tmp/inferline/reactive_fg_deltas" + std::to_string(run_number) + ".deltas";
//   std::ofstream deltas_file;
//   deltas_file.open(deltas_fname, std::ofstream::out | std::ofstream::trunc);
//   for (int i = 0; i < deltas.size(); ++i) {
//     deltas_file << std::to_string(deltas[i]) << std::endl;
//   }
//   deltas_file.close();
//   // send a json line (with \n) to the named pipe
//   json j;
//   j["action"] = "rerun_optimizer";
//   j["arrival_deltas_path"] = deltas_fname;
//   j["scale_factors"] = last_model_scale_snapshot_;
//   std::string s = j.dump() + "\n";
//   socket.send(s.data(), s.size());
//
//   zmq::message_t msg_response;
//   socket.recv(&msg_response);
//   std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
//   json json_response = json::parse(response_str);
//   if (json_response["result"] != "success") {
//     throw runtime_error("Error requesting optimizer run in FG reactive component.");
//   }
//   return true;
// }

// bool FineGrainedReactiveComponentNoOpt::apply_optimizer_config(zmq::socket_t& socket, float cur_lambda) {
//   json j;
//   j["action"] = "apply_changes";
//   j["cur_lambda"] = std::round(cur_lambda);
//   std::string s = j.dump() + "\n";
//   socket.send(s.data(), s.size());
//
//   zmq::message_t msg_response;
//   socket.recv(&msg_response);
//   std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
//   json json_response = json::parse(response_str);
//
//   if (json_response["result"] == "still_running") {
//     return false;
//   } else if (json_response["result"] == "no_change") {
//     std::cout << "Optimizer detected no change" << std::endl;
//     return true;
//   } else if (json_response["result"] == "changes_applied") {
//     max_load_ = json_response["max_load"];
//     auto model_info = json_response["model_info"];
//     for (auto entry : model_info) {
//       auto model_name = entry["model_name"];
//       model_num_replicas_[model_name] = entry["num_replicas"];
//       model_throughputs_[model_name] = entry["profiled_thru"];
//       model_max_loads_[model_name] = entry["max_load"];
//     }
//     std::cout << "Applied optimizer changes" << std::endl;
//     return true;
//   } else if (json_response["result"] == "no_optimizer_run") {
//     throw std::logic_error("Requested to apply optimizer changes but no optimizer run");
//   } else {
//     throw std::runtime_error("Received unrecognized response from action server: " + response_str);
//   }
// }

long long FineGrainedReactiveComponentNoOpt::compute_p99_latency(
    std::vector<std::pair<long long, long long>> e2e_lats) {
  /* std::cout << "Calculating p99 latency from " << e2e_lats.size() << " observations" << std::endl; */

  // Sort lats in ascending order
  std::sort(e2e_lats.begin(), e2e_lats.end(), sortbysec);

  // Get the p99 latency of the observation window, assumes that the observation window is
  // going to be large because rerunning the optimizer algorithm takes 31 - 45 seconds
  int p99_idx = int(e2e_lats.size() * 99.0 / 100.0) - 1;

  long long p99_lat_micros = e2e_lats[p99_idx].second;
  return p99_lat_micros;
}

void FineGrainedReactiveComponentNoOpt::run_monitor_thread() {
  int iter_number = 0;
  zmq::context_t context(1);
  // Use socket type ZMQ_REQ for the client in a request-response pair
  // connection
  zmq::socket_t socket(context, ZMQ_REQ);
  socket.connect("tcp://127.0.0.1:" + std::to_string(actions_port_));
  // system_clock::time_point last_arrival_rate_check;
  // std::this_thread::sleep_for(std::chrono::seconds(10));
  std::vector<std::pair<float, std::chrono::system_clock::time_point>> prev_lambdas;
  /* float prev_lambda = -1; */
  /* auto prev_lambda_timestamp = std::chrono::system_clock::now(); */
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
    /* std::vector<std::pair<long long, long long>> e2e_lats = metrics_->get_latencies_in_window(); */
    /* if (e2e_lats.size() < 10) { */
    /*   printf("Reactive component skipping to wait for initialization..\n"); */
    /*   continue; */
    /* } else { */
    /*   long long p99_lat_micros = compute_p99_latency(std::move(e2e_lats)); */
    /* } */

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

    float arrival_curve_max_lambda = check_arrival_curve_exceeded();
    if (arrival_curve_max_lambda > 0.0 && iter_number > 10) {
      // First check if we need to add any replicas
      bool replicas_added = check_add_replicas_max(socket, prev_lambdas,
          arrival_curve_max_lambda);
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
        bool replicas_removed = check_remove_replicas(socket, prev_lambdas);
        if (replicas_removed) {
        }
      }
    }


  /*   // First check if reactive component has been activated */
  /*   if (!reactive_comp_activated) { */
  /*     int buckets_exceeded = check_arrival_curve_exceeded(); */
  /*     if (buckets_exceeded > max_arrival_curve_buckets_violated_) { */
  /*       std::cout << "Arrival process violated (" << buckets_exceeded << " buckets exceeded). Activating reactive component" << std::endl; */
  /*       if (iter_number > 15) { */
  /*         reactive_comp_activated = true; */
  /*       } else { */
  /*         std::cout << "Still in warmup period, not triggering reactive component yet" << std::endl; */
  /*       } */
  /*     } */
  /*   } */
  /*  */
  /*   if (reactive_comp_activated) { */
  /*     // First check if we need to add any replicas */
  /*     bool replicas_added = check_add_replicas_max(socket, prev_lambdas, arrival_curve_max_lambda); */
  /*     if (replicas_added) { */
  /*       last_add_replica_timestamp = std::chrono::system_clock::now(); */
  /*       optimizer_running = false;  // Throw away the current optimizer run if it exists */
  /*     } else { */
  /*       int secs_since_last_replica_add = */
  /*           duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - */
  /*                                               last_add_replica_timestamp) */
  /*               .count(); */
  /*       if (secs_since_last_replica_add >= reactive2_wait_time_secs_) { */
  /*         bool replicas_removed = check_remove_replicas(socket, prev_lambdas); */
  /*         if (replicas_removed) { */
  /*  */
  /*         } */
  /*       } */
  /*     } */
  /* } */

  auto iter_end = std::chrono::system_clock::now();
  last_iter_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      iter_end - iter_start).count();
  }
  signal_close_connection(socket);
  socket.close();
}

FineGrainedReactiveComponent::FineGrainedReactiveComponent(
    std::vector<std::shared_ptr<ClipperReplica>> initial_replica_set, int actions_port,
    std::shared_ptr<ClientMetrics> metrics, long long slo_micros, bool take_reactive_action)
    : actions_port_(actions_port),
      active_(true),
      metrics_(metrics),
      take_reactive_action_(take_reactive_action),
      slo_micros_(slo_micros) {
  for (auto rep : initial_replica_set) {
    for (auto model : rep->models_) {
      // Ensure that each model is registered with exactly one replica
      auto find = model_partition_map_.find(model);
      if (find == model_partition_map_.end()) {
        model_partition_map_.emplace(model, rep);
      } else {
        throw runtime_error("Error: model " + model + " registered with multiple Clipper replicas");
      }
    }
  }
  for (auto model : model_partition_map_) {
    model_counts_.emplace(std::piecewise_construct, std::forward_as_tuple(model.first),
                          std::forward_as_tuple(0));
    last_model_count_snapshot_.emplace(model.first, 0);
  }
}

void FineGrainedReactiveComponent::start() {
  if (take_reactive_action_) {
    monitor_thread_ = std::thread([this]() { run_monitor_thread(); });
  }
}

void FineGrainedReactiveComponent::stop() {
  active_ = false;
  for (auto entry : model_partition_map_) {
    if (entry.second->active_) {
      entry.second->active_ = false;
      entry.second->zmq_client_->stop();
    }
  }
  if (take_reactive_action_) {
    monitor_thread_.join();
    std::cout << "Reactive monitoring terminated" << std::endl;
  }
}

// returns replica address to send to
std::shared_ptr<FrontendRPCClient> FineGrainedReactiveComponent::get_replica(std::string model,
                                                                             int query_id) {
  model_counts_[model] += 1;
  {  // Acquire arrival_time_lock
    std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
    auto query_find = inflight_queries_.find(query_id);
    if (query_find == inflight_queries_.end()) {
      inflight_queries_.insert(query_id);
      arrival_times_.push_back(std::chrono::system_clock::now());
      while (arrival_times_.size() > max_collected_arrival_times_) {
        arrival_times_.pop_front();
      }
    }
  }  // Release arrival_time_lock

  auto find = model_partition_map_.find(model);
  if (find != model_partition_map_.end()) {
    return find->second->zmq_client_;
  } else {
    throw runtime_error("Error: model " + model + " not found in partion map");
  }
}

void FineGrainedReactiveComponent::complete_query(int query_id) {
  std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
  inflight_queries_.erase(query_id);
}

void FineGrainedReactiveComponent::rerun_optimizer_algorithm(zmq::socket_t& socket,
                                                             long long p99_lat_micros) {
  std::vector<double> deltas;
  {  // Acquire arrival_time_lock
    // First collect arrival trace workload for optimizer and write to delta file
    std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
    // Don't re-run if we've recorded less than 100 queries
    if (arrival_times_.size() < 500) {
      return;
    }
    for (auto it_first = arrival_times_.begin(), it_second = arrival_times_.begin() + 1;
         it_second != arrival_times_.end(); it_first++, it_second++) {
      long delta_micros =
          std::chrono::duration_cast<std::chrono::microseconds>(*it_second - *it_first).count();
      double delta_millis = delta_micros / 1000.0;
      deltas.emplace_back(delta_millis);
    }

  }  // Release arrival_time_lock

  // Write the file outside the lock
  std::string deltas_fname = "/tmp/inferline/reactive_fg_deltas.deltas";
  std::ofstream deltas_file;
  deltas_file.open(deltas_fname, std::ofstream::out | std::ofstream::trunc);
  for (int i = 0; i < deltas.size(); ++i) {
    deltas_file << std::to_string(deltas[i]) << std::endl;
  }
  deltas_file.close();
  // send a json line (with \n) to the named pipe
  json j;
  j["action"] = "rerun_optimizer";
  j["arrival_deltas_path"] = deltas_fname;
  j["model_counts"] = last_model_count_snapshot_;
  j["p99_lat_micros"] = p99_lat_micros;
  std::string s = j.dump() + "\n";
  socket.send(s.data(), s.size());

  zmq::message_t msg_response;
  socket.recv(&msg_response);
  std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
  json json_response = json::parse(response_str);
  if (json_response["result"] != "success") {
    throw runtime_error("Error yweiurhiuhewei");
  }

  // // Give the pipeline some time to recover
  // std::this_thread::sleep_for(std::chrono::seconds(action_recovery_time_));
}

void FineGrainedReactiveComponent::run_monitor_thread() {
  int iter_number = 0;
  zmq::context_t context(1);
  // Use socket type ZMQ_REQ for the client in a request-response pair
  // connection
  zmq::socket_t socket(context, ZMQ_REQ);
  socket.connect("tcp://127.0.0.1:" + std::to_string(actions_port_));
  system_clock::time_point last_arrival_rate_check;
  // std::this_thread::sleep_for(std::chrono::seconds(10));

  while (active_) {
    // std::this_thread::sleep_for(std::chrono::seconds(iter_sleep_time_));
    std::vector<long long> e2e_lats = metrics_->get_latencies();
    if (e2e_lats.size() < 10) {
      // This if-statement should only get entered while we're waiting to initialize.
      // We sleep to avoid spinning.
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    // Sort lats in ascending order
    std::sort(e2e_lats.begin(), e2e_lats.end());

    // Get the p99 latency of the observation window
    int p99_idx = int(e2e_lats.size() * 99.0 / 100.0) - 1;

    long long p99_lat_micros = e2e_lats[p99_idx];
    std::cout << "Calculating p99 latency from " << e2e_lats.size() << " observations" << std::endl;

    // Check whether to update the model counts snapshot to re-estimate the scale factors
    bool update_snapshot = false;
    for (const auto& entry : model_counts_) {
      if (entry.second > max_collected_arrival_times_) {
        update_snapshot = true;
        break;
      }
    }
    if (update_snapshot) {
      for (auto& entry : model_counts_) {
        // fetch the old count and reset to 0
        int cur_count = entry.second.exchange(0);
        last_model_count_snapshot_[entry.first] = cur_count;
      }
    }

    // Triggering conditions for re-running the optimizer
    float upper_bound = slo_perc_upper_bound_ * slo_micros_;
    float lower_bound = slo_perc_lower_bound_ * slo_micros_;
    std::cout << "SLO: " << slo_micros_ << ", P99 lat micros: " << p99_lat_micros
              << ", upper bound: " << upper_bound << ", lower bound: " << lower_bound << std::endl;
    int arrival_times_size = 0;
    {
      std::unique_lock<std::mutex> arrival_time_lock(arrival_time_mutex_);
      arrival_times_size = arrival_times_.size();
    }
    if (arrival_times_size > 500 || p99_lat_micros > upper_bound) {
      // Continuously re-run optimizer once we have enough queries for the simulator
      rerun_optimizer_algorithm(socket, p99_lat_micros);
    }

    //   // std::cout << "Reactive component triggered. Observation list:" << std::endl;
    //   // for (auto t : e2e_lats) {
    //   //   std::cout << t << ", ";
    //   // }
    //   // std::cout << std::endl;
    //   rerun_optimizer_algorithm(socket, p99_lat_micros);
    // }

    iter_number += 1;
  }
  signal_close_connection(socket);
  socket.close();
}

shared_ptr<ClipperReplica> add_replica(zmq::socket_t& socket) {
  // send a json line (with \n) to the named pipe
  std::cout << "ADDING CG REPLICA" << std::endl;
  json j;
  j["action"] = "add";
  std::string s = j.dump() + "\n";
  socket.send(s.data(), s.size());

  zmq::message_t msg_response;
  socket.recv(&msg_response);
  std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
  json json_response = json::parse(response_str);
  std::cout << "ADD REPLICA RESPONSE:\n" << response_str << std::endl;

  if (json_response["result"] == "error") {
    throw runtime_error("Error: could not add coarse-grained replica");
  }

  std::shared_ptr<ClipperReplica> new_replica = std::make_shared<ClipperReplica>(
      json_response["address"], json_response["send_port"], json_response["recv_port"],
      json_response["query_port"], json_response["replica_id"], json_response["models"]);

  return new_replica;
}

void remove_replica(zmq::socket_t& socket, int id) {
  std::cout << "REMOVING CG REPLICA" << std::endl;
  // send a json line (with \n) to the named pipe
  json j;
  j["action"] = "remove";
  j["id"] = id;
  std::string s = j.dump() + "\n";
  socket.send(s.data(), s.size());

  zmq::message_t msg_response;
  socket.recv(&msg_response);
  std::string response_str(static_cast<char*>(msg_response.data()), msg_response.size());
  json json_response = json::parse(response_str);

  assert(json_response["result"] == "success");
}

CoarseGrainedReactiveComponent::CoarseGrainedReactiveComponent(
    std::vector<std::shared_ptr<ClipperReplica>> initial_replica_set, int actions_port,
    int max_batchsize, float replica_throughput, bool take_reactive_action)
    : arrival_window_count_(),
      max_batchsize_(max_batchsize),
      replica_throughput_(replica_throughput),
      actions_port_(actions_port),
      active_(true),
      take_reactive_action_(take_reactive_action) {
  for (auto rep : initial_replica_set) {
    replica_map_.emplace(rep->replica_id_, rep);
  }
}

void CoarseGrainedReactiveComponent::start() {
  if (take_reactive_action_) {
    int num_active_replicas = replica_map_.size();
    monitor_thread_ =
        std::thread([this, num_active_replicas]() { monitor_thread_loop(num_active_replicas); });
  }
}

std::shared_ptr<FrontendRPCClient> CoarseGrainedReactiveComponent::get_replica(int query_id) {
  // // First check if we've already assigned a replica for this query:
  std::unique_lock<std::mutex> inflight_query_lock(inflight_query_replica_map_mutex_);
  auto find = inflight_query_replica_map_.find(query_id);
  if (find != inflight_query_replica_map_.end()) {
    std::unique_lock<std::mutex> replica_map_lock(replica_map_mutex_);
    return replica_map_[find->second]->zmq_client_;
  } else {
    arrival_window_count_ += 1;
    int chosen_replica_id = -1;
    std::shared_ptr<FrontendRPCClient> chosen_zmq;
    std::unique_lock<std::mutex> replica_map_lock(replica_map_mutex_);
    int replica_id_with_least_queries = -1;
    int least_number_of_queries = -1;
    bool found = false;
    for (auto entry : replica_map_) {
      shared_ptr<ClipperReplica> rep = entry.second;
      if (!rep->active_) {
        continue;
      }
      if (rep->num_inflight_messages_ < max_batchsize_) {
        found = true;
        if (rep->num_inflight_messages_ == 0) {
          // std::cout << "Sending a query and waking up idle replica " << rep->replica_id_ <<
          // std::endl;
        }
        rep->num_inflight_messages_ += 1;
        chosen_replica_id = rep->replica_id_;
        chosen_zmq = rep->zmq_client_;
        break;
      }
      if (rep->num_inflight_messages_ < least_number_of_queries || least_number_of_queries == -1) {
        least_number_of_queries = rep->num_inflight_messages_;
        replica_id_with_least_queries = rep->replica_id_;
      }
    }
    if (!found) {
      chosen_replica_id = replica_id_with_least_queries;
      auto find = replica_map_.find(chosen_replica_id);
      if (find != replica_map_.end()) {
        find->second->num_inflight_messages_ += 1;
        chosen_zmq = find->second->zmq_client_;
      } else {
        throw runtime_error("Something weird happened");
      }
    }
    // std::cout << "Using replica " << chosen_replica_id << std::endl;
    inflight_query_replica_map_.emplace(query_id, chosen_replica_id);
    return chosen_zmq;
  }
}

void CoarseGrainedReactiveComponent::complete_query(int query_id) {
  std::unique_lock<std::mutex> inflight_query_lock(inflight_query_replica_map_mutex_);
  // First find the replica ID assigned to this query
  auto rep_id_find = inflight_query_replica_map_.find(query_id);
  if (rep_id_find != inflight_query_replica_map_.end()) {
    int replica_id = rep_id_find->second;
    inflight_query_replica_map_.erase(rep_id_find);
    std::unique_lock<std::mutex> replica_map_lock(replica_map_mutex_);
    auto find = replica_map_.find(replica_id);
    if (find != replica_map_.end()) {
      find->second->num_inflight_messages_ -= 1;
      if (find->second->num_inflight_messages_ == 0) {
        find->second->last_query_time_ = system_clock::now();
      }
    }
  } else {
    throw runtime_error("No inflight query found with provided query id: " +
                        std::to_string(query_id));
  }
}

void CoarseGrainedReactiveComponent::stop() {
  // std::cout << "Stopping CG Reactive" << std::endl;
  active_ = false;
  {
    std::unique_lock<std::mutex> replica_map_lock(replica_map_mutex_);
    for (auto rep : replica_map_) {
      if (rep.second->active_) {
        rep.second->active_ = false;
        rep.second->zmq_client_->stop();
      }
    }
  }
  if (take_reactive_action_) {
    monitor_thread_.join();
  }
}

void CoarseGrainedReactiveComponent::monitor_thread_loop(int num_active_replicas) {
  int iter_number = 0;
  int iter_sleep_time = 5;
  int iters_per_add_rep_check = 4;

  zmq::context_t context(1);
  // Use socket type ZMQ_REQ for the client in a request-response pair
  // connection
  zmq::socket_t socket(context, ZMQ_REQ);
  socket.connect("tcp://127.0.0.1:" + std::to_string(actions_port_));
  system_clock::time_point last_arrival_rate_check;
  // std::cout << "Monitor thread started" << std::endl;

  while (active_) {
    // std::cout << "Monitor iter: " << iter_number << std::endl;
    iter_number += 1;
    std::this_thread::sleep_for(std::chrono::seconds(iter_sleep_time));
    std::vector<int> reps_to_remove;
    // ACQUIRE BOOKKEEPING LOCK
    {
      std::unique_lock<std::mutex> lck(replica_map_mutex_);
      for (auto it = replica_map_.begin(); it != replica_map_.end(); it++) {
        if (!it->second->active_) {
          continue;
        }
        int current_idle_time_ms =
            duration_cast<milliseconds>(system_clock::now() - it->second->last_query_time_).count();
        if (it->second->num_inflight_messages_ == 0 &&
            current_idle_time_ms > MAX_IDLE_TIME_SECS * 1e3) {
          reps_to_remove.emplace_back(it->first);
          it->second->active_ = false;
          num_active_replicas -= 1;
        }
      }
    }
    if (reps_to_remove.size() > 0) {
      for (int rep_id : reps_to_remove) {
        remove_replica(socket, rep_id);
      }
    } else if (iter_number % iters_per_add_rep_check == 0) {
      // std::cout << "Checking whether to add replicas" << std::endl;
      int value_in_window = arrival_window_count_;
      int window_duration =
          duration_cast<milliseconds>(system_clock::now() - last_arrival_rate_check).count();

      float rate = float(value_in_window) / float(window_duration) * 1000.0;
      last_arrival_rate_check = system_clock::now();
      std::cout << "Reactive component detected arrival rate: " << rate << std::endl;
      std::cout << "Replica throughput " << replica_throughput_ << std::endl;
      ;
      arrival_window_count_ = 0;
      int number_of_needed_replicas =
          (int)std::ceil(rate / float(replica_throughput_));  // round up
      if (number_of_needed_replicas > 0) {
        std::cout << "Reactive component adding " << number_of_needed_replicas - num_active_replicas
                  << " new reps" << std::endl;
        std::vector<std::shared_ptr<ClipperReplica>> new_reps;
        for (int i = 0; i < number_of_needed_replicas - num_active_replicas; ++i) {
          new_reps.emplace_back(add_replica(socket));
        }
        std::unique_lock<std::mutex> lck(replica_map_mutex_);
        for (auto rep : new_reps) {
          replica_map_.emplace(rep->replica_id_, rep);
          num_active_replicas += 1;
        }
      }
    }
  }
  signal_close_connection(socket);
  socket.close();
}
/////////////////////////////////////////////////////

CoarseGrainedRandomLB::CoarseGrainedRandomLB(
    std::vector<std::shared_ptr<ClipperReplica>> initial_replica_set)
    : replica_list_(initial_replica_set),
      gen_(seed_),
      client_sampler_(0, initial_replica_set.size() - 1) {}

std::shared_ptr<FrontendRPCClient> CoarseGrainedRandomLB::get_replica(int query_id) {
  std::unique_lock<std::mutex> inflight_query_lock(inflight_query_replica_map_mutex_);
  auto find = inflight_query_replica_map_.find(query_id);
  if (find != inflight_query_replica_map_.end()) {
    return replica_list_[find->second]->zmq_client_;
  } else {
    int index = 0;
    if (replica_list_.size() > 1) {
      index = client_sampler_(gen_);
    }
    inflight_query_replica_map_.emplace(query_id, index);
    return replica_list_[index]->zmq_client_;
  }
}

void CoarseGrainedRandomLB::start() {}
void CoarseGrainedRandomLB::stop() {}

void CoarseGrainedRandomLB::complete_query(int query_id) {
  std::unique_lock<std::mutex> inflight_query_lock(inflight_query_replica_map_mutex_);
  auto rep_id_find = inflight_query_replica_map_.find(query_id);
  if (rep_id_find != inflight_query_replica_map_.end()) {
    inflight_query_replica_map_.erase(rep_id_find);
  }
}

LoadBalancer::LoadBalancer(std::vector<std::shared_ptr<ClipperReplica>> initial_replica_set,
                           int actions_server_port, long long slo_micros, int max_batchsize,
                           float replica_throughput, std::string policy,
                           std::shared_ptr<ClientMetrics> metrics, bool take_reactive_action,
                           std::map<std::string, std::tuple<int, float, float, float>> model_info, float max_load,
                           std::unordered_map<float, int> arrival_curve_max_counts)
    : policy_(policy) {
  std::cout << "Load balancer policy: " << policy_ << std::endl;
  if (policy_ == COARSE) {
    cg_reactive_ = std::make_shared<CoarseGrainedReactiveComponent>(
        std::move(initial_replica_set), actions_server_port, max_batchsize, replica_throughput,
        take_reactive_action);
    cg_reactive_->start();
  } else if (policy_ == RANDOM) {
    rlb_reactive_ = std::make_shared<CoarseGrainedRandomLB>(std::move(initial_replica_set));
    rlb_reactive_->start();
  } else if (policy_ == FINE) {
    fg_reactive_ = std::make_shared<FineGrainedReactiveComponent>(std::move(initial_replica_set),
                                                                  actions_server_port, metrics,
                                                                  slo_micros, take_reactive_action);
    fg_reactive_->start();
  } else if (policy_ == FINE_NO_OPT) {
    fg_reactive_no_opt_ = std::make_shared<FineGrainedReactiveComponentNoOpt>(
        std::move(initial_replica_set), actions_server_port, metrics, slo_micros,
        take_reactive_action, model_info, max_load, arrival_curve_max_counts);
    fg_reactive_no_opt_->start();
  } else {
    throw runtime_error("Provided invalid reactive component policy: " + policy_);
  }
}

std::shared_ptr<FrontendRPCClient> LoadBalancer::get_replica(int query_id, std::string model) {
  if (policy_ == COARSE) {
    return cg_reactive_->get_replica(query_id);
  } else if (policy_ == FINE) {
    return fg_reactive_->get_replica(model, query_id);
  } else if (policy_ == RANDOM) {
    return rlb_reactive_->get_replica(query_id);
  } else if (policy_ == FINE_NO_OPT) {
    return fg_reactive_no_opt_->get_replica(model, query_id);
  } else {
    throw runtime_error("UNREACHABLE");
  }
}

void LoadBalancer::complete_query(int query_id) {
  if (policy_ == COARSE) {
    return cg_reactive_->complete_query(query_id);
  } else if (policy_ == FINE) {
    return fg_reactive_->complete_query(query_id);
  } else if (policy_ == RANDOM) {
    return rlb_reactive_->complete_query(query_id);
  } else if (policy_ == FINE_NO_OPT) {
    return fg_reactive_no_opt_->complete_query(query_id);
  }
}

void LoadBalancer::stop() {
  if (policy_ == COARSE) {
    return cg_reactive_->stop();
  } else if (policy_ == FINE) {
    return fg_reactive_->stop();
  } else if (policy_ == RANDOM) {
    return rlb_reactive_->stop();
  } else if (policy_ == FINE_NO_OPT) {
    return fg_reactive_no_opt_->stop();
  }
}
