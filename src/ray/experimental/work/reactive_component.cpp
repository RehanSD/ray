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

  auto iter_end = std::chrono::system_clock::now();
  last_iter_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      iter_end - iter_start).count();
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


