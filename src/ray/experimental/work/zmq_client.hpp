#ifndef CLIPPER_ZMQ_CLIENT_HPP
#define CLIPPER_ZMQ_CLIENT_HPP

#include <mutex>

#include <concurrentqueue.h>
#include <clipper/callback_threadpool.hpp>
#include <clipper/datatypes.hpp>
#include <zmq.hpp>

using namespace clipper;

const std::string LOGGING_TAG_ZMQ_CLIENT = "ZMQ_FRONTEND";

// // We may have up to 50,000 outstanding requests
constexpr size_t QUEUE_SIZE = 100000;

class ClientFeatureVector {
 public:
  ClientFeatureVector() = default;

  ClientFeatureVector(std::shared_ptr<void> data, size_t size_typed, size_t size_bytes,
                      DataType type);

  // Copy constructors
  ClientFeatureVector(const ClientFeatureVector &other) = default;
  ClientFeatureVector &operator=(const ClientFeatureVector &other) = default;

  // move constructors
  ClientFeatureVector(ClientFeatureVector &&other) = default;
  ClientFeatureVector &operator=(ClientFeatureVector &&other) = default;

  void *get_data();
  std::shared_ptr<void> data_;
  size_t size_typed_;
  size_t size_bytes_;
  DataType type_;
};

// Tuple of request ID, app name, input
typedef std::tuple<int, std::string, ClientFeatureVector> FrontendRPCClientRequest;
// Tuple of request id, output.
// typedef std::tuple<int, Output> FrontendRPCClientResponse;

class FrontendRPCClient {
 public:
  FrontendRPCClient();
  explicit FrontendRPCClient(int num_threads);
  ~FrontendRPCClient() = default;

  // move constructors
  FrontendRPCClient(FrontendRPCClient &&other) = default;
  FrontendRPCClient &operator=(FrontendRPCClient &&other) = default;

  FrontendRPCClient(const FrontendRPCClient &) = delete;
  FrontendRPCClient &operator=(const FrontendRPCClient &) = delete;

  void start(const std::string address, int send_port, int recv_port);
  void stop();
  void send_request(
      std::string app_name, ClientFeatureVector input,
      std::function<void(ClientFeatureVector, std::shared_ptr<QueryLineage>)> &&callback);

 private:
  void manage_send_service(const std::string ip, int port);
  void manage_recv_service(const std::string ip, int port);
  void receive_response(zmq::socket_t &socket);
  void handle_new_connection(zmq::socket_t &socket);
  void send_messages(zmq::socket_t &socket, int max_num_messages);

  std::shared_ptr<moodycamel::ConcurrentQueue<FrontendRPCClientRequest>> request_queue_;
  // std::shared_ptr<clipper::CallbackThreadPool> prediction_executor_;
  std::atomic_bool active_;
  std::thread rpc_send_thread_;
  std::thread rpc_recv_thread_;

  std::unordered_map<int, std::function<void(ClientFeatureVector, std::shared_ptr<QueryLineage>)>>
      closure_map_;
  std::mutex closure_map_mutex_;

  CallbackThreadPool closure_threadpool_;

  int client_id_;
  std::atomic<int> request_id_;
  std::atomic_bool connected_;
};

class ClipperReplica {
 public:
  ClipperReplica(std::string address, int send_port, int recv_port, int query_port, int replica_id,
                 std::vector<std::string> models);

  std::string address_;
  int send_port_;
  int recv_port_;
  int query_port_;
  std::shared_ptr<FrontendRPCClient> zmq_client_;
  int replica_id_;
  std::chrono::system_clock::time_point last_query_time_;
  // int num_inflight_messages_;
  std::atomic<int> num_inflight_messages_;
  bool active_;
  // A list of the models registered with this replica. Only used for fine-grained
  // InferLine when partitioning the ZMQ frontend across models.
  std::vector<std::string> models_;
};

#endif  // CLIPPER_ZMQ_CLIENT_HPP
