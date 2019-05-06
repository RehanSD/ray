#include <concurrentqueue.h>
#include <mutex>
#include <zmq.hpp>

#include <clipper/datatypes.hpp>
#include <clipper/logging.hpp>
#include <clipper/metrics.hpp>
#include <clipper/threadpool.hpp>
#include <clipper/util.hpp>

#include "zmq_client.hpp"

using zmq::socket_t;
using zmq::message_t;
using zmq::context_t;
using std::shared_ptr;
using std::string;
using std::vector;

using namespace clipper;

ClipperReplica::ClipperReplica(std::string address, int send_port, int recv_port, int query_port,
                               int replica_id, std::vector<std::string> models)
    : address_(address),
      send_port_(send_port),
      recv_port_(recv_port),
      query_port_(query_port),
      zmq_client_{std::make_shared<FrontendRPCClient>(2)},
      replica_id_(replica_id),
      last_query_time_(std::chrono::system_clock::now()),
      num_inflight_messages_(0),
      active_(true),
      models_(models) {
  zmq_client_->start(address_, send_port_, recv_port_);
}

FrontendRPCClient::FrontendRPCClient() : FrontendRPCClient(1) {}

FrontendRPCClient::FrontendRPCClient(int num_threads)
    : request_queue_(
          std::make_shared<moodycamel::ConcurrentQueue<FrontendRPCClientRequest>>(QUEUE_SIZE)),
      active_(false),
      closure_map_{},
      closure_threadpool_("frontend_rpc", num_threads),
      client_id_(-1),
      request_id_(0),
      connected_(false) {}

void FrontendRPCClient::start(const std::string address, int send_port, int recv_port) {
  closure_threadpool_.start();
  active_ = true;
  rpc_send_thread_ =
      std::thread([this, address, send_port]() { manage_send_service(address, send_port); });
  rpc_recv_thread_ =
      std::thread([this, address, recv_port]() { manage_recv_service(address, recv_port); });
}

void FrontendRPCClient::stop() {
  if (active_) {
    active_ = false;
    rpc_recv_thread_.join();
    rpc_send_thread_.join();
  }
}

void FrontendRPCClient::send_request(
    std::string app_name, ClientFeatureVector input,
    std::function<void(ClientFeatureVector, std::shared_ptr<QueryLineage>)> &&callback) {
  std::unique_lock<std::mutex> closure_map_lock(closure_map_mutex_);
  int cur_request_id = request_id_.fetch_add(1);
  closure_map_.emplace(cur_request_id, callback);

  request_queue_->enqueue(std::make_tuple(cur_request_id, app_name, input));
}

void FrontendRPCClient::manage_send_service(const std::string ip, int port) {
  const string send_address = "tcp://" + ip + ":" + std::to_string(port);
  context_t context = context_t(1);
  socket_t socket = socket_t(context, ZMQ_DEALER);
  socket.connect(send_address);
  int num_send = -1;
  while (active_) {
    if (client_id_ == -1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } else {
      send_messages(socket, num_send);
    }
  }
  socket.disconnect(send_address);
}

void noop_free(void *data, void *hint) {
  // std::cout << "NOOP FREE CALLED" << std::endl;
}

void FrontendRPCClient::send_messages(socket_t &socket, int max_num_messages) {
  if (max_num_messages == -1) {
    max_num_messages = request_queue_->size_approx();
  }
  FrontendRPCClientRequest request;
  size_t sent_requests = 0;
  while (sent_requests < max_num_messages && request_queue_->try_dequeue(request) && active_) {
    int request_id = std::get<0>(request);
    std::string app_name = std::get<1>(request);
    ClientFeatureVector input = std::get<2>(request);
    int datatype = (int)input.type_;
    int datalength = int(input.size_typed_);

    zmq::message_t msg_data(input.get_data(), input.size_bytes_, noop_free);

    socket.send("", 0, ZMQ_SNDMORE);
    socket.send(&client_id_, sizeof(int), ZMQ_SNDMORE);
    socket.send(&request_id, sizeof(int), ZMQ_SNDMORE);
    socket.send(app_name.data(), app_name.length(), ZMQ_SNDMORE);
    socket.send(&datatype, sizeof(int), ZMQ_SNDMORE);
    socket.send(&datalength, sizeof(int), ZMQ_SNDMORE);
    socket.send(msg_data);
  }
}

void FrontendRPCClient::manage_recv_service(const std::string ip, int port) {
  const string recv_address = "tcp://" + ip + ":" + std::to_string(port);
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_DEALER);
  zmq::pollitem_t items[] = {{socket, 0, ZMQ_POLLIN, 0}};
  socket.connect(recv_address);

  // Send a blank message to establish a connection
  socket.send("", 0, ZMQ_SNDMORE);
  socket.send("", 0);
  while (active_) {
    int timeout = 1000;
    if (!connected_) {
      timeout = 5000;
    }
    zmq_poll(items, 1, timeout);
    if (items[0].revents & ZMQ_POLLIN) {
      if (connected_) {
        receive_response(socket);
      } else {
        handle_new_connection(socket);
        connected_ = true;
      }
    }
  }
  socket.disconnect(recv_address);
}

void FrontendRPCClient::handle_new_connection(zmq::socket_t &socket) {
  zmq::message_t msg_delimiter;
  zmq::message_t msg_client_id;
  socket.recv(&msg_delimiter, 0);
  socket.recv(&msg_client_id, 0);
  client_id_ = static_cast<int *>(msg_client_id.data())[0];
}

void FrontendRPCClient::receive_response(zmq::socket_t &socket) {
  zmq::message_t msg_delimiter;
  zmq::message_t msg_request_id;
  zmq::message_t msg_data_type;
  zmq::message_t msg_data_length_bytes;

  socket.recv(&msg_delimiter, 0);
  socket.recv(&msg_request_id, 0);
  socket.recv(&msg_data_type, 0);
  socket.recv(&msg_data_length_bytes, 0);

  int request_id = static_cast<int *>(msg_request_id.data())[0];
  DataType output_type = static_cast<DataType>(static_cast<int *>(msg_data_type.data())[0]);
  size_t data_length_bytes = (size_t)(static_cast<int *>(msg_data_length_bytes.data())[0]);

  size_t bytes_per_input;
  switch (output_type) {
    case DataType::Bytes: {
      bytes_per_input = sizeof(uint8_t);
    } break;
    case DataType::Floats: {
      bytes_per_input = sizeof(float);
    } break;
    case DataType::Doubles: {
      bytes_per_input = sizeof(double);
    } break;
    case DataType::Strings: {
      bytes_per_input = sizeof(char);
    } break;
    case DataType::Invalid:
    default: {
      std::stringstream ss;
      ss << "Received a response with an invalid output type: "
         << std::to_string(static_cast<int *>(msg_data_type.data())[0]);
      throw std::runtime_error(ss.str());
    }
  }

  size_t data_length_typed = data_length_bytes / bytes_per_input;
  if (data_length_bytes % bytes_per_input != 0) {
    std::stringstream ss;
    ss << "Received a response with a corrupted output length." << std::endl;
    throw std::runtime_error(ss.str());
  }

  std::shared_ptr<void> output_recv_buffer(malloc(data_length_bytes), free);
  socket.recv(output_recv_buffer.get(), data_length_bytes, 0);
  ClientFeatureVector output(output_recv_buffer, data_length_typed, data_length_bytes, output_type);

  zmq::message_t msg_lineage_length;

  socket.recv(&msg_lineage_length, 0);
  int lineage_length = static_cast<int *>(msg_lineage_length.data())[0];
  std::shared_ptr<QueryLineage> lineage = std::make_shared<QueryLineage>(request_id);
  for (int i = 0; i < lineage_length; ++i) {
    zmq::message_t msg_description;
    socket.recv(&msg_description, 0);
    std::string description(static_cast<char *>(msg_description.data()), msg_description.size());
    zmq::message_t msg_timestamp;
    socket.recv(&msg_timestamp, 0);
    long long timestamp = static_cast<long long *>(msg_timestamp.data())[0];
    lineage->add_timestamp(description, timestamp);
  }

  std::unique_lock<std::mutex> closure_map_lock(closure_map_mutex_);
  auto search = closure_map_.find(request_id);
  if (search == closure_map_.end()) {
    std::stringstream ss;
    ss << "Received a response for request ID " << std::to_string(request_id);
    ss << " with no associated closure";
    throw std::runtime_error(ss.str());
  } else {
    auto closure = search->second;
    closure_map_.erase(search);
    closure_map_lock.unlock();
    closure_threadpool_.submit(closure, std::move(output), lineage);
  }
}

ClientFeatureVector::ClientFeatureVector(std::shared_ptr<void> data, size_t size_typed,
                                         size_t size_bytes, DataType type)
    : data_(data), size_typed_(size_typed), size_bytes_(size_bytes), type_(type) {}

void *ClientFeatureVector::get_data() { return data_.get(); }
