#pragma once

#include "queue/message_queue.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mini_sqs {

struct SimulatorConfig {
    std::uint32_t consumerCount;
    std::chrono::milliseconds processingDelay;
    double failureProbability;
};

struct SimulatorSnapshot {
    bool running;
    std::string queueName;
    SimulatorConfig config;
    std::uint64_t received;
    std::uint64_t acknowledged;
    std::uint64_t failed;
};

class ConsumerSimulator {
public:
    ~ConsumerSimulator();

    void start(
        std::string queueName,
        std::shared_ptr<MessageQueue> queue,
        SimulatorConfig config);
    void stop();
    SimulatorSnapshot snapshot() const;

private:
    void consume(std::stop_token stopToken, std::shared_ptr<MessageQueue> queue);

    mutable std::mutex mutex_;
    std::mutex delayMutex_;
    std::condition_variable_any delayCondition_;
    std::vector<std::jthread> workers_;
    std::string queueName_;
    SimulatorConfig config_{.consumerCount = 0, .processingDelay = {}, .failureProbability = 0};
    bool running_{false};
    std::atomic<std::uint64_t> received_{0};
    std::atomic<std::uint64_t> acknowledged_{0};
    std::atomic<std::uint64_t> failed_{0};
};

}  // namespace mini_sqs