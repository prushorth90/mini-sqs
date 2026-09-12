#pragma once

#include "simulator/consumer_simulator.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

namespace mini_sqs {

class QueueRegistry;

struct LoadTestConfig {
    std::string queueName;
    std::uint32_t messageCount;
    std::uint32_t producerCount;
    SimulatorConfig consumers;
    std::chrono::milliseconds visibilityTimeout;
    std::uint32_t maxReceiveCount;
};

struct LoadTestSnapshot {
    std::string status;
    LoadTestConfig config;
    std::uint64_t published;
    std::uint64_t completed;
    std::uint64_t retried;
    std::uint64_t deadLettered;
    double currentThroughput;
    double averageThroughput;
    double peakThroughput;
    std::uint64_t durationMs;
    std::string error;
};

class LoadTestRunner {
public:
    LoadTestRunner(QueueRegistry& queues, ConsumerSimulator& simulator);
    ~LoadTestRunner();

    bool start(LoadTestConfig config);
    void stop();
    LoadTestSnapshot snapshot() const;

private:
    void run(std::stop_token stopToken, std::shared_ptr<MessageQueue> queue);

    QueueRegistry& queues_;
    ConsumerSimulator& simulator_;
    mutable std::mutex mutex_;
    std::jthread coordinator_;
    LoadTestConfig config_{};
    std::string status_{"idle"};
    std::string error_;
    std::atomic<std::uint64_t> published_{0};
    double peakThroughput_{0};
    std::chrono::steady_clock::time_point startedAt_{};
    std::chrono::steady_clock::time_point finishedAt_{};
};

}  // namespace mini_sqs