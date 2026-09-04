#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mini_sqs {

class MetricsRegistry {
public:
    void queueCreated(std::string_view queueName, std::size_t depth, std::size_t inFlight);
    void messagePublished(std::string_view queueName, std::size_t depth, std::size_t inFlight);
    void messageReceived(
        std::string_view queueName,
        std::chrono::duration<double> waitTime,
        std::size_t depth,
        std::size_t inFlight);
    void messageAcknowledged(
        std::string_view queueName,
        std::chrono::duration<double> processingLatency,
        std::size_t depth,
        std::size_t inFlight);
    void messagesRetried(
        std::string_view queueName,
        std::uint64_t count,
        std::size_t depth,
        std::size_t inFlight);
    void messagesDeadLettered(
        std::string_view queueName,
        std::uint64_t count,
        std::size_t depth,
        std::size_t inFlight);
    std::string prometheusText() const;

private:
    struct QueueMetrics {
        std::uint64_t published{0};
        std::uint64_t acknowledged{0};
        std::uint64_t retried{0};
        std::uint64_t deadLettered{0};
        std::size_t depth{0};
        std::size_t inFlight{0};
        std::vector<double> waitTimes;
        std::vector<double> processingLatencies;
    };

    static void setGauges(QueueMetrics& metrics, std::size_t depth, std::size_t inFlight);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, QueueMetrics> queues_;
};

}  // namespace mini_sqs