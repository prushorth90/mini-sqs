#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mini_sqs {

struct QueueMetricsSnapshot {
    std::uint64_t published;
    std::uint64_t acknowledged;
    std::uint64_t retried;
    std::uint64_t deadLettered;
    std::size_t depth;
    std::size_t inFlight;
    double currentThroughput;
    double averageThroughput;
    double peakThroughput;
    double p95ProcessingLatencySeconds;
};

class MetricsRegistry {
public:
    void queueCreated(std::string_view queueName, std::size_t depth, std::size_t inFlight);
    void queueDeleted(std::string_view queueName);
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
    QueueMetricsSnapshot snapshot(
        std::string_view queueName, bool includeLatency = true) const;
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
        std::deque<std::chrono::steady_clock::time_point> acknowledgementTimes;
        std::optional<std::chrono::steady_clock::time_point> firstAcknowledgedAt;
        std::optional<std::chrono::steady_clock::time_point> lastAcknowledgedAt;
        double peakThroughput{0};
    };

    static void setGauges(QueueMetrics& metrics, std::size_t depth, std::size_t inFlight);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, QueueMetrics> queues_;
};

}  // namespace mini_sqs