#include "metrics/metrics_registry.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <numeric>
#include <sstream>
#include <utility>

namespace mini_sqs {
namespace {

std::string escapeLabel(std::string_view value) {
    std::string escaped;
    for (const char character : value) {
        if (character == '\\' || character == '"' || character == '\n') {
            escaped.push_back('\\');
            escaped.push_back(character == '\n' ? 'n' : character);
        } else {
            escaped.push_back(character);
        }
    }
    return escaped;
}

double quantile(const std::vector<double>& sortedValues, double requestedQuantile) {
    if (sortedValues.empty()) {
        return 0.0;
    }
    const auto rank = static_cast<std::size_t>(
        std::ceil(requestedQuantile * static_cast<double>(sortedValues.size())));
    return sortedValues[std::max<std::size_t>(1, rank) - 1];
}

void writeSummary(
    std::ostringstream& output,
    std::string_view name,
    std::string_view queueName,
    std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const auto label = "queue=\"" + escapeLabel(queueName) + "\"";
    for (const double requestedQuantile : {0.5, 0.95, 0.99}) {
        output << name << '{' << label << ",quantile=\"" << requestedQuantile << "\"} "
               << quantile(values, requestedQuantile) << '\n';
    }
    output << name << "_sum{" << label << "} "
           << std::accumulate(values.begin(), values.end(), 0.0) << '\n'
           << name << "_count{" << label << "} " << values.size() << '\n';
}

}  // namespace

void MetricsRegistry::setGauges(
    QueueMetrics& metrics,
    std::size_t depth,
    std::size_t inFlight) {
    metrics.depth = depth;
    metrics.inFlight = inFlight;
}

void MetricsRegistry::queueCreated(
    std::string_view queueName,
    std::size_t depth,
    std::size_t inFlight) {
    std::lock_guard lock(mutex_);
    setGauges(queues_[std::string(queueName)], depth, inFlight);
}

void MetricsRegistry::messagePublished(
    std::string_view queueName,
    std::size_t depth,
    std::size_t inFlight) {
    std::lock_guard lock(mutex_);
    auto& metrics = queues_[std::string(queueName)];
    ++metrics.published;
    setGauges(metrics, depth, inFlight);
}

void MetricsRegistry::messageReceived(
    std::string_view queueName,
    std::chrono::duration<double> waitTime,
    std::size_t depth,
    std::size_t inFlight) {
    std::lock_guard lock(mutex_);
    auto& metrics = queues_[std::string(queueName)];
    metrics.waitTimes.push_back(waitTime.count());
    setGauges(metrics, depth, inFlight);
}

void MetricsRegistry::messageAcknowledged(
    std::string_view queueName,
    std::chrono::duration<double> processingLatency,
    std::size_t depth,
    std::size_t inFlight) {
    std::lock_guard lock(mutex_);
    auto& metrics = queues_[std::string(queueName)];
    ++metrics.acknowledged;
    metrics.processingLatencies.push_back(processingLatency.count());
    setGauges(metrics, depth, inFlight);
}

void MetricsRegistry::messagesRetried(
    std::string_view queueName,
    std::uint64_t count,
    std::size_t depth,
    std::size_t inFlight) {
    std::lock_guard lock(mutex_);
    auto& metrics = queues_[std::string(queueName)];
    metrics.retried += count;
    setGauges(metrics, depth, inFlight);
}

void MetricsRegistry::messagesDeadLettered(
    std::string_view queueName,
    std::uint64_t count,
    std::size_t depth,
    std::size_t inFlight) {
    std::lock_guard lock(mutex_);
    auto& metrics = queues_[std::string(queueName)];
    metrics.deadLettered += count;
    setGauges(metrics, depth, inFlight);
}

std::string MetricsRegistry::prometheusText() const {
    std::vector<std::pair<std::string, QueueMetrics>> snapshot;
    {
        std::lock_guard lock(mutex_);
        snapshot.assign(queues_.begin(), queues_.end());
    }
    std::sort(snapshot.begin(), snapshot.end(), [](const auto& left, const auto& right) {
        return left.first < right.first;
    });

    std::ostringstream output;
    output << std::setprecision(9);
        output << "# HELP messages_published_total Messages accepted by the queue.\n"
            << "# TYPE messages_published_total counter\n"
            << "# HELP messages_acked_total Messages successfully acknowledged.\n"
            << "# TYPE messages_acked_total counter\n"
            << "# HELP messages_retried_total Messages returned after visibility timeout.\n"
            << "# TYPE messages_retried_total counter\n"
            << "# HELP messages_dlq_total Messages moved to the dead-letter queue.\n"
            << "# TYPE messages_dlq_total counter\n"
            << "# HELP queue_depth Messages currently available.\n"
            << "# TYPE queue_depth gauge\n"
            << "# HELP messages_in_flight Messages awaiting acknowledgement.\n"
            << "# TYPE messages_in_flight gauge\n"
            << "# HELP message_wait_time_seconds Time from publication to delivery.\n"
            << "# TYPE message_wait_time_seconds summary\n"
            << "# HELP message_processing_latency_seconds Time from delivery to acknowledgement.\n"
            << "# TYPE message_processing_latency_seconds summary\n";
    for (const auto& [queueName, metrics] : snapshot) {
        const auto label = "{queue=\"" + escapeLabel(queueName) + "\"}";
         output << "messages_published_total" << label << ' ' << metrics.published << '\n'
             << "messages_acked_total" << label << ' ' << metrics.acknowledged << '\n'
             << "messages_retried_total" << label << ' ' << metrics.retried << '\n'
             << "messages_dlq_total" << label << ' ' << metrics.deadLettered << '\n'
             << "queue_depth" << label << ' ' << metrics.depth << '\n'
               << "messages_in_flight" << label << ' ' << metrics.inFlight << '\n';
         writeSummary(output, "message_wait_time_seconds", queueName, metrics.waitTimes);
         writeSummary(output, "message_processing_latency_seconds", queueName,
                     metrics.processingLatencies);
    }
    return output.str();
}

}  // namespace mini_sqs