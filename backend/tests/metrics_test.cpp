#include "metrics/metrics_registry.h"
#include "queue/message_queue.h"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

void expectContains(const std::string& metrics, std::string_view expected) {
    if (metrics.find(expected) == std::string::npos) {
        throw std::runtime_error("missing metric: " + std::string(expected));
    }
}

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void exposesQueueMetricsSnapshot() {
    mini_sqs::MetricsRegistry metrics;
    metrics.queueCreated("load", 0, 0);
    metrics.messagePublished("load", 1, 0);
    metrics.messageReceived("load", 10ms, 0, 1);
    metrics.messagesRetried("load", 2, 1, 0);
    metrics.messageAcknowledged("load", 25ms, 0, 0);
    metrics.messagesDeadLettered("load", 1, 0, 0);

    const auto snapshot = metrics.snapshot("load");
    expect(snapshot.published == 1, "snapshot should include published count");
    expect(snapshot.acknowledged == 1, "snapshot should include acknowledged count");
    expect(snapshot.retried == 2, "snapshot should include retry count");
    expect(snapshot.deadLettered == 1, "snapshot should include DLQ count");
    expect(snapshot.depth == 0 && snapshot.inFlight == 0,
           "snapshot should include current gauges");
    expect(snapshot.p95ProcessingLatencySeconds == 0.025,
           "snapshot should include processing latency P95");
        expect(snapshot.currentThroughput == 1,
            "snapshot should count ACKs in the one-second throughput window");
}

    void currentThroughputReturnsToZeroWhileAveragePersists() {
        mini_sqs::MetricsRegistry metrics;
        metrics.queueCreated("throughput", 0, 0);
        metrics.messageAcknowledged("throughput", 1ms, 0, 0);
        std::this_thread::sleep_for(10ms);
        metrics.messageAcknowledged("throughput", 1ms, 0, 0);

        const auto active = metrics.snapshot("throughput");
        expect(active.currentThroughput == 2,
            "current throughput should use actual recent ACK timestamps");
        expect(active.averageThroughput > 0,
            "average throughput should use the full ACK timestamp span");
            expect(active.peakThroughput == 2,
                "peak throughput should preserve the highest rolling ACK rate");

        std::this_thread::sleep_for(1050ms);
        const auto idle = metrics.snapshot("throughput");
        expect(idle.currentThroughput == 0,
            "current throughput should return to zero after one idle window");
        expect(idle.averageThroughput == active.averageThroughput,
            "average throughput should remain after the queue becomes idle");
            expect(idle.peakThroughput == active.peakThroughput,
                "peak throughput should remain after the queue becomes idle");
    }

}  // namespace

int main() {
    try {
        auto metrics = std::make_shared<mini_sqs::MetricsRegistry>();
        mini_sqs::MessageQueue queue(10ms, 2, 5min, {}, metrics, "orders");
        metrics->queueCreated("orders", 0, 0);

        queue.publish(mini_sqs::Message::create("acknowledge"));
        const auto acknowledged = queue.receive();
        std::this_thread::sleep_for(2ms);
        queue.acknowledge(acknowledged->receiptHandle);

        queue.publish(mini_sqs::Message::create("dead-letter"));
        queue.receive();
        queue.receive();
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (queue.deadLetterMessages().empty()
               && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }

        const auto output = metrics->prometheusText();
        exposesQueueMetricsSnapshot();
        currentThroughputReturnsToZeroWhileAveragePersists();
        expectContains(output, "messages_published_total{queue=\"orders\"} 2");
        expectContains(output, "messages_acked_total{queue=\"orders\"} 1");
        expectContains(output, "messages_retried_total{queue=\"orders\"} 1");
        expectContains(output, "messages_dlq_total{queue=\"orders\"} 1");
        expectContains(output, "queue_depth{queue=\"orders\"} 0");
        expectContains(output, "messages_in_flight{queue=\"orders\"} 0");
        expectContains(output, "messages_completed_per_second{queue=\"orders\"} 1");
        expectContains(output, "message_wait_time_seconds{queue=\"orders\",quantile=\"0.5\"}");
        expectContains(output, "message_wait_time_seconds_count{queue=\"orders\"} 3");
        expectContains(output, "message_processing_latency_seconds{queue=\"orders\",quantile=\"0.95\"}");
        expectContains(output, "message_processing_latency_seconds_count{queue=\"orders\"} 1");

        std::vector<std::thread> writers;
        for (std::size_t writer = 0; writer < 8; ++writer) {
            writers.emplace_back([metrics] {
                for (std::size_t update = 0; update < 250; ++update) {
                    metrics->messagePublished("concurrent", update, 0);
                    metrics->messageReceived("concurrent", 1ms, update, 1);
                }
            });
        }
        for (auto& writer : writers) {
            writer.join();
        }
        const auto concurrentOutput = metrics->prometheusText();
        expectContains(
            concurrentOutput, "messages_published_total{queue=\"concurrent\"} 2000");
        expectContains(
            concurrentOutput, "message_wait_time_seconds_count{queue=\"concurrent\"} 2000");

        std::cout << "metrics tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "metrics tests failed: " << error.what() << '\n';
        return 1;
    }
}