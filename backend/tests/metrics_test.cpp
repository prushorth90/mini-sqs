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
        expectContains(output, "messages_published_total{queue=\"orders\"} 2");
        expectContains(output, "messages_acked_total{queue=\"orders\"} 1");
        expectContains(output, "messages_retried_total{queue=\"orders\"} 1");
        expectContains(output, "messages_dlq_total{queue=\"orders\"} 1");
        expectContains(output, "queue_depth{queue=\"orders\"} 0");
        expectContains(output, "messages_in_flight{queue=\"orders\"} 0");
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