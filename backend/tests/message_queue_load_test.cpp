#include "queue/message_queue.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>

namespace {

constexpr std::size_t producerCount = 4;
constexpr std::size_t consumerCount = 8;
constexpr std::size_t messageCount = 50'000;

}  // namespace

int main() {
    mini_sqs::MessageQueue queue;
    std::atomic<std::size_t> published{0};
    std::atomic<std::size_t> consumed{0};

    const auto startedAt = std::chrono::steady_clock::now();

    std::array<std::thread, consumerCount> consumers;
    for (auto& consumer : consumers) {
        consumer = std::thread([&queue, &consumed] {
            while (const auto delivery = queue.receive()) {
                queue.acknowledge(delivery->receiptHandle);
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::array<std::thread, producerCount> producers;
    for (std::size_t producerIndex = 0; producerIndex < producers.size(); ++producerIndex) {
        producers[producerIndex] = std::thread([producerIndex, &queue, &published] {
            for (std::size_t index = producerIndex; index < messageCount; index += producerCount) {
                queue.publish(mini_sqs::Message::create(std::to_string(index)));
                published.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    queue.shutdown();

    for (auto& consumer : consumers) {
        consumer.join();
    }

    const auto elapsed = std::chrono::steady_clock::now() - startedAt;
    const auto publishedTotal = published.load(std::memory_order_relaxed);
    const auto consumedTotal = consumed.load(std::memory_order_relaxed);

    std::cout << "producers=" << producerCount
              << " consumers=" << consumerCount
              << " published=" << publishedTotal
              << " consumed=" << consumedTotal
              << " elapsed_ms="
              << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
              << '\n';

    if (publishedTotal != messageCount || consumedTotal != publishedTotal) {
        std::cerr << "message count mismatch under concurrent load\n";
        return 1;
    }

    return 0;
}