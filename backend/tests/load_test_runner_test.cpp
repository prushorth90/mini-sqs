#include "queue/queue_registry.h"
#include "simulator/consumer_simulator.h"
#include "simulator/load_test_runner.h"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <thread>

namespace {

using namespace std::chrono_literals;

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

mini_sqs::LoadTestSnapshot waitForResult(
    mini_sqs::LoadTestRunner& runner,
    std::chrono::steady_clock::duration timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto state = runner.snapshot();
        if (state.status == "completed" || state.status == "failed") {
            return state;
        }
        std::this_thread::sleep_for(10ms);
    }
    return runner.snapshot();
}

void processesOneThousandMessagesWithConcurrentProducers() {
    mini_sqs::QueueRegistry queues;
    mini_sqs::ConsumerSimulator simulator;
    mini_sqs::LoadTestRunner runner(queues, simulator);

    expect(runner.start({
        .queueName = "concurrent-load",
        .messageCount = 1'000,
        .producerCount = 8,
        .consumers = {
            .consumerCount = 16,
            .processingDelay = 0ms,
            .failureProbability = 0,
        },
        .visibilityTimeout = 20ms,
        .maxReceiveCount = 3,
    }), "load test should start on a fresh queue");

    const auto result = waitForResult(runner, 5s);
    expect(result.status == "completed", "load test should complete");
    expect(result.published == 1'000, "all messages should be published");
    expect(result.completed == 1'000, "all messages should be acknowledged");
    expect(result.retried == 0 && result.deadLettered == 0,
           "successful consumers should not retry or dead-letter messages");
    expect(result.peakThroughput > 0, "load test should measure peak throughput");
}

void failedConsumersDriveRetriesAndDeadLetters() {
    mini_sqs::QueueRegistry queues;
    mini_sqs::ConsumerSimulator simulator;
    mini_sqs::LoadTestRunner runner(queues, simulator);

    expect(runner.start({
        .queueName = "failing-load",
        .messageCount = 1'000,
        .producerCount = 4,
        .consumers = {
            .consumerCount = 12,
            .processingDelay = 0ms,
            .failureProbability = 1,
        },
        .visibilityTimeout = 2ms,
        .maxReceiveCount = 2,
    }), "failing load test should start on a fresh queue");

    const auto result = waitForResult(runner, 10s);
    expect(result.status == "completed", "failing load test should reach the DLQ");
    expect(result.completed == 0, "failed consumers must not acknowledge messages");
    expect(result.retried == 1'000, "each message should retry after visibility expiry");
    expect(result.deadLettered == 1'000, "each message should enter the DLQ at max receives");
}

}  // namespace

int main() {
    try {
        processesOneThousandMessagesWithConcurrentProducers();
        failedConsumersDriveRetriesAndDeadLetters();
        std::cout << "load test runner tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "load test runner tests failed: " << error.what() << '\n';
        return 1;
    }
}