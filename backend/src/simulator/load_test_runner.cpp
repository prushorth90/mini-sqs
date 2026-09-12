#include "simulator/load_test_runner.h"

#include "metrics/metrics_registry.h"
#include "queue/message.h"
#include "queue/queue_registry.h"

#include <algorithm>
#include <condition_variable>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

namespace mini_sqs {
namespace {

using namespace std::chrono_literals;

}  // namespace

LoadTestRunner::LoadTestRunner(QueueRegistry& queues, ConsumerSimulator& simulator):
    queues_(queues), simulator_(simulator) {}

LoadTestRunner::~LoadTestRunner() {
    stop();
}

bool LoadTestRunner::start(LoadTestConfig config) {
    stop();
    if (!queues_.create(
            config.queueName, config.maxReceiveCount, config.visibilityTimeout)) {
        std::lock_guard lock(mutex_);
        config_ = std::move(config);
        status_ = "failed";
        error_ = "queue already exists";
        return false;
    }

    const auto queue = queues_.find(config.queueName);
    {
        std::lock_guard lock(mutex_);
        config_ = std::move(config);
        status_ = "publishing";
        error_.clear();
        published_.store(0, std::memory_order_relaxed);
        peakThroughput_ = 0;
        startedAt_ = std::chrono::steady_clock::now();
        finishedAt_ = {};
        simulator_.start(config_.queueName, queue, config_.consumers);
        coordinator_ = std::jthread([this, queue](std::stop_token stopToken) {
            run(stopToken, queue);
        });
    }
    return true;
}

void LoadTestRunner::stop() {
    std::jthread coordinator;
    {
        std::lock_guard lock(mutex_);
        if (coordinator_.joinable()) {
            coordinator_.request_stop();
            coordinator = std::move(coordinator_);
        }
    }
    simulator_.stop();
    if (coordinator.joinable()) {
        coordinator.join();
    }
    std::lock_guard lock(mutex_);
    if (status_ == "publishing" || status_ == "processing") {
        status_ = "stopped";
        finishedAt_ = std::chrono::steady_clock::now();
    }
}

LoadTestSnapshot LoadTestRunner::snapshot() const {
    LoadTestConfig config;
    std::string status;
    std::string error;
    double peakThroughput;
    std::chrono::steady_clock::time_point startedAt;
    std::chrono::steady_clock::time_point finishedAt;
    {
        std::lock_guard lock(mutex_);
        config = config_;
        status = status_;
        error = error_;
        peakThroughput = peakThroughput_;
        startedAt = startedAt_;
        finishedAt = finishedAt_;
    }

    const auto metrics = config.queueName.empty()
        ? QueueMetricsSnapshot{}
        : queues_.metrics()->snapshot(config.queueName, false);
    const auto now = status == "publishing" || status == "processing"
        ? std::chrono::steady_clock::now()
        : finishedAt;
    const auto duration = startedAt == std::chrono::steady_clock::time_point{}
        ? std::chrono::milliseconds::zero()
        : std::chrono::duration_cast<std::chrono::milliseconds>(now - startedAt);
    return LoadTestSnapshot{
        .status = std::move(status),
        .config = std::move(config),
        .published = published_.load(std::memory_order_relaxed),
        .completed = metrics.acknowledged,
        .retried = metrics.retried,
        .deadLettered = metrics.deadLettered,
        .peakThroughput = peakThroughput,
        .durationMs = static_cast<std::uint64_t>(std::max(duration.count(), std::int64_t{0})),
        .error = std::move(error),
    };
}

void LoadTestRunner::run(
    std::stop_token stopToken, std::shared_ptr<MessageQueue> queue) {
    LoadTestConfig config;
    {
        std::lock_guard lock(mutex_);
        config = config_;
    }

    std::atomic<std::uint32_t> activeProducers{config.producerCount};
    std::vector<std::jthread> producers;
    producers.reserve(config.producerCount);
    for (std::uint32_t producer = 0; producer < config.producerCount; ++producer) {
        producers.emplace_back([&, producer](std::stop_token producerStopToken) {
            try {
                for (std::uint32_t index = producer; index < config.messageCount;
                     index += config.producerCount) {
                    if (stopToken.stop_requested() || producerStopToken.stop_requested()) {
                        break;
                    }
                    queue->publish(Message::create(
                        "load-test-message-" + std::to_string(index)));
                    published_.fetch_add(1, std::memory_order_relaxed);
                }
            } catch (const std::exception& exception) {
                std::lock_guard lock(mutex_);
                error_ = exception.what();
            }
            activeProducers.fetch_sub(1, std::memory_order_relaxed);
        });
    }

    auto sampledAt = std::chrono::steady_clock::now();
    std::uint64_t previousOperations = 0;
    while (!stopToken.stop_requested()) {
        const auto metrics = queues_.metrics()->snapshot(config.queueName, false);
        const auto published = published_.load(std::memory_order_relaxed);
        const auto now = std::chrono::steady_clock::now();
        const double seconds = std::chrono::duration<double>(now - sampledAt).count();
        if (seconds >= 0.05) {
            std::lock_guard lock(mutex_);
            peakThroughput_ = std::max(
                peakThroughput_, static_cast<double>(published - previousOperations) / seconds);
            sampledAt = now;
            previousOperations = published;
        }

        if (activeProducers.load(std::memory_order_relaxed) == 0) {
            for (auto& producer : producers) {
                if (producer.joinable()) {
                    producer.join();
                }
            }
            if (!error_.empty()) {
                break;
            }
            {
                std::lock_guard lock(mutex_);
                status_ = "processing";
            }
            if (published == config.messageCount
                && metrics.acknowledged + metrics.deadLettered == config.messageCount) {
                std::lock_guard lock(mutex_);
                status_ = "completed";
                finishedAt_ = now;
                break;
            }
        }
        std::this_thread::sleep_for(100ms);
    }

    for (auto& producer : producers) {
        producer.request_stop();
    }
    simulator_.stop();
    std::lock_guard lock(mutex_);
    if (!error_.empty()) {
        status_ = "failed";
        finishedAt_ = std::chrono::steady_clock::now();
    } else if (stopToken.stop_requested()
               && status_ != "completed" && status_ != "stopped") {
        status_ = "stopped";
        finishedAt_ = std::chrono::steady_clock::now();
    }
}

}  // namespace mini_sqs