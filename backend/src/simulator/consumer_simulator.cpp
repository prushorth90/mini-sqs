#include "simulator/consumer_simulator.h"

#include <random>
#include <utility>

namespace mini_sqs {

ConsumerSimulator::~ConsumerSimulator() {
    stop();
}

void ConsumerSimulator::start(
    std::string queueName,
    std::shared_ptr<MessageQueue> queue,
    SimulatorConfig config) {
    stop();

    std::lock_guard lock(mutex_);
    queueName_ = std::move(queueName);
    config_ = config;
    received_.store(0, std::memory_order_relaxed);
    acknowledged_.store(0, std::memory_order_relaxed);
    failed_.store(0, std::memory_order_relaxed);
    running_ = true;
    workers_.reserve(config.consumerCount);
    for (std::uint32_t index = 0; index < config.consumerCount; ++index) {
        workers_.emplace_back([this, queue](std::stop_token stopToken) {
            consume(stopToken, queue);
        });
    }
}

void ConsumerSimulator::stop() {
    std::vector<std::jthread> workers;
    {
        std::lock_guard lock(mutex_);
        running_ = false;
        for (auto& worker : workers_) {
            worker.request_stop();
        }
        delayCondition_.notify_all();
        workers.swap(workers_);
    }
}

SimulatorSnapshot ConsumerSimulator::snapshot() const {
    std::lock_guard lock(mutex_);
    return SimulatorSnapshot{
        .running = running_,
        .queueName = queueName_,
        .config = config_,
        .received = received_.load(std::memory_order_relaxed),
        .acknowledged = acknowledged_.load(std::memory_order_relaxed),
        .failed = failed_.load(std::memory_order_relaxed),
    };
}

void ConsumerSimulator::consume(
    std::stop_token stopToken, std::shared_ptr<MessageQueue> queue) {
    std::random_device seed;
    std::mt19937 generator(seed());
    std::bernoulli_distribution shouldFail(config_.failureProbability);

    while (!stopToken.stop_requested()) {
        const auto delivery = queue->receiveFor(std::chrono::milliseconds(100));
        if (!delivery) {
            continue;
        }
        received_.fetch_add(1, std::memory_order_relaxed);

        std::unique_lock delayLock(delayMutex_);
        delayCondition_.wait_for(
            delayLock, stopToken, config_.processingDelay, [] { return false; });
        delayLock.unlock();
        if (stopToken.stop_requested()) {
            break;
        }

        if (shouldFail(generator) || !queue->acknowledge(delivery->receiptHandle)) {
            failed_.fetch_add(1, std::memory_order_relaxed);
        } else {
            acknowledged_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

}  // namespace mini_sqs