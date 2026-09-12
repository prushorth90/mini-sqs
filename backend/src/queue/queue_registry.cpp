#include "queue/queue_registry.h"

#include "metrics/metrics_registry.h"
#include "storage/append_only_log.h"

#include <algorithm>
#include <utility>

namespace mini_sqs {

QueueRegistry::QueueRegistry(): metrics_(std::make_shared<MetricsRegistry>()) {}

QueueRegistry::QueueRegistry(const std::filesystem::path& logPath):
    log_(std::make_shared<AppendOnlyLog>(logPath)),
    metrics_(std::make_shared<MetricsRegistry>()) {
    for (auto& [queueName, recovered] : log_->replay()) {
        auto queue = makeQueue(
            queueName, recovered.maxReceiveCount, recovered.visibilityTimeout);
        for (auto& message : recovered.availableMessages) {
            queue->restoreAvailable(std::move(message));
        }
        for (auto& message : recovered.deadLetterMessages) {
            queue->restoreDeadLetter(std::move(message));
        }
        metrics_->queueCreated(
            queueName, recovered.availableMessages.size(), 0);
        queues_.emplace(std::move(queueName), std::move(queue));
    }
}

bool QueueRegistry::create(
    std::string_view queueName,
    std::uint32_t maxReceiveCount,
    std::chrono::milliseconds visibilityTimeout) {
    std::lock_guard lock(mutex_);
    std::string name(queueName);
    if (queues_.contains(name)) {
        return false;
    }
    auto queue = makeQueue(name, maxReceiveCount, visibilityTimeout);
    if (log_) {
        log_->recordCreate(name, maxReceiveCount, visibilityTimeout);
    }
    queues_.emplace(std::move(name), std::move(queue));
    metrics_->queueCreated(queueName, 0, 0);
    return true;
}

bool QueueRegistry::remove(std::string_view queueName) {
    std::lock_guard lock(mutex_);
    const auto iterator = queues_.find(std::string(queueName));
    if (iterator == queues_.end()) {
        return false;
    }
    iterator->second->shutdown();
    if (log_) {
        log_->recordDelete(queueName);
    }
    queues_.erase(iterator);
    metrics_->queueDeleted(queueName);
    return true;
}

std::shared_ptr<MessageQueue> QueueRegistry::makeQueue(
    std::string queueName,
    std::uint32_t maxReceiveCount,
    std::chrono::milliseconds visibilityTimeout) {
    QueueEventSink eventSink;
    if (log_) {
        eventSink = [log = log_, queueName](QueueEventType type, const Message& message) {
            switch (type) {
                case QueueEventType::Publish:
                    log->recordPublish(queueName, message);
                    break;
                case QueueEventType::Receive:
                    log->recordReceive(queueName, message);
                    break;
                case QueueEventType::Acknowledge:
                    log->recordAcknowledge(queueName, message);
                    break;
                case QueueEventType::Requeue:
                    log->recordRequeue(queueName, message);
                    break;
                case QueueEventType::DeadLetter:
                    log->recordDeadLetter(queueName, message);
                    break;
            }
        };
    }
    return std::make_shared<MessageQueue>(
        visibilityTimeout, maxReceiveCount, std::chrono::minutes(5),
        std::move(eventSink), metrics_, std::move(queueName));
}

std::shared_ptr<MessageQueue> QueueRegistry::find(std::string_view queueName) const {
    std::lock_guard lock(mutex_);
    const auto iterator = queues_.find(std::string(queueName));
    if (iterator == queues_.end()) {
        return nullptr;
    }
    return iterator->second;
}

std::vector<std::string> QueueRegistry::list() const {
    std::vector<std::string> names;
    {
        std::lock_guard lock(mutex_);
        names.reserve(queues_.size());
        for (const auto& [name, queue] : queues_) {
            names.push_back(name);
        }
    }
    std::sort(names.begin(), names.end());
    return names;
}

std::shared_ptr<MetricsRegistry> QueueRegistry::metrics() const {
    return metrics_;
}

}  // namespace mini_sqs