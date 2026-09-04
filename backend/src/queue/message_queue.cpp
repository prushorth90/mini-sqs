#include "queue/message_queue.h"

#include "metrics/metrics_registry.h"
#include "queue/id_generator.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <utility>

namespace mini_sqs {

MessageQueue::MessageQueue(
        std::chrono::milliseconds visibilityTimeout,
        std::uint32_t maxReceiveCount,
        std::chrono::milliseconds deduplicationWindow,
        QueueEventSink eventSink,
        std::shared_ptr<MetricsRegistry> metrics,
        std::string queueName):
    visibilityTimeout_(visibilityTimeout),
    deduplicationWindow_(deduplicationWindow),
    maxReceiveCount_(maxReceiveCount),
    eventSink_(std::move(eventSink)),
    metrics_(std::move(metrics)),
    queueName_(std::move(queueName)) {
    if (visibilityTimeout_ <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("visibility timeout must be positive");
    }
        if (maxReceiveCount_ == 0) {
                throw std::invalid_argument("maximum receive count must be positive");
        }
        if (deduplicationWindow_ <= std::chrono::milliseconds::zero()) {
            throw std::invalid_argument("deduplication window must be positive");
        }
    reaperThread_ = std::thread(&MessageQueue::reapExpiredMessages, this);
}

MessageQueue::~MessageQueue() {
    shutdown();
}

PublishResult MessageQueue::publish(Message message) {
    PublishResult result;
    {
        std::lock_guard lock(mutex_);
        if (isShuttingDown_) {
            throw std::logic_error("cannot publish to a queue that is shutting down");
        }

        const auto now = std::chrono::steady_clock::now();
        removeExpiredDeduplicationEntries(now);
        if (!message.idempotencyKey.empty()) {
            const auto existing = deduplicationEntries_.find(message.idempotencyKey);
            if (existing != deduplicationEntries_.end()) {
                return PublishResult{
                    .message = existing->second.message,
                    .deduplicated = true,
                };
            }
            deduplicationEntries_.emplace(message.idempotencyKey, DeduplicationEntry{
                .message = message,
                .expiresAt = now + deduplicationWindow_,
            });
        }

        result = PublishResult{
            .message = message,
            .deduplicated = false,
        };
        if (eventSink_) {
            eventSink_(QueueEventType::Publish, message);
        }
        availableMessages_.push_back(std::move(message));
        if (metrics_) {
            metrics_->messagePublished(
                queueName_, availableMessages_.size(), inFlight_.size());
        }
    }
    condition_.notify_all();
    return result;
}

std::optional<Delivery> MessageQueue::receive() {
    std::unique_lock lock(mutex_);
    condition_.wait(lock, [this] {
        return isShuttingDown_ || !availableMessages_.empty();
    });

    if (availableMessages_.empty()) {
        return std::nullopt;
    }

    Message message = availableMessages_.front();
    ++message.receiveCount;

    std::string receiptHandle;
    do {
        receiptHandle = message.messageId
            + "-delivery-" + std::to_string(message.receiveCount)
            + "-" + generateUniqueId();
    } while (inFlight_.contains(receiptHandle));

    if (eventSink_) {
        eventSink_(QueueEventType::Receive, message);
    }
    availableMessages_.pop_front();
    const auto receivedAt = std::chrono::steady_clock::now();
    inFlight_.emplace(receiptHandle, InFlightEntry{
        .message = message,
        .visibilityDeadline = std::chrono::steady_clock::now() + visibilityTimeout_,
        .receivedAt = receivedAt,
    });
    if (metrics_) {
        const auto waitTime = std::chrono::system_clock::now() - message.createdAt;
        metrics_->messageReceived(
            queueName_, waitTime, availableMessages_.size(), inFlight_.size());
    }
    Delivery delivery{
        .message = std::move(message),
        .receiptHandle = std::move(receiptHandle),
    };
    lock.unlock();
    condition_.notify_all();
    return delivery;
}

bool MessageQueue::acknowledge(std::string_view receiptHandle) {
    bool acknowledged;
    {
        std::lock_guard lock(mutex_);
        const auto delivery = inFlight_.find(std::string(receiptHandle));
        acknowledged = delivery != inFlight_.end();
        if (acknowledged) {
            if (eventSink_) {
                eventSink_(QueueEventType::Acknowledge, delivery->second.message);
            }
            const auto processingLatency =
                std::chrono::steady_clock::now() - delivery->second.receivedAt;
            inFlight_.erase(delivery);
            if (metrics_) {
                metrics_->messageAcknowledged(
                    queueName_, processingLatency, availableMessages_.size(), inFlight_.size());
            }
        }
    }
    if (acknowledged) {
        condition_.notify_all();
    }
    return acknowledged;
}

std::vector<Message> MessageQueue::deadLetterMessages() const {
    std::lock_guard lock(mutex_);
    return {deadLetterMessages_.begin(), deadLetterMessages_.end()};
}

void MessageQueue::reapExpiredMessages() {
    std::unique_lock lock(mutex_);
    while (!isShuttingDown_) {
        if (inFlight_.empty()) {
            condition_.wait(lock, [this] {
                return isShuttingDown_ || !inFlight_.empty();
            });
            continue;
        }

        condition_.wait_until(lock, nextVisibilityDeadline());
        if (isShuttingDown_) {
            break;
        }

        const bool requeued = requeueExpiredMessages(std::chrono::steady_clock::now());
        if (requeued) {
            lock.unlock();
            condition_.notify_all();
            lock.lock();
        }
    }
}

bool MessageQueue::requeueExpiredMessages(std::chrono::steady_clock::time_point now) {
    bool requeued = false;
    std::uint64_t retriedCount = 0;
    std::uint64_t deadLetteredCount = 0;
    for (auto iterator = inFlight_.begin(); iterator != inFlight_.end();) {
        if (iterator->second.visibilityDeadline <= now) {
            if (iterator->second.message.receiveCount >= maxReceiveCount_) {
                if (eventSink_) {
                    eventSink_(QueueEventType::DeadLetter, iterator->second.message);
                }
                deadLetterMessages_.push_back(std::move(iterator->second.message));
                ++deadLetteredCount;
            } else {
                if (eventSink_) {
                    eventSink_(QueueEventType::Requeue, iterator->second.message);
                }
                availableMessages_.push_back(std::move(iterator->second.message));
                requeued = true;
                ++retriedCount;
            }
            iterator = inFlight_.erase(iterator);
        } else {
            ++iterator;
        }
    }
    if (metrics_ && retriedCount > 0) {
        metrics_->messagesRetried(
            queueName_, retriedCount, availableMessages_.size(), inFlight_.size());
    }
    if (metrics_ && deadLetteredCount > 0) {
        metrics_->messagesDeadLettered(
            queueName_, deadLetteredCount, availableMessages_.size(), inFlight_.size());
    }
    return requeued;
}

std::chrono::steady_clock::time_point MessageQueue::nextVisibilityDeadline() const {
    return std::min_element(
        inFlight_.begin(), inFlight_.end(), [](const auto& left, const auto& right) {
            return left.second.visibilityDeadline < right.second.visibilityDeadline;
        })->second.visibilityDeadline;
}

void MessageQueue::removeExpiredDeduplicationEntries(
    std::chrono::steady_clock::time_point now) {
    for (auto iterator = deduplicationEntries_.begin(); iterator != deduplicationEntries_.end();) {
        if (iterator->second.expiresAt <= now) {
            iterator = deduplicationEntries_.erase(iterator);
        } else {
            ++iterator;
        }
    }
}

void MessageQueue::restoreAvailable(Message message) {
    std::lock_guard lock(mutex_);
    availableMessages_.push_back(std::move(message));
}

void MessageQueue::restoreDeadLetter(Message message) {
    std::lock_guard lock(mutex_);
    deadLetterMessages_.push_back(std::move(message));
}

void MessageQueue::shutdown() {
    {
        std::lock_guard lock(mutex_);
        isShuttingDown_ = true;
    }
    condition_.notify_all();
    if (reaperThread_.joinable() && reaperThread_.get_id() != std::this_thread::get_id()) {
        reaperThread_.join();
    }
}

}  // namespace mini_sqs