#pragma once

#include "queue/message.h"

#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mini_sqs {

class MetricsRegistry;

struct Delivery {
    Message message;
    std::string receiptHandle;
};

struct PublishResult {
    Message message;
    bool deduplicated;
};

enum class QueueEventType {
    Publish,
    Receive,
    Acknowledge,
    Requeue,
    DeadLetter,
};

using QueueEventSink = std::function<void(QueueEventType, const Message&)>;

class MessageQueue {
public:
    explicit MessageQueue(
        std::chrono::milliseconds visibilityTimeout = std::chrono::seconds(30),
        std::uint32_t maxReceiveCount = 5,
        std::chrono::milliseconds deduplicationWindow = std::chrono::minutes(5),
        QueueEventSink eventSink = {},
        std::shared_ptr<MetricsRegistry> metrics = {},
        std::string queueName = {});
    ~MessageQueue();

    MessageQueue(const MessageQueue&) = delete;
    MessageQueue& operator=(const MessageQueue&) = delete;

    PublishResult publish(Message message);
    std::optional<Delivery> receive();
    bool acknowledge(std::string_view receiptHandle);
    std::vector<Message> deadLetterMessages() const;
    void shutdown();

private:
    friend class QueueRegistry;

    struct InFlightEntry {
        Message message;
        std::chrono::steady_clock::time_point visibilityDeadline;
        std::chrono::steady_clock::time_point receivedAt;
    };

    struct DeduplicationEntry {
        Message message;
        std::chrono::steady_clock::time_point expiresAt;
    };

    void reapExpiredMessages();
    bool requeueExpiredMessages(std::chrono::steady_clock::time_point now);
    std::chrono::steady_clock::time_point nextVisibilityDeadline() const;
    void removeExpiredDeduplicationEntries(std::chrono::steady_clock::time_point now);
    void restoreAvailable(Message message);
    void restoreDeadLetter(Message message);

    std::deque<Message> availableMessages_;
    std::unordered_map<std::string, InFlightEntry> inFlight_;
    std::deque<Message> deadLetterMessages_;
    std::unordered_map<std::string, DeduplicationEntry> deduplicationEntries_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::chrono::milliseconds visibilityTimeout_;
    std::chrono::milliseconds deduplicationWindow_;
    std::uint32_t maxReceiveCount_;
    QueueEventSink eventSink_;
    std::shared_ptr<MetricsRegistry> metrics_;
    std::string queueName_;
    bool isShuttingDown_{false};
    std::thread reaperThread_;
};

}  // namespace mini_sqs