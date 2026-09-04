#pragma once

#include "queue/message.h"

#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mini_sqs {

struct Delivery {
    Message message;
    std::string receiptHandle;
};

class MessageQueue {
public:
    explicit MessageQueue(
        std::chrono::milliseconds visibilityTimeout = std::chrono::seconds(30),
        std::uint32_t maxReceiveCount = 5);
    ~MessageQueue();

    MessageQueue(const MessageQueue&) = delete;
    MessageQueue& operator=(const MessageQueue&) = delete;

    void publish(Message message);
    std::optional<Delivery> receive();
    bool acknowledge(std::string_view receiptHandle);
    std::vector<Message> deadLetterMessages() const;
    void shutdown();

private:
    struct InFlightEntry {
        Message message;
        std::chrono::steady_clock::time_point visibilityDeadline;
    };

    void reapExpiredMessages();
    bool requeueExpiredMessages(std::chrono::steady_clock::time_point now);
    std::chrono::steady_clock::time_point nextVisibilityDeadline() const;

    std::deque<Message> availableMessages_;
    std::unordered_map<std::string, InFlightEntry> inFlight_;
    std::deque<Message> deadLetterMessages_;
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::chrono::milliseconds visibilityTimeout_;
    std::uint32_t maxReceiveCount_;
    bool isShuttingDown_{false};
    std::thread reaperThread_;
};

}  // namespace mini_sqs