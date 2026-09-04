#pragma once

#include "queue/message.h"

#include <condition_variable>
#include <chrono>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace mini_sqs {

struct Delivery {
    Message message;
    std::string receiptHandle;
};

class MessageQueue {
public:
    explicit MessageQueue(
        std::chrono::milliseconds visibilityTimeout = std::chrono::seconds(30));

    void publish(Message message);
    std::optional<Delivery> receive();
    bool acknowledge(std::string_view receiptHandle);
    void shutdown();

private:
    struct InFlightEntry {
        Message message;
        std::chrono::steady_clock::time_point visibilityDeadline;
    };

    void requeueExpiredMessages(std::chrono::steady_clock::time_point now);
    std::chrono::steady_clock::time_point nextVisibilityDeadline() const;

    std::deque<Message> availableMessages_;
    std::unordered_map<std::string, InFlightEntry> inFlight_;
    std::mutex mutex_;
    std::condition_variable condition_;
    std::chrono::milliseconds visibilityTimeout_;
    bool isShuttingDown_{false};
};

}  // namespace mini_sqs