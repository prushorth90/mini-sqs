#pragma once

#include "queue/message.h"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>

namespace mini_sqs {

class MessageQueue {
public:
    void publish(Message message);
    std::optional<Message> receive();
    void shutdown();

private:
    std::deque<Message> availableMessages_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool isShuttingDown_{false};
};

}  // namespace mini_sqs