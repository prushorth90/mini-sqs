#pragma once

#include "queue/message_queue.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mini_sqs {

class QueueRegistry {
public:
    bool create(
        std::string_view queueName,
        std::uint32_t maxReceiveCount = 5,
        std::chrono::milliseconds visibilityTimeout = std::chrono::seconds(30));
    std::shared_ptr<MessageQueue> find(std::string_view queueName) const;
    std::vector<std::string> list() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<MessageQueue>> queues_;
};

}  // namespace mini_sqs