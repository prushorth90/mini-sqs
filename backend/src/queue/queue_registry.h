#pragma once

#include "queue/message_queue.h"

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace mini_sqs {

class QueueRegistry {
public:
    std::shared_ptr<MessageQueue> getOrCreate(std::string_view queueName);

private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<MessageQueue>> queues_;
};

}  // namespace mini_sqs