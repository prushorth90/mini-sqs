#include "queue/queue_registry.h"

#include <algorithm>

namespace mini_sqs {

bool QueueRegistry::create(
    std::string_view queueName,
    std::uint32_t maxReceiveCount,
    std::chrono::milliseconds visibilityTimeout) {
    std::lock_guard lock(mutex_);
    auto [iterator, inserted] = queues_.try_emplace(
        std::string(queueName),
        std::make_shared<MessageQueue>(visibilityTimeout, maxReceiveCount));
    return inserted;
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

}  // namespace mini_sqs