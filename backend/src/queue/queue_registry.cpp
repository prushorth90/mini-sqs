#include "queue/queue_registry.h"

namespace mini_sqs {

std::shared_ptr<MessageQueue> QueueRegistry::getOrCreate(std::string_view queueName) {
    std::lock_guard lock(mutex_);
    auto [iterator, inserted] = queues_.try_emplace(std::string(queueName));
    if (inserted) {
        iterator->second = std::make_shared<MessageQueue>();
    }
    return iterator->second;
}

}  // namespace mini_sqs