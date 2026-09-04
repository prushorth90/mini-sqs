#include "queue/message_queue.h"

#include <stdexcept>
#include <utility>

namespace mini_sqs {

void MessageQueue::publish(Message message) {
    {
        std::lock_guard lock(mutex_);
        if (isShuttingDown_) {
            throw std::logic_error("cannot publish to a queue that is shutting down");
        }
        availableMessages_.push_back(std::move(message));
    }
    condition_.notify_one();
}

std::optional<Message> MessageQueue::receive() {
    std::unique_lock lock(mutex_);
    condition_.wait(lock, [this] {
        return isShuttingDown_ || !availableMessages_.empty();
    });

    if (availableMessages_.empty()) {
        return std::nullopt;
    }

    Message message = std::move(availableMessages_.front());
    availableMessages_.pop_front();
    ++message.receiveCount;
    return message;
}

void MessageQueue::shutdown() {
    {
        std::lock_guard lock(mutex_);
        isShuttingDown_ = true;
    }
    condition_.notify_all();
}

}  // namespace mini_sqs