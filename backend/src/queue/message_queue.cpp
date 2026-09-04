#include "queue/message_queue.h"

#include "queue/id_generator.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <utility>

namespace mini_sqs {

MessageQueue::MessageQueue(std::chrono::milliseconds visibilityTimeout):
    visibilityTimeout_(visibilityTimeout) {
        if (visibilityTimeout_ <= std::chrono::milliseconds::zero()) {
                throw std::invalid_argument("visibility timeout must be positive");
        }
}

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

std::optional<Delivery> MessageQueue::receive() {
    std::unique_lock lock(mutex_);

    while (availableMessages_.empty() && !isShuttingDown_) {
        requeueExpiredMessages(std::chrono::steady_clock::now());
        if (!availableMessages_.empty()) {
            break;
        }

        if (inFlight_.empty()) {
            condition_.wait(lock, [this] {
                return isShuttingDown_ || !availableMessages_.empty();
            });
        } else {
            condition_.wait_until(lock, nextVisibilityDeadline(), [this] {
                return isShuttingDown_ || !availableMessages_.empty();
            });
        }
    }

    if (availableMessages_.empty()) {
        return std::nullopt;
    }

    Message message = std::move(availableMessages_.front());
    availableMessages_.pop_front();
    ++message.receiveCount;

    std::string receiptHandle;
    do {
        receiptHandle = generateUniqueId();
    } while (inFlight_.contains(receiptHandle));

    inFlight_.emplace(receiptHandle, InFlightEntry{
        .message = message,
        .visibilityDeadline = std::chrono::steady_clock::now() + visibilityTimeout_,
    });
    return Delivery{
        .message = std::move(message),
        .receiptHandle = std::move(receiptHandle),
    };
}

bool MessageQueue::acknowledge(std::string_view receiptHandle) {
    std::lock_guard lock(mutex_);
    return inFlight_.erase(std::string(receiptHandle)) == 1;
}

void MessageQueue::requeueExpiredMessages(std::chrono::steady_clock::time_point now) {
    for (auto iterator = inFlight_.begin(); iterator != inFlight_.end();) {
        if (iterator->second.visibilityDeadline <= now) {
            availableMessages_.push_back(std::move(iterator->second.message));
            iterator = inFlight_.erase(iterator);
        } else {
            ++iterator;
        }
    }
}

std::chrono::steady_clock::time_point MessageQueue::nextVisibilityDeadline() const {
    return std::min_element(
        inFlight_.begin(), inFlight_.end(), [](const auto& left, const auto& right) {
            return left.second.visibilityDeadline < right.second.visibilityDeadline;
        })->second.visibilityDeadline;
}

void MessageQueue::shutdown() {
    {
        std::lock_guard lock(mutex_);
        isShuttingDown_ = true;
    }
    condition_.notify_all();
}

}  // namespace mini_sqs