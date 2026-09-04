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
    reaperThread_ = std::thread(&MessageQueue::reapExpiredMessages, this);
}

MessageQueue::~MessageQueue() {
    shutdown();
}

void MessageQueue::publish(Message message) {
    {
        std::lock_guard lock(mutex_);
        if (isShuttingDown_) {
            throw std::logic_error("cannot publish to a queue that is shutting down");
        }
        availableMessages_.push_back(std::move(message));
    }
    condition_.notify_all();
}

std::optional<Delivery> MessageQueue::receive() {
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

    std::string receiptHandle;
    do {
        receiptHandle = generateUniqueId();
    } while (inFlight_.contains(receiptHandle));

    inFlight_.emplace(receiptHandle, InFlightEntry{
        .message = message,
        .visibilityDeadline = std::chrono::steady_clock::now() + visibilityTimeout_,
    });
    Delivery delivery{
        .message = std::move(message),
        .receiptHandle = std::move(receiptHandle),
    };
    lock.unlock();
    condition_.notify_all();
    return delivery;
}

bool MessageQueue::acknowledge(std::string_view receiptHandle) {
    bool acknowledged;
    {
        std::lock_guard lock(mutex_);
        acknowledged = inFlight_.erase(std::string(receiptHandle)) == 1;
    }
    if (acknowledged) {
        condition_.notify_all();
    }
    return acknowledged;
}

void MessageQueue::reapExpiredMessages() {
    std::unique_lock lock(mutex_);
    while (!isShuttingDown_) {
        if (inFlight_.empty()) {
            condition_.wait(lock, [this] {
                return isShuttingDown_ || !inFlight_.empty();
            });
            continue;
        }

        condition_.wait_until(lock, nextVisibilityDeadline());
        if (isShuttingDown_) {
            break;
        }

        const bool requeued = requeueExpiredMessages(std::chrono::steady_clock::now());
        if (requeued) {
            lock.unlock();
            condition_.notify_all();
            lock.lock();
        }
    }
}

bool MessageQueue::requeueExpiredMessages(std::chrono::steady_clock::time_point now) {
    bool requeued = false;
    for (auto iterator = inFlight_.begin(); iterator != inFlight_.end();) {
        if (iterator->second.visibilityDeadline <= now) {
            availableMessages_.push_back(std::move(iterator->second.message));
            iterator = inFlight_.erase(iterator);
            requeued = true;
        } else {
            ++iterator;
        }
    }
    return requeued;
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
    if (reaperThread_.joinable() && reaperThread_.get_id() != std::this_thread::get_id()) {
        reaperThread_.join();
    }
}

}  // namespace mini_sqs