#pragma once

#include "queue/message.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mini_sqs {

struct RecoveredQueue {
    std::uint32_t maxReceiveCount;
    std::chrono::milliseconds visibilityTimeout;
    std::vector<Message> availableMessages;
    std::vector<Message> deadLetterMessages;
};

class AppendOnlyLog {
public:
    explicit AppendOnlyLog(std::filesystem::path path);

    void recordCreate(
        std::string_view queueName,
        std::uint32_t maxReceiveCount,
        std::chrono::milliseconds visibilityTimeout);
    void recordDelete(std::string_view queueName);
    void recordPublish(std::string_view queueName, const Message& message);
    void recordReceive(std::string_view queueName, const Message& message);
    void recordAcknowledge(std::string_view queueName, const Message& message);
    void recordRequeue(std::string_view queueName, const Message& message);
    void recordDeadLetter(std::string_view queueName, const Message& message);
    std::unordered_map<std::string, RecoveredQueue> replay() const;

private:
    void append(std::string_view record);

    std::filesystem::path path_;
    mutable std::mutex mutex_;
};

}  // namespace mini_sqs