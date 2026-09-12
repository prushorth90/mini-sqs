#pragma once

#include "queue/message_queue.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mini_sqs {

class AppendOnlyLog;
class MetricsRegistry;

class QueueRegistry {
public:
    QueueRegistry();
    explicit QueueRegistry(const std::filesystem::path& logPath);

    bool create(
        std::string_view queueName,
        std::uint32_t maxReceiveCount = 5,
        std::chrono::milliseconds visibilityTimeout = std::chrono::seconds(30));
    bool remove(std::string_view queueName);
    std::shared_ptr<MessageQueue> find(std::string_view queueName) const;
    std::vector<std::string> list() const;
    std::shared_ptr<MetricsRegistry> metrics() const;

private:
    std::shared_ptr<MessageQueue> makeQueue(
        std::string queueName,
        std::uint32_t maxReceiveCount,
        std::chrono::milliseconds visibilityTimeout);

    std::shared_ptr<AppendOnlyLog> log_;
    std::shared_ptr<MetricsRegistry> metrics_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<MessageQueue>> queues_;
};

}  // namespace mini_sqs