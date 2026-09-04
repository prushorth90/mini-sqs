#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace mini_sqs {

struct Message {
    std::string messageId;
    std::string body;
    std::string idempotencyKey;
    std::chrono::system_clock::time_point createdAt;
    std::uint32_t receiveCount;

    static Message create(std::string body, std::string idempotencyKey = {});
};

}  // namespace mini_sqs