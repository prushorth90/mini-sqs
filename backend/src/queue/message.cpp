#include "queue/message.h"

#include <array>
#include <random>
#include <utility>

namespace mini_sqs {
namespace {

std::string generateMessageId() {
    static constexpr char hexDigits[] = "0123456789abcdef";
    static thread_local std::mt19937 generator(std::random_device{}());
    static thread_local std::uniform_int_distribution<unsigned int> distribution(0, 255);

    std::array<unsigned char, 16> bytes{};
    for (auto& byte : bytes) {
        byte = static_cast<unsigned char>(distribution(generator));
    }

    bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0f) | 0x40);
    bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3f) | 0x80);

    std::string id;
    id.reserve(36);
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10) {
            id.push_back('-');
        }
        id.push_back(hexDigits[bytes[index] >> 4]);
        id.push_back(hexDigits[bytes[index] & 0x0f]);
    }
    return id;
}

}  // namespace

Message Message::create(std::string body, std::string idempotencyKey) {
    return Message{
        .messageId = generateMessageId(),
        .body = std::move(body),
        .idempotencyKey = std::move(idempotencyKey),
        .createdAt = std::chrono::system_clock::now(),
        .receiveCount = 0,
    };
}

}  // namespace mini_sqs