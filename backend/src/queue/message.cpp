#include "queue/message.h"

#include "queue/id_generator.h"

#include <utility>

namespace mini_sqs {

Message Message::create(std::string body, std::string idempotencyKey) {
    return Message{
        .messageId = generateUniqueId(),
        .body = std::move(body),
        .idempotencyKey = std::move(idempotencyKey),
        .createdAt = std::chrono::system_clock::now(),
        .receiveCount = 0,
    };
}

}  // namespace mini_sqs