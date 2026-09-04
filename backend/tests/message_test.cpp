#include "queue/message.h"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace {

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void createsMessageWithExpectedFields() {
    const auto beforeCreation = std::chrono::system_clock::now();
    const auto message = mini_sqs::Message::create("hello queue", "order-123");
    const auto afterCreation = std::chrono::system_clock::now();

    expect(!message.messageId.empty(), "message ID should not be empty");
    expect(message.body == "hello queue", "message body should be preserved");
    expect(message.idempotencyKey == "order-123", "idempotency key should be preserved");
    expect(message.createdAt >= beforeCreation, "creation time should not be in the past");
    expect(message.createdAt <= afterCreation, "creation time should not be in the future");
    expect(message.receiveCount == 0, "new messages should not have been received");
}

void generatesUniqueMessageIds() {
    const auto first = mini_sqs::Message::create("first");
    const auto second = mini_sqs::Message::create("second");

    expect(first.messageId != second.messageId, "each message should have a unique ID");
    expect(first.messageId.size() == 36, "message ID should use UUID format");
    expect(first.idempotencyKey.empty(), "idempotency key should be optional");
}

}  // namespace

int main() {
    try {
        createsMessageWithExpectedFields();
        generatesUniqueMessageIds();
        std::cout << "message tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "message tests failed: " << error.what() << '\n';
        return 1;
    }
}