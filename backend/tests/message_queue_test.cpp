#include "queue/message_queue.h"

#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace {

using namespace std::chrono_literals;

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void deliversMessagesInPublishOrder() {
    mini_sqs::MessageQueue queue;
    queue.publish(mini_sqs::Message::create("first"));
    queue.publish(mini_sqs::Message::create("second"));

    const auto first = queue.receive();
    const auto second = queue.receive();

    expect(first.has_value(), "first published message should be received");
    expect(second.has_value(), "second published message should be received");
    expect(first->body == "first", "queue should preserve FIFO order");
    expect(second->body == "second", "queue should preserve FIFO order");
    expect(first->receiveCount == 1, "receiving should increment the receive count");
}

void wakesBlockedConsumerWhenMessageIsPublished() {
    mini_sqs::MessageQueue queue;
    std::promise<void> consumerStarted;
    auto started = consumerStarted.get_future();
    auto received = std::async(std::launch::async, [&queue, &consumerStarted] {
        consumerStarted.set_value();
        return queue.receive();
    });

    started.wait();
    expect(received.wait_for(50ms) == std::future_status::timeout,
           "consumer should block while the queue is empty");

    queue.publish(mini_sqs::Message::create("wake up"));

    expect(received.wait_for(1s) == std::future_status::ready,
           "publishing should wake a blocked consumer");
    expect(received.get()->body == "wake up", "consumer should receive the published message");
}

void wakesBlockedConsumerDuringShutdown() {
    mini_sqs::MessageQueue queue;
    std::promise<void> consumerStarted;
    auto started = consumerStarted.get_future();
    auto received = std::async(std::launch::async, [&queue, &consumerStarted] {
        consumerStarted.set_value();
        return queue.receive();
    });

    started.wait();
    expect(received.wait_for(50ms) == std::future_status::timeout,
           "consumer should block before shutdown");

    queue.shutdown();

    expect(received.wait_for(1s) == std::future_status::ready,
           "shutdown should wake a blocked consumer");
    expect(!received.get().has_value(), "shutdown should return no message");
}

}  // namespace

int main() {
    try {
        deliversMessagesInPublishOrder();
        wakesBlockedConsumerWhenMessageIsPublished();
        wakesBlockedConsumerDuringShutdown();
        std::cout << "message queue tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "message queue tests failed: " << error.what() << '\n';
        return 1;
    }
}