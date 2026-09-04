#include "queue/message_queue.h"

#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

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
    expect(first->message.body == "first", "queue should preserve FIFO order");
    expect(second->message.body == "second", "queue should preserve FIFO order");
    expect(first->message.receiveCount == 1, "receiving should increment the receive count");
    expect(!first->receiptHandle.empty(), "delivery should contain a receipt handle");
    expect(first->receiptHandle != second->receiptHandle, "receipt handles should be unique");
        expect(first->receiptHandle.starts_with(first->message.messageId + "-delivery-1-"),
            "first receipt handle should contain delivery version 1");
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
    expect(received.get()->message.body == "wake up", "consumer should receive the published message");
}

void acknowledgesOnlyInFlightMessages() {
    mini_sqs::MessageQueue queue;
    queue.publish(mini_sqs::Message::create("acknowledge me"));

    const auto delivery = queue.receive();

    expect(delivery.has_value(), "published message should be delivered");
    expect(!queue.acknowledge("unknown"), "unknown receipt handle should not acknowledge");
    expect(queue.acknowledge(delivery->receiptHandle), "valid receipt handle should acknowledge");
    expect(!queue.acknowledge(delivery->receiptHandle), "receipt handle should be single use");
}

void keepsUnacknowledgedMessagesInFlight() {
    mini_sqs::MessageQueue queue;
    queue.publish(mini_sqs::Message::create("in flight"));
    const auto delivery = queue.receive();
    auto nextReceive = std::async(std::launch::async, [&queue] {
        return queue.receive();
    });

    expect(delivery.has_value(), "published message should be delivered");
    expect(nextReceive.wait_for(50ms) == std::future_status::timeout,
           "unacknowledged message should not remain available");

    queue.shutdown();
    expect(!nextReceive.get().has_value(), "shutdown should wake the waiting receiver");
}

void redeliversAfterVisibilityTimeout() {
    mini_sqs::MessageQueue queue(30ms);
    queue.publish(mini_sqs::Message::create("try again"));

    const auto first = queue.receive();
    std::this_thread::sleep_for(60ms);

    expect(!queue.acknowledge(first->receiptHandle),
           "reaper should invalidate an expired receipt without another receive call");
    const auto second = queue.receive();

    expect(first.has_value() && second.has_value(), "unacknowledged message should be redelivered");
    expect(second->message.messageId == first->message.messageId,
           "redelivery should preserve the message ID");
    expect(second->message.receiveCount == 2, "redelivery should increment receive count");
    expect(second->receiptHandle != first->receiptHandle,
           "redelivery should issue a new receipt handle");
        expect(second->receiptHandle.starts_with(second->message.messageId + "-delivery-2-"),
            "redelivery receipt handle should contain delivery version 2");
        expect(!queue.acknowledge(first->receiptHandle),
            "old delivery version must not acknowledge the current retry");
    expect(queue.acknowledge(second->receiptHandle), "current receipt handle should acknowledge");
}

void acknowledgementWinsBeforeVisibilityTimeout() {
    mini_sqs::MessageQueue queue(100ms);
    queue.publish(mini_sqs::Message::create("finish in time"));
    const auto delivery = queue.receive();

    std::this_thread::sleep_for(20ms);
    expect(queue.acknowledge(delivery->receiptHandle),
           "acknowledgement before the deadline should succeed");

    auto nextReceive = std::async(std::launch::async, [&queue] {
        return queue.receive();
    });
    expect(nextReceive.wait_for(120ms) == std::future_status::timeout,
           "acknowledged message should not be requeued at its old deadline");

    queue.shutdown();
    expect(!nextReceive.get().has_value(), "shutdown should wake the waiting receiver");
}

    void movesMessagesToDeadLetterQueueAfterMaximumReceives() {
        mini_sqs::MessageQueue queue(20ms, 2);
        queue.publish(mini_sqs::Message::create("cannot process"));

        const auto first = queue.receive();
        const auto second = queue.receive();
        std::this_thread::sleep_for(50ms);
        const auto deadLetters = queue.deadLetterMessages();

        expect(first.has_value() && second.has_value(), "message should receive both allowed attempts");
        expect(first->message.messageId == second->message.messageId,
            "retry should preserve the original message");
        expect(second->message.receiveCount == 2, "second delivery should be the final allowed attempt");
        expect(deadLetters.size() == 1, "exhausted message should move to the dead-letter queue");
        expect(deadLetters.front().messageId == first->message.messageId,
            "dead-letter queue should contain the exhausted message");
        expect(deadLetters.front().receiveCount == 2,
            "dead-letter message should retain its receive count");
        expect(!queue.acknowledge(second->receiptHandle),
            "receipt handle should expire when the message is dead-lettered");
    }

    void deduplicatesConcurrentPublishesByIdempotencyKey() {
        mini_sqs::MessageQueue queue;
        constexpr std::size_t producerCount = 24;
        std::vector<std::thread> producers;
        std::vector<std::string> returnedMessageIds;
        std::mutex resultsMutex;
        producers.reserve(producerCount);

        for (std::size_t index = 0; index < producerCount; ++index) {
            producers.emplace_back([&queue, &returnedMessageIds, &resultsMutex, index] {
                const auto result = queue.publish(mini_sqs::Message::create(
                    "attempt-" + std::to_string(index), "request-123"));
                std::lock_guard lock(resultsMutex);
                returnedMessageIds.push_back(result.message.messageId);
            });
        }
        for (auto& producer : producers) {
            producer.join();
        }

        expect(returnedMessageIds.size() == producerCount, "every producer should receive a result");
        expect(std::all_of(
                   returnedMessageIds.begin(), returnedMessageIds.end(),
                   [&returnedMessageIds](const std::string& messageId) {
                       return messageId == returnedMessageIds.front();
                   }),
               "concurrent retries should return the original message ID");

        const auto delivery = queue.receive();
        expect(delivery->message.messageId == returnedMessageIds.front(),
               "only the original accepted message should be delivered");

        auto extraReceive = std::async(std::launch::async, [&queue] {
            return queue.receive();
        });
        expect(extraReceive.wait_for(50ms) == std::future_status::timeout,
               "deduplicated retries should not enqueue extra messages");
        queue.shutdown();
        expect(!extraReceive.get().has_value(), "shutdown should wake the waiting receiver");
    }

    void acceptsIdempotencyKeyAgainAfterDeduplicationWindow() {
        mini_sqs::MessageQueue queue(1s, 5, 20ms);
        const auto first = queue.publish(mini_sqs::Message::create("first", "reusable-key"));
        std::this_thread::sleep_for(40ms);
        const auto second = queue.publish(mini_sqs::Message::create("second", "reusable-key"));

        expect(!first.deduplicated && !second.deduplicated,
               "expired idempotency key should be accepted as a new message");
        expect(first.message.messageId != second.message.messageId,
               "new deduplication window should create a new message ID");
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
        acknowledgesOnlyInFlightMessages();
        keepsUnacknowledgedMessagesInFlight();
        redeliversAfterVisibilityTimeout();
        acknowledgementWinsBeforeVisibilityTimeout();
        movesMessagesToDeadLetterQueueAfterMaximumReceives();
        deduplicatesConcurrentPublishesByIdempotencyKey();
        acceptsIdempotencyKeyAgainAfterDeduplicationWindow();
        wakesBlockedConsumerDuringShutdown();
        std::cout << "message queue tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "message queue tests failed: " << error.what() << '\n';
        return 1;
    }
}