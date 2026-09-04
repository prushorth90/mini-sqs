#include "queue/message.h"
#include "queue/queue_registry.h"

#include <array>
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

namespace {

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void createsAndListsNamedQueues() {
    mini_sqs::QueueRegistry queues;

    expect(queues.create("orders"), "new queue should be created");
    expect(queues.create("image-processing"), "second queue should be created");
    expect(queues.create("notifications"), "third queue should be created");
    expect(!queues.create("orders"), "duplicate queue should not be created");
    expect(queues.find("missing") == nullptr, "missing queue should not be found");

    const std::vector<std::string> expected{
        "image-processing", "notifications", "orders"};
    expect(queues.list() == expected, "queue names should be listed in stable order");
}

void isolatesMessagesByQueueName() {
    mini_sqs::QueueRegistry queues;
    queues.create("orders");
    queues.create("notifications");

    queues.find("orders")->publish(mini_sqs::Message::create("order-created"));
    queues.find("notifications")->publish(mini_sqs::Message::create("email-user"));

        expect(queues.find("orders")->receive()->message.body == "order-created",
           "orders queue should contain only its message");
        expect(queues.find("notifications")->receive()->message.body == "email-user",
           "notifications queue should contain only its message");
}

void handlesConcurrentQueueCreation() {
    mini_sqs::QueueRegistry queues;
    constexpr std::array names{"orders", "image-processing", "notifications"};
    constexpr std::size_t threadCount = 24;
    std::atomic<std::size_t> created{0};
    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (std::size_t index = 0; index < threadCount; ++index) {
        threads.emplace_back([&queues, &created, &names, index] {
            if (queues.create(names[index % names.size()])) {
                created.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    expect(created.load(std::memory_order_relaxed) == names.size(),
           "exactly one thread should create each queue");
    expect(queues.list().size() == names.size(), "concurrent creation should not duplicate queues");
}

}  // namespace

int main() {
    try {
        createsAndListsNamedQueues();
        isolatesMessagesByQueueName();
        handlesConcurrentQueueCreation();
        std::cout << "queue registry tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "queue registry tests failed: " << error.what() << '\n';
        return 1;
    }
}