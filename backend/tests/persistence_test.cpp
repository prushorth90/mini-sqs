#include "queue/queue_registry.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace {

using namespace std::chrono_literals;

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void recoversOutstandingMessages(const std::filesystem::path& logPath) {
    std::string acknowledgedId;
    std::string unacknowledgedId;
    {
        mini_sqs::QueueRegistry queues(logPath);
        queues.create("orders");
        const auto queue = queues.find("orders");
        queue->publish(mini_sqs::Message::create("acknowledged"));
        queue->publish(mini_sqs::Message::create("in-flight-at-restart"));
        queue->publish(mini_sqs::Message::create("still-available"));

        const auto acknowledged = queue->receive();
        acknowledgedId = acknowledged->message.messageId;
        expect(queue->acknowledge(acknowledged->receiptHandle), "acknowledgement should succeed");
        unacknowledgedId = queue->receive()->message.messageId;
    }

    mini_sqs::QueueRegistry recovered(logPath);
    const auto queue = recovered.find("orders");
    expect(queue != nullptr, "queue should be recreated from the log");
    const auto first = queue->receive();
    const auto second = queue->receive();
    expect(first->message.messageId == unacknowledgedId,
           "in-flight message should become available after restart");
    expect(first->message.messageId != acknowledgedId,
           "acknowledged message should not be recovered");
    expect(second->message.body == "still-available",
           "available message should survive restart");
    queue->acknowledge(first->receiptHandle);
    queue->acknowledge(second->receiptHandle);
}

void recoversRetryAndDeadLetterState(const std::filesystem::path& logPath) {
    {
        mini_sqs::QueueRegistry queues(logPath);
        queues.create("retries", 2, 10ms);
        const auto queue = queues.find("retries");
        queue->publish(mini_sqs::Message::create("retry-once"));
        queue->receive();
        const auto second = queue->receive();
        expect(second->message.receiveCount == 2, "message should be requeued once");
        queue->acknowledge(second->receiptHandle);

        queues.create("dead-letters", 1, 10ms);
        const auto deadLetterQueue = queues.find("dead-letters");
        deadLetterQueue->publish(mini_sqs::Message::create("poison"));
        deadLetterQueue->receive();
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (deadLetterQueue->deadLetterMessages().empty()
               && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        expect(deadLetterQueue->deadLetterMessages().size() == 1,
               "message should enter the dead-letter queue");
    }

    mini_sqs::QueueRegistry recovered(logPath);
    const auto deadLetters = recovered.find("dead-letters")->deadLetterMessages();
    expect(deadLetters.size() == 1 && deadLetters.front().body == "poison",
           "dead-letter state should survive restart");
}

}  // namespace

int main() {
    const auto logPath = std::filesystem::temp_directory_path()
        / ("mini-sqs-persistence-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) + ".log");
    try {
        recoversOutstandingMessages(logPath);
        recoversRetryAndDeadLetterState(logPath);

        std::ifstream input(logPath);
        const std::string log((std::istreambuf_iterator<char>(input)), {});
         expect(log.find("PUBLISH\t") != std::string::npos,
             "log should contain PUBLISH records");
         expect(log.find("RECEIVE\t") != std::string::npos,
             "log should contain RECEIVE records");
         expect(log.find("ACK\t") != std::string::npos,
             "log should contain ACK records");
         expect(log.find("REQUEUE\t") != std::string::npos,
             "log should contain REQUEUE records");
         expect(log.find("DLQ\t") != std::string::npos,
             "log should contain DLQ records");

        std::filesystem::remove(logPath);
        std::cout << "persistence tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::filesystem::remove(logPath);
        std::cerr << "persistence tests failed: " << error.what() << '\n';
        return 1;
    }
}