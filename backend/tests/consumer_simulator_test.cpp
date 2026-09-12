#include "queue/message.h"
#include "queue/message_queue.h"
#include "simulator/consumer_simulator.h"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <thread>

namespace {

using namespace std::chrono_literals;

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

}  // namespace

int main() {
    try {
        auto queue = std::make_shared<mini_sqs::MessageQueue>();
        for (int index = 0; index < 10; ++index) {
            queue->publish(mini_sqs::Message::create("message-" + std::to_string(index)));
        }

        mini_sqs::ConsumerSimulator simulator;
        simulator.start("load-test", queue, {
            .consumerCount = 3,
            .processingDelay = 2ms,
            .failureProbability = 0,
        });

        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (simulator.snapshot().acknowledged < 10
               && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(10ms);
        }
        simulator.stop();

        const auto result = simulator.snapshot();
        expect(!result.running, "simulator should report stopped");
        expect(result.queueName == "load-test", "simulator should report its queue");
        expect(result.received == 10, "simulator should receive every message");
        expect(result.acknowledged == 10, "successful processing should acknowledge messages");
        expect(result.failed == 0, "zero failure probability should not fail messages");
        std::cout << "consumer simulator tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "consumer simulator tests failed: " << error.what() << '\n';
        return 1;
    }
}