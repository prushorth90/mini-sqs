#include "http/api.h"

#include "metrics/metrics_registry.h"
#include "queue/message.h"
#include "queue/queue_registry.h"
#include "simulator/consumer_simulator.h"
#include "simulator/load_test_runner.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <string_view>
#include <string>
#include <utility>

namespace mini_sqs::http {
namespace {

crow::response jsonResponse(int status, crow::json::wvalue body) {
    crow::response response(status, body.dump());
    response.set_header("Content-Type", "application/json");
    return response;
}

crow::json::wvalue messageJson(const Message& message) {
    const auto createdAt = std::chrono::duration_cast<std::chrono::milliseconds>(
        message.createdAt.time_since_epoch());

    crow::json::wvalue body;
    body["messageId"] = message.messageId;
    body["body"] = message.body;
    body["idempotencyKey"] = message.idempotencyKey;
    body["createdAt"] = createdAt.count();
    body["receiveCount"] = message.receiveCount;
    return body;
}

crow::json::wvalue deliveryJson(const Delivery& delivery) {
    auto body = messageJson(delivery.message);
    body["receiptHandle"] = delivery.receiptHandle;
    return body;
}

bool isValidQueueName(std::string_view queueName) {
    if (queueName.empty() || queueName.size() > 80) {
        return false;
    }
    return std::all_of(queueName.begin(), queueName.end(), [](unsigned char character) {
        return std::isalnum(character) || character == '-' || character == '_';
    });
}

}  // namespace

void configureRoutes(
    QueueApi& app,
    QueueRegistry& queues,
    ConsumerSimulator& simulator,
    LoadTestRunner& loadTests) {
    CROW_ROUTE(app, "/metrics")
    ([&queues] {
        crow::response response(200, queues.metrics()->prometheusText());
        response.set_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        return response;
    });

    CROW_ROUTE(app, "/queues")
        .methods(crow::HTTPMethod::POST)
    ([&queues](const crow::request& request) {
        const auto payload = crow::json::load(request.body);
        if (!payload || !payload.has("name") || payload["name"].t() != crow::json::type::String) {
            return jsonResponse(400, {{"error", "request body must contain a string field named name"}});
        }

        const std::string queueName = payload["name"].s();
        if (!isValidQueueName(queueName)) {
            return jsonResponse(400, {{"error", "queue name must use 1-80 letters, numbers, hyphens, or underscores"}});
        }

        std::uint32_t maxReceiveCount = 5;
        if (payload.has("maxReceiveCount")) {
            if (payload["maxReceiveCount"].t() != crow::json::type::Number
                || payload["maxReceiveCount"].nt() == crow::json::num_type::Floating_point
                || payload["maxReceiveCount"].i() < 1
                || payload["maxReceiveCount"].i() > 1'000) {
                return jsonResponse(400, {{"error", "maxReceiveCount must be an integer from 1 to 1000"}});
            }
            maxReceiveCount = static_cast<std::uint32_t>(payload["maxReceiveCount"].i());
        }

        std::int64_t visibilityTimeoutMs = 30'000;
        if (payload.has("visibilityTimeoutMs")) {
            if (payload["visibilityTimeoutMs"].t() != crow::json::type::Number
                || payload["visibilityTimeoutMs"].nt() == crow::json::num_type::Floating_point
                || payload["visibilityTimeoutMs"].i() < 1
                || payload["visibilityTimeoutMs"].i() > 43'200'000) {
                return jsonResponse(400, {{"error", "visibilityTimeoutMs must be an integer from 1 to 43200000"}});
            }
            visibilityTimeoutMs = payload["visibilityTimeoutMs"].i();
        }

        if (!queues.create(
                queueName, maxReceiveCount, std::chrono::milliseconds(visibilityTimeoutMs))) {
            return jsonResponse(409, {{"error", "queue already exists"}});
        }
        return jsonResponse(201, {
            {"name", queueName},
            {"maxReceiveCount", maxReceiveCount},
            {"visibilityTimeoutMs", visibilityTimeoutMs},
        });
    });

    CROW_ROUTE(app, "/queues")
        .methods(crow::HTTPMethod::GET)
    ([&queues] {
        crow::json::wvalue body;
        body["queues"] = queues.list();
        return jsonResponse(200, std::move(body));
    });

    CROW_ROUTE(app, "/simulator")
        .methods(crow::HTTPMethod::POST)
    ([&queues, &simulator](const crow::request& request) {
        const auto payload = crow::json::load(request.body);
        if (!payload
            || !payload.has("queue") || payload["queue"].t() != crow::json::type::String
            || !payload.has("consumerCount") || payload["consumerCount"].t() != crow::json::type::Number
            || !payload.has("processingDelayMs") || payload["processingDelayMs"].t() != crow::json::type::Number
            || !payload.has("failureProbability") || payload["failureProbability"].t() != crow::json::type::Number) {
            return jsonResponse(400, {{"error", "queue, consumerCount, processingDelayMs, and failureProbability are required"}});
        }

        const auto consumerCount = payload["consumerCount"].i();
        const auto processingDelayMs = payload["processingDelayMs"].i();
        const auto failureProbability = payload["failureProbability"].d();
        if (consumerCount < 1 || consumerCount > 500) {
            return jsonResponse(400, {{"error", "consumerCount must be from 1 to 500"}});
        }
        if (processingDelayMs < 0 || processingDelayMs > 600'000) {
            return jsonResponse(400, {{"error", "processingDelayMs must be from 0 to 600000"}});
        }
        if (failureProbability < 0 || failureProbability > 1) {
            return jsonResponse(400, {{"error", "failureProbability must be from 0 to 1"}});
        }

        const std::string queueName = payload["queue"].s();
        const auto queue = queues.find(queueName);
        if (!queue) {
            return jsonResponse(404, {{"error", "queue not found"}});
        }
        simulator.start(queueName, queue, {
            .consumerCount = static_cast<std::uint32_t>(consumerCount),
            .processingDelay = std::chrono::milliseconds(processingDelayMs),
            .failureProbability = failureProbability,
        });
        return jsonResponse(200, {{"running", true}});
    });

    CROW_ROUTE(app, "/simulator")
        .methods(crow::HTTPMethod::GET)
    ([&simulator] {
        const auto state = simulator.snapshot();
        return jsonResponse(200, {
            {"running", state.running},
            {"queue", state.queueName},
            {"consumerCount", state.config.consumerCount},
            {"processingDelayMs", state.config.processingDelay.count()},
            {"failureProbability", state.config.failureProbability},
            {"received", state.received},
            {"acknowledged", state.acknowledged},
            {"failed", state.failed},
        });
    });

    CROW_ROUTE(app, "/simulator")
        .methods(crow::HTTPMethod::DELETE)
    ([&simulator] {
        simulator.stop();
        return crow::response(204);
    });

    CROW_ROUTE(app, "/load-tests")
        .methods(crow::HTTPMethod::POST)
    ([&loadTests](const crow::request& request) {
        const auto payload = crow::json::load(request.body);
        if (!payload
            || !payload.has("queue") || payload["queue"].t() != crow::json::type::String
            || !payload.has("messageCount") || payload["messageCount"].t() != crow::json::type::Number
            || !payload.has("producerCount") || payload["producerCount"].t() != crow::json::type::Number
            || !payload.has("consumerCount") || payload["consumerCount"].t() != crow::json::type::Number
            || !payload.has("processingDelayMs") || payload["processingDelayMs"].t() != crow::json::type::Number
            || !payload.has("failureProbability") || payload["failureProbability"].t() != crow::json::type::Number
            || !payload.has("visibilityTimeoutMs") || payload["visibilityTimeoutMs"].t() != crow::json::type::Number
            || !payload.has("maxReceiveCount") || payload["maxReceiveCount"].t() != crow::json::type::Number) {
            return jsonResponse(400, {{"error", "all load-test configuration fields are required"}});
        }

        const std::string queueName = payload["queue"].s();
        const auto messageCount = payload["messageCount"].i();
        const auto producerCount = payload["producerCount"].i();
        const auto consumerCount = payload["consumerCount"].i();
        const auto processingDelayMs = payload["processingDelayMs"].i();
        const auto failureProbability = payload["failureProbability"].d();
        const auto visibilityTimeoutMs = payload["visibilityTimeoutMs"].i();
        const auto maxReceiveCount = payload["maxReceiveCount"].i();
        if (!isValidQueueName(queueName)) {
            return jsonResponse(400, {{"error", "queue name must use 1-80 letters, numbers, hyphens, or underscores"}});
        }
        if (messageCount < 1'000 || messageCount > 50'000) {
            return jsonResponse(400, {{"error", "messageCount must be from 1000 to 50000"}});
        }
        if (producerCount < 1 || producerCount > 100
            || consumerCount < 1 || consumerCount > 500) {
            return jsonResponse(400, {{"error", "producerCount must be 1-100 and consumerCount must be 1-500"}});
        }
        if (processingDelayMs < 0 || processingDelayMs > 600'000
            || visibilityTimeoutMs < 1 || visibilityTimeoutMs > 43'200'000
            || maxReceiveCount < 1 || maxReceiveCount > 1'000
            || failureProbability < 0 || failureProbability > 1) {
            return jsonResponse(400, {{"error", "load-test configuration is outside allowed ranges"}});
        }

        const bool started = loadTests.start({
            .queueName = queueName,
            .messageCount = static_cast<std::uint32_t>(messageCount),
            .producerCount = static_cast<std::uint32_t>(producerCount),
            .consumers = {
                .consumerCount = static_cast<std::uint32_t>(consumerCount),
                .processingDelay = std::chrono::milliseconds(processingDelayMs),
                .failureProbability = failureProbability,
            },
            .visibilityTimeout = std::chrono::milliseconds(visibilityTimeoutMs),
            .maxReceiveCount = static_cast<std::uint32_t>(maxReceiveCount),
        });
        if (!started) {
            return jsonResponse(409, {{"error", "queue already exists; use a unique load-test queue name"}});
        }
        return jsonResponse(202, {{"status", "publishing"}, {"queue", queueName}});
    });

    CROW_ROUTE(app, "/load-tests")
        .methods(crow::HTTPMethod::GET)
    ([&loadTests] {
        const auto state = loadTests.snapshot();
        return jsonResponse(200, {
            {"status", state.status},
            {"queue", state.config.queueName},
            {"messageCount", state.config.messageCount},
            {"producerCount", state.config.producerCount},
            {"consumerCount", state.config.consumers.consumerCount},
            {"processingDelayMs", state.config.consumers.processingDelay.count()},
            {"failureProbability", state.config.consumers.failureProbability},
            {"visibilityTimeoutMs", state.config.visibilityTimeout.count()},
            {"maxReceiveCount", state.config.maxReceiveCount},
            {"published", state.published},
            {"completed", state.completed},
            {"retried", state.retried},
            {"deadLettered", state.deadLettered},
            {"peakThroughput", state.peakThroughput},
            {"durationMs", state.durationMs},
            {"error", state.error},
        });
    });

    CROW_ROUTE(app, "/load-tests")
        .methods(crow::HTTPMethod::DELETE)
    ([&loadTests] {
        loadTests.stop();
        return crow::response(204);
    });

    CROW_ROUTE(app, "/queues/<string>/messages")
        .methods(crow::HTTPMethod::POST)
    ([&queues](const crow::request& request, const std::string& queueName) {
        const auto payload = crow::json::load(request.body);
        if (!payload || !payload.has("body") || payload["body"].t() != crow::json::type::String) {
            return jsonResponse(400, {{"error", "request body must contain a string field named body"}});
        }

        std::string idempotencyKey;
        if (payload.has("idempotencyKey")) {
            if (payload["idempotencyKey"].t() != crow::json::type::String) {
                return jsonResponse(400, {{"error", "idempotencyKey must be a string"}});
            }
            idempotencyKey = payload["idempotencyKey"].s();
        }

        const auto queue = queues.find(queueName);
        if (!queue) {
            return jsonResponse(404, {{"error", "queue not found"}});
        }

        auto message = Message::create(payload["body"].s(), std::move(idempotencyKey));
        const auto result = queue->publish(std::move(message));
        auto response = messageJson(result.message);
        response["deduplicated"] = result.deduplicated;
        return jsonResponse(result.deduplicated ? 200 : 201, std::move(response));
    });

    CROW_ROUTE(app, "/queues/<string>/messages")
        .methods(crow::HTTPMethod::GET)
    ([&queues](const std::string& queueName) {
        const auto queue = queues.find(queueName);
        if (!queue) {
            return jsonResponse(404, {{"error", "queue not found"}});
        }

        const auto delivery = queue->receive();
        if (!delivery.has_value()) {
            return crow::response(204);
        }
        return jsonResponse(200, deliveryJson(*delivery));
    });

    CROW_ROUTE(app, "/queues/<string>/messages/<string>")
        .methods(crow::HTTPMethod::DELETE)
    ([&queues](const std::string& queueName, const std::string& receiptHandle) {
        const auto queue = queues.find(queueName);
        if (!queue) {
            return jsonResponse(404, {{"error", "queue not found"}});
        }
        if (!queue->acknowledge(receiptHandle)) {
            return jsonResponse(404, {{"error", "receipt handle not found"}});
        }
        return crow::response(204);
    });

    CROW_ROUTE(app, "/queues/<string>/dlq/messages")
        .methods(crow::HTTPMethod::GET)
    ([&queues](const std::string& queueName) {
        const auto queue = queues.find(queueName);
        if (!queue) {
            return jsonResponse(404, {{"error", "queue not found"}});
        }

        crow::json::wvalue::list messages;
        for (const auto& message : queue->deadLetterMessages()) {
            messages.push_back(messageJson(message));
        }
        crow::json::wvalue body;
        body["messages"] = std::move(messages);
        return jsonResponse(200, std::move(body));
    });

    CROW_ROUTE(app, "/queues/<string>/messages/recent")
        .methods(crow::HTTPMethod::GET)
    ([&queues](const std::string& queueName) {
        const auto queue = queues.find(queueName);
        if (!queue) {
            return jsonResponse(404, {{"error", "queue not found"}});
        }

        crow::json::wvalue::list messages;
        for (const auto& snapshot : queue->recentMessages()) {
            auto message = messageJson(snapshot.message);
            message["status"] = snapshot.status;
            messages.push_back(std::move(message));
        }
        crow::json::wvalue body;
        body["messages"] = std::move(messages);
        return jsonResponse(200, std::move(body));
    });
}

}  // namespace mini_sqs::http