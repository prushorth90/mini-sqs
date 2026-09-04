#include "http/api.h"

#include "queue/message.h"
#include "queue/queue_registry.h"

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

void configureRoutes(QueueApi& app, QueueRegistry& queues) {
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
        if (!queues.create(queueName)) {
            return jsonResponse(409, {{"error", "queue already exists"}});
        }
        return jsonResponse(201, {{"name", queueName}});
    });

    CROW_ROUTE(app, "/queues")
        .methods(crow::HTTPMethod::GET)
    ([&queues] {
        crow::json::wvalue body;
        body["queues"] = queues.list();
        return jsonResponse(200, std::move(body));
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
        queue->publish(message);
        return jsonResponse(201, messageJson(message));
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
}

}  // namespace mini_sqs::http