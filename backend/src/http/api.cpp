#include "http/api.h"

#include "queue/message.h"
#include "queue/queue_registry.h"

#include <chrono>
#include <cstdint>
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

}  // namespace

void configureRoutes(QueueApi& app, QueueRegistry& queues) {
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

        auto message = Message::create(payload["body"].s(), std::move(idempotencyKey));
        queues.getOrCreate(queueName)->publish(message);
        return jsonResponse(201, messageJson(message));
    });

    CROW_ROUTE(app, "/queues/<string>/messages")
        .methods(crow::HTTPMethod::GET)
    ([&queues](const std::string& queueName) {
        const auto message = queues.getOrCreate(queueName)->receive();
        if (!message.has_value()) {
            return crow::response(204);
        }
        return jsonResponse(200, messageJson(*message));
    });

    CROW_ROUTE(app, "/queues/<string>/messages/<string>")
        .methods(crow::HTTPMethod::DELETE)
    ([](const std::string&, const std::string&) {
        return jsonResponse(501, {{"error", "message acknowledgement is not implemented yet"}});
    });
}

}  // namespace mini_sqs::http