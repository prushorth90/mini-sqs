#pragma once

#include <crow.h>
#include <crow/middlewares/cors.h>

namespace mini_sqs {

class QueueRegistry;

namespace http {

using QueueApi = crow::App<crow::CORSHandler>;

void configureRoutes(QueueApi& app, QueueRegistry& queues);

}  // namespace http
}  // namespace mini_sqs