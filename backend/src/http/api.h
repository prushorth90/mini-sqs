#pragma once

#include <crow.h>
#include <crow/middlewares/cors.h>

namespace mini_sqs {

class QueueRegistry;
class ConsumerSimulator;
class LoadTestRunner;

namespace http {

using QueueApi = crow::App<crow::CORSHandler>;

void configureRoutes(
	QueueApi& app,
	QueueRegistry& queues,
	ConsumerSimulator& simulator,
	LoadTestRunner& loadTests);

}  // namespace http
}  // namespace mini_sqs