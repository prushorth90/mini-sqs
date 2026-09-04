#include "http/api.h"
#include "queue/queue_registry.h"

#include <crow.h>

#include <cstdlib>
#include <filesystem>
#include <iostream>

int main() {
    const char* configuredLogPath = std::getenv("MINI_SQS_LOG_PATH");
    const std::filesystem::path logPath = configuredLogPath
        ? configuredLogPath
        : "data/queue.log";
    mini_sqs::QueueRegistry queues(logPath);
    mini_sqs::http::QueueApi app;
    mini_sqs::http::configureRoutes(app, queues);

    std::cout << "mini-sqs backend started on port 8080" << std::endl;
    app.port(8080).multithreaded().run();
}
