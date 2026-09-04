#include "http/api.h"
#include "queue/queue_registry.h"

#include <crow.h>

#include <iostream>

int main() {
    mini_sqs::QueueRegistry queues;
    mini_sqs::http::QueueApi app;
    mini_sqs::http::configureRoutes(app, queues);

    std::cout << "mini-sqs backend started on port 8080" << std::endl;
    app.port(8080).multithreaded().run();
}
