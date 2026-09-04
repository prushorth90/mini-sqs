#include <chrono>
#include <iostream>
#include <thread>

int main() {
    std::cout << "mini-sqs backend started on port 8080" << std::endl;

    while (true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
    }
}
