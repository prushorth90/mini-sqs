cmake_minimum_required(VERSION 3.20)

project(mini_sqs_backend LANGUAGES CXX)

find_package(Threads REQUIRED)

add_library(mini-sqs-core
	src/queue/message.cpp
	src/queue/message_queue.cpp
)
target_compile_features(mini-sqs-core PUBLIC cxx_std_20)
target_include_directories(mini-sqs-core PUBLIC src)

add_executable(mini-sqs src/main.cpp)
target_compile_features(mini-sqs PRIVATE cxx_std_20)
target_link_libraries(mini-sqs PRIVATE mini-sqs-core)

include(CTest)

if(BUILD_TESTING)
	add_executable(message-tests tests/message_test.cpp)
	target_link_libraries(message-tests PRIVATE mini-sqs-core)
	add_test(NAME message-tests COMMAND message-tests)

	add_executable(message-queue-tests tests/message_queue_test.cpp)
	target_link_libraries(message-queue-tests PRIVATE mini-sqs-core Threads::Threads)
	add_test(NAME message-queue-tests COMMAND message-queue-tests)

	add_executable(message-queue-load-test tests/message_queue_load_test.cpp)
	target_link_libraries(message-queue-load-test PRIVATE mini-sqs-core Threads::Threads)
	add_test(NAME message-queue-load-test COMMAND message-queue-load-test)
	set_tests_properties(message-queue-load-test PROPERTIES TIMEOUT 30)
endif()
