cmake_minimum_required(VERSION 3.20)

project(mini_sqs_backend LANGUAGES CXX)

include(FetchContent)

find_package(Threads REQUIRED)

FetchContent_Declare(
	Crow
	GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
	GIT_TAG v1.3.3
	GIT_SHALLOW TRUE
)
set(CROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CROW_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(Crow)

add_library(mini-sqs-core
	src/queue/message.cpp
	src/queue/message_queue.cpp
	src/queue/queue_registry.cpp
)
target_compile_features(mini-sqs-core PUBLIC cxx_std_20)
target_include_directories(mini-sqs-core PUBLIC src)

add_library(mini-sqs-http
	src/http/api.cpp
)
target_compile_features(mini-sqs-http PUBLIC cxx_std_20)
target_include_directories(mini-sqs-http PUBLIC src)
target_link_libraries(mini-sqs-http PUBLIC mini-sqs-core Crow::Crow)

add_executable(mini-sqs
	src/main.cpp
)
target_compile_features(mini-sqs PRIVATE cxx_std_20)
target_link_libraries(mini-sqs PRIVATE mini-sqs-http Threads::Threads)

include(CTest)

if(BUILD_TESTING)
	add_executable(message-tests tests/message_test.cpp)
	target_link_libraries(message-tests PRIVATE mini-sqs-core)
	add_test(NAME message-tests COMMAND message-tests)

	add_executable(message-queue-tests tests/message_queue_test.cpp)
	target_link_libraries(message-queue-tests PRIVATE mini-sqs-core Threads::Threads)
	add_test(NAME message-queue-tests COMMAND message-queue-tests)

	add_executable(queue-registry-tests tests/queue_registry_test.cpp)
	target_link_libraries(queue-registry-tests PRIVATE mini-sqs-core Threads::Threads)
	add_test(NAME queue-registry-tests COMMAND queue-registry-tests)

	add_executable(message-queue-load-test tests/message_queue_load_test.cpp)
	target_link_libraries(message-queue-load-test PRIVATE mini-sqs-core Threads::Threads)
	add_test(NAME message-queue-load-test COMMAND message-queue-load-test)
	set_tests_properties(message-queue-load-test PROPERTIES TIMEOUT 30)
endif()
