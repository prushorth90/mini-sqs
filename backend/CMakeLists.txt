cmake_minimum_required(VERSION 3.20)

project(mini_sqs_backend LANGUAGES CXX)

add_executable(mini-sqs src/main.cpp)
target_compile_features(mini-sqs PRIVATE cxx_std_20)
