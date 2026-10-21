cmake_minimum_required(VERSION 3.18)
project(McapTestPackage CXX)

find_package(mcap REQUIRED CONFIG)

add_executable(test_package test_package.cpp)
target_link_libraries(test_package PRIVATE mcap::mcap)
