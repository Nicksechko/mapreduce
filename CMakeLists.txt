cmake_minimum_required(VERSION 3.16)
project(MapReduce)

set(CMAKE_CXX_STANDARD 17)

add_executable(MapReduce main.cpp executor.cpp executor.h)