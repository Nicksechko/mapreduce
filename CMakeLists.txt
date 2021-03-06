cmake_minimum_required(VERSION 3.15)
project(MapReduce)

set(CMAKE_CXX_STANDARD 17)

#set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-fsanitize=address -fsanitize=leak -fsanitize=undefined")
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-fsanitize=thread")
set(CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} "-lboost_system -pthread")

find_package(Boost 1.65.1 COMPONENTS system filesystem REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(MapReduce main.cpp mapreduce.cpp mapreduce.h executor.cpp executor.h table_io.h table_io.cpp)
add_executable(MapScript map_script.cpp)
add_executable(ReduceScript reduce_script.cpp)

target_link_libraries(MapReduce ${Boost_LIBRARIES})