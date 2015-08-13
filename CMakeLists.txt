cmake_minimum_required(VERSION 2.8)
project(nosql-benchmark)
set(CMAKE_C_FLAGS "-std=gnu11 -ggdb -O0 -Iinclude -I/usr/local/include -L/usr/local/lib/x86_64-linux-gnu/ -pthread")

add_library(tarantool STATIC src/tarantool.c)
target_link_libraries(tarantool msgpuck)
add_library(redis STATIC src/redis.c)
add_library(memcached STATIC src/memcached.c)

add_library(nosql STATIC src/nosql.c)
target_link_libraries(nosql tarantool redis memcached msgpuck)

add_executable(nosql-benchmark src/main.c)
target_link_libraries(nosql-benchmark nosql msgpuck)
