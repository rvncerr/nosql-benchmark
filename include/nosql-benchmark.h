#ifndef NOSQL_BENCHMARK_H
#define NOSQL_BENCHMARK_H

#include <stdlib.h>     // malloc, free.
#include <string.h>     // memset.
#include <sys/types.h>  // getaddrinfo, socket.
#include <sys/socket.h> // getaddrinfo, socket.
#include <netdb.h>      // getaddrinfo.
#include <unistd.h>     // close.
#include <stdio.h>      // sprintf.
#include <pthread.h>    // pthread_create.
#include <sys/epoll.h>  // epoll_create, epoll_ctl, epoll_wait.
#include <stdarg.h>     // va_list.

#include <msgpuck/msgpuck.h>

#define NOSQL_TYPE_TARANTOOL 0
#define NOSQL_TYPE_REDIS     1
#define NOSQL_TYPE_MEMCACHED 2
#define NOSQL_TYPE_DYNAMODB  3
#define NOSQL_TYPE_AEROSPIKE 4

typedef struct nosql_s {
	unsigned int type;
	unsigned int no;
	int socket_fd;
	int epoll_fd;
	pthread_t thread_ar;
	union {
		struct {
			unsigned int space_id;
			struct {
				struct { unsigned int send; unsigned int recv; } set;
				struct { unsigned int send; unsigned int recv; } get;
			} stat; 
		} tarantool;
		struct {
			unsigned int database;
			struct {
				struct { unsigned int send; unsigned int recv; } set;
				struct { unsigned int send; unsigned int recv; } get;
			} stat; 
		} redis;
		struct { } memcached;
		struct { } dynamodb;
		struct { } aerospike;
	};
} nosql_t;

nosql_t *nosql_connect(unsigned int type, const char *host, const char *port, unsigned int no, ...);
void nosql_auto_read(nosql_t *nosql);
void nosql_insert(nosql_t *nosql, unsigned int iteration_no, const char *value, unsigned int value_size);
void nosql_select(nosql_t *nosql, unsigned int iteration_no);
void nosql_read(nosql_t *nosql, const unsigned int affect_stat);
void *nosql_auto_read_routine(void *arg); 

// Tarantool.

void nosql_tarantool_connect(nosql_t *nosql);
void nosql_tarantool_insert(nosql_t *nosql, const char *key, unsigned int key_size, const char *value, unsigned int value_size);
void nosql_tarantool_select(nosql_t *nosql, const char *key, unsigned int key_size);
void nosql_tarantool_read(nosql_t *nosql, const unsigned int affect_stat);

// Redis.

void nosql_redis_connect(nosql_t *nosql);
void nosql_redis_insert(nosql_t *nosql, const char *key, unsigned int key_size, const char *value, unsigned int value_size);
void nosql_redis_select(nosql_t *nosql, const char *key, unsigned int key_size); 
void nosql_redis_read(nosql_t *nosql, const unsigned int affect_stat);

// Memcached.

typedef struct memcached_header_s {
	uint8_t magic;
	uint8_t opcode;
	uint16_t key_length;
	uint8_t extras_length;
	uint8_t data_type;
	union { uint16_t vbucket_id; uint16_t status; };
	uint32_t total_body_length;
	uint32_t opaque;
	uint64_t cas;
} __attribute__((packed)) memcached_header_t;
typedef struct memcached_set_request_s {
	memcached_header_t header;
	struct { uint32_t flags; uint32_t expiration; } __attribute__((packed)) extras;
} __attribute__((packed)) memcached_set_request_t;
typedef struct memcached_get_request_s {
	memcached_header_t header;
} __attribute__((packed)) memcached_get_request_t;
#define MEMCACHED_FILL_SET_REQUEST(r, opaque, key_size, value_size) \
	r.header.magic = 0x80; \
	r.header.opcode = 0x01; \
	r.header.key_length = htons(key_size); \
	r.header.extras_length = 0x08; \
	r.header.vbucket_id = 0x0000; \
	r.header.total_body_length = htonl(0x08 + key_size + value_size); \
	r.header.opaque = htonl(opaque); \
	r.header.cas = 0x0000000000000000; \
	r.extras.flags = 0x00000000; \
	r.extras.expiration = 0x00000000;
#define MEMCACHED_FILL_GET_REQUEST(r, opaque, key_size) \
	r.header.magic = 0x80; \
	r.header.opcode = 0x00; \
	r.header.key_length = htons(key_size); \
	r.header.extras_length = 0x00; \
	r.header.vbucket_id = 0x0000; \
	r.header.total_body_length = htonl(key_size); \
	r.header.opaque = htonl(opaque); \
	r.header.cas = 0x0000000000000000;

void nosql_memcached_connect(nosql_t *nosql);
void nosql_memcached_insert(nosql_t *nosql, unsigned int opaque, const char *key, unsigned int key_size, const char *value, unsigned int value_size);
void nosql_memcached_select(nosql_t *nosql, unsigned int opaque, const char *key, unsigned int key_size);
void nosql_memcached_read(nosql_t *nosql, const unsigned int affect_stat);

// Config.

typedef struct config_s {
	unsigned int type;
	char host[256];
	char port[6];
	unsigned int database_id;
	unsigned int thread_count;
	unsigned int iteration_count;
} config_t;
extern config_t config;

// Stat.

typedef struct stat_s {
	struct { unsigned long long send; unsigned long long recv; } set;
	struct { unsigned long long send; unsigned long long recv; } get;
	struct { unsigned int count; double time; } latency;
	unsigned int error;
} stat_t;
extern stat_t stat;

// Pid.

extern pid_t pid;

#endif
