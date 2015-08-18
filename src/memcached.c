#include "nosql-benchmark.h"

void nosql_memcached_connect(nosql_t *nosql) {
}

void nosql_memcached_insert(nosql_t *nosql, unsigned int opaque, const char *key, unsigned int key_size, const char *value, unsigned int value_size) {
	memcached_set_request_t request;
	MEMCACHED_FILL_SET_REQUEST(request, opaque, key_size, value_size);
	send(nosql->socket_fd, &request, sizeof(request), MSG_NOSIGNAL);
	send(nosql->socket_fd, key, key_size, MSG_NOSIGNAL);
	send(nosql->socket_fd, value, value_size, MSG_NOSIGNAL);
	__sync_add_and_fetch(&stat.set.send, 1);
}

void nosql_memcached_select(nosql_t *nosql, unsigned int opaque, const char *key, unsigned int key_size, const unsigned int affect_stat) {
	memcached_get_request_t request;
	MEMCACHED_FILL_GET_REQUEST(request, opaque, key_size);
	send(nosql->socket_fd, &request, sizeof(request), MSG_NOSIGNAL);
	send(nosql->socket_fd, key, key_size, MSG_NOSIGNAL);
	if(affect_stat) __sync_add_and_fetch(&stat.get.send, 1);
}

void nosql_memcached_read(nosql_t *nosql, const unsigned int affect_stat) {
	struct epoll_event event;
	if(epoll_wait(nosql->epoll_fd, &event, 1, -1)) {
		memcached_header_t header;
		unsigned int header_offset = 0;
		while(header_offset < sizeof(header)) {
			int recv_result = recv(nosql->socket_fd, &header + header_offset, sizeof(header) - header_offset, MSG_NOSIGNAL);
			if(recv_result > 0) header_offset += recv_result;
		}

		char buffer[1048576];
		unsigned int to_read = ntohl(header.total_body_length);
		while(to_read) {
			int recv_result = recv(nosql->socket_fd, buffer, to_read /* min to_read , 1Mb */, MSG_NOSIGNAL);
			if(recv_result > 0) to_read -= recv_result;
		}

		if(affect_stat) {
			if(header.opcode == 0x01) __sync_add_and_fetch(&stat.set.recv, 1);
			if(header.opcode == 0x00) __sync_add_and_fetch(&stat.get.recv, 1);
			if(ntohs(header.status)) __sync_add_and_fetch(&stat.error, 1);
		}
	}
}
