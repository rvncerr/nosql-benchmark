#include "nosql-benchmark.h"

#include <fcntl.h>

int set_nonblock(int fd)
{
	int flags;
#if defined(O_NONBLOCK)
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#else
	flags = 1;
	return ioctl(fd, FIOBIO, &flags);
#endif
} 

void nosql_redis_connect(nosql_t *nosql) {
}

void nosql_redis_insert(nosql_t *nosql, const char *key, unsigned int key_size, const char *value, unsigned int value_size) {
	char request[1024];
	sprintf(request, "SET \"%s\" \"%s\"\r\n", key, value);
	send(nosql->socket_fd, &request, strlen(request), MSG_NOSIGNAL);
	__sync_add_and_fetch(&stat.set.send, 1);
}

void nosql_redis_select(nosql_t *nosql, const char *key, unsigned int key_size, const unsigned int affect_stat) {
	char request[1024];
	sprintf(request, "GET \"%s\"\r\n", key);
	send(nosql->socket_fd, &request, strlen(request), MSG_NOSIGNAL);
	if(affect_stat) __sync_add_and_fetch(&stat.get.send, 1);
}

void nosql_redis_read(nosql_t *nosql, const unsigned int affect_stat) {
	struct epoll_event event;
	if(epoll_wait(nosql->epoll_fd, &event, 1, -1)) {
		char buffer[1048576];
		int recv_result = recv(nosql->socket_fd, buffer, 1048576, MSG_NOSIGNAL);
		if(recv_result > 0) {
			unsigned int plus = 0;
			unsigned int dollar = 0;
			for(unsigned int i = 0; i < recv_result; i++) {
				if(buffer[i] == '+') plus++;
				if(buffer[i] == '$') dollar++;
			}
			if(affect_stat) {
				__sync_add_and_fetch(&stat.set.recv, plus);
				__sync_add_and_fetch(&stat.get.recv, dollar);
			}
		}
	}

}
