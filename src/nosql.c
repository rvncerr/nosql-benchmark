#include "nosql-benchmark.h"

nosql_t *nosql_connect(unsigned int type, const char *host, const char *port, unsigned int no, ...) {
	// Allocating.
	nosql_t *nosql = malloc(sizeof(nosql_t));
	if(!nosql) {
		return NULL;
	}
	nosql->type = type;
	nosql->no = no;

	// Resolving.
	struct addrinfo hints, *servinfo;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	if(getaddrinfo(host, port, &hints, &servinfo)) {
		free(nosql);
		return NULL;
	}

	// Connecting.
	nosql->socket_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(nosql->socket_fd == -1) {
		freeaddrinfo(servinfo);
		free(nosql);
		return NULL;
	}
	if(connect(nosql->socket_fd, servinfo->ai_addr, servinfo->ai_addrlen)) {
		freeaddrinfo(servinfo);
		close(nosql->socket_fd);
		free(nosql);
		return NULL;
	}

	// Creating epoll.
	nosql->epoll_fd = epoll_create1(0);
	struct epoll_event event;
	event.data.fd = nosql->socket_fd;
	event.events = EPOLLIN;
	epoll_ctl(nosql->epoll_fd, EPOLL_CTL_ADD, nosql->socket_fd, &event);

	// Customization. Initializing.
	if(type == NOSQL_TYPE_TARANTOOL) {
		/*va_list a_list;
		va_start(a_list, no);
		nosql->tarantool.space_id = va_arg(a_list, unsigned int);
		va_end(a_list);*/
		nosql->tarantool.space_id = 512; 

		nosql_tarantool_connect(nosql);
	} else if (type == NOSQL_TYPE_REDIS) {
		nosql_redis_connect(nosql);
	} else if (type == NOSQL_TYPE_MEMCACHED) {
		nosql_memcached_connect(nosql);	
	}

	return nosql;
}

void nosql_read(nosql_t *nosql, const unsigned int affect_stat) {
	switch(nosql->type) {
	case NOSQL_TYPE_TARANTOOL:
		nosql_tarantool_read(nosql, affect_stat);
		break;
	case NOSQL_TYPE_REDIS:
		nosql_redis_read(nosql, affect_stat);
		break;
	case NOSQL_TYPE_MEMCACHED:
		nosql_memcached_read(nosql, affect_stat);
		break;
	}
}

void *nosql_auto_read_routine(void *arg) {
	nosql_t *nosql = (nosql_t*)arg;
	switch(nosql->type) {
	case NOSQL_TYPE_TARANTOOL:
		while(1) nosql_tarantool_read(nosql, 1); 
	case NOSQL_TYPE_REDIS:
		while(1) nosql_redis_read(nosql, 1); 
	case NOSQL_TYPE_MEMCACHED:
		while(1) nosql_memcached_read(nosql, 1); 
	}

}

void nosql_auto_read(nosql_t *nosql) {
	pthread_create(&nosql->thread_ar, NULL, nosql_auto_read_routine, nosql);
}

void nosql_latency(nosql_t *nosql) {
	unsigned int thread_rand = rand() % config.thread_count;
	unsigned int iteration_rand = rand() % config.iteration_count;
	unsigned int batch_rand = 0;
	char key[32]; sprintf(key, "%05u_%03u_%010u_%04u", pid, thread_rand, iteration_rand, batch_rand);

	struct timeval start, stop, diff;
	gettimeofday(&start, NULL);
	nosql_select(nosql, key, strlen(key), 0);
	nosql_read(nosql, 0);
	gettimeofday(&stop, NULL);
	timersub(&stop, &start, &diff);

	unsigned int diff_num = diff.tv_sec*1e6 + diff.tv_usec;

	__sync_add_and_fetch(&stat.latency.hard.count, 1);
	__sync_add_and_fetch(&stat.latency.soft.count, 1);
	__sync_add_and_fetch(&stat.latency.hard.time, diff_num);
	__sync_add_and_fetch(&stat.latency.soft.time, diff_num);
}

void nosql_insert(nosql_t *nosql, unsigned int iteration_no, const char *value, unsigned int value_size) {
	char key[32]; sprintf(key, "%05u_%03u_%010u_0000", pid, nosql->no, iteration_no);
	switch(nosql->type) {
		case NOSQL_TYPE_TARANTOOL:
			nosql_tarantool_insert(nosql, key, strlen(key), value, value_size);
			break;
		case NOSQL_TYPE_REDIS:
			nosql_redis_insert(nosql, key, strlen(key), value, value_size); // why strlen?
			break;
		case NOSQL_TYPE_MEMCACHED:
			nosql_memcached_insert(nosql, iteration_no * 100 + nosql->no, key, strlen(key), value, value_size);
			break;
	}
}

void nosql_select(nosql_t *nosql, const char *key, unsigned int key_size, const unsigned int affect_stat) {
	switch(nosql->type) {
		case NOSQL_TYPE_TARANTOOL:
			nosql_tarantool_select(nosql, key, strlen(key), affect_stat);
			break;
		case NOSQL_TYPE_REDIS:
			nosql_redis_select(nosql, key, strlen(key), affect_stat); // why strlen use key_size
			break;
		case NOSQL_TYPE_MEMCACHED:
			nosql_memcached_select(nosql, 0, key, strlen(key), affect_stat);
			break;
	}
}

/*double nosql_latency(nosql_t *nosql, unsigned int thread_max, unsigned int iteration_max, unsigned int batch_max) {
  unsigned int thread_rand = thread_max ? rand() % thread_max : 0;
  unsigned int iteration_rand = iteration_max ? rand() % iteration_max : 0;
  unsigned int batch_rand = batch_max ? rand() % batch_max : 0;
  char key[32]; sprintf(key, "%05u_%03u_%010u_%04u", pid, thread_rand, iteration_rand, batch_rand);
  switch(nosql->type) {
  case NOSQL_TYPE_MEMCACHED:
  return nosql_memcached_latency(nosql, key, strlen(key));
  }
  }*/
