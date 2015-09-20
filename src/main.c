#include "nosql-benchmark.h"

#include <getopt.h>

config_t config;
stat_t stat;

pid_t pid;
nosql_t **nosql;
pthread_t *benchmark;
pthread_barrier_t barrier;

void *visual_routine(void *arg) {
	printf("n;set.send;set.recv;get.send;get.recv;latency.soft;latency.hard;error\n");
	unsigned int i = 0;
	while(1) {
		printf("%u;%llu;%llu;%llu;%llu;%f;%f;%u\n",
			i,
			stat.set.send,
			stat.set.recv,
			stat.get.send,
			stat.get.recv,
			stat.latency.soft.count ? 1e-6 * stat.latency.soft.time / stat.latency.soft.count : 0,
			stat.latency.hard.count ? 1e-6 * stat.latency.hard.time / stat.latency.hard.count : 0,
			stat.error);
		
		stat.latency.hard.time = 0;
		stat.latency.hard.count = 0;
		
		i++;
		sleep(1);
	}
}

void *latency_routine(void *arg) {
	nosql_t *nosql = (nosql_t*) arg;
	while(1) nosql_latency(nosql);
}

void *benchmark_routine(void *arg) {
	nosql_t *nosql = (nosql_t*) arg;
	for(unsigned int i = 0; i < config.iteration_count; i++) {
		nosql_insert(nosql, i, "XXXXXXXXXXXXXXXX", 16);
	}
	pthread_barrier_wait(&barrier);
	for(unsigned int i = 0; i < config.iteration_count; i++) {
		char key[32]; sprintf(key, "%05u_%03u_%010u_0000", pid, nosql->no, i);
		nosql_select(nosql, key, strlen(key), 1);
	}
}

int main(int argc, char **argv) {
	setbuf(stdout, NULL);

	strcpy(config.host, "localhost");
	strcpy(config.port, "11211");
	config.database_id = 512; // after specif
	config.thread_count = 128;
	config.iteration_count = 200000;
	config.type = NOSQL_TYPE_TARANTOOL;

	const struct option long_options[] = {
		{"type", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"database-id", required_argument, NULL, 'b'},
		{"thread-count", required_argument, NULL, 't'},
		{"iteration-count", required_argument, NULL, 'i'},
		{NULL, 0, NULL, 0}
	};

	int getopt_result, getopt_index;
	while((getopt_result = getopt_long_only(argc, argv, "", long_options, &getopt_index)) != -1) {
		switch(getopt_result) {
		case 'd':
			if(!strcmp(optarg, "tarantool")) {
				config.type = NOSQL_TYPE_TARANTOOL;
			}
			if(!strcmp(optarg, "redis")) {
				config.type = NOSQL_TYPE_REDIS;
			}
			if(!strcmp(optarg, "memcached")) {
				config.type = NOSQL_TYPE_MEMCACHED;
			}
		case 'h':
			strcpy(config.host, optarg);
			break;
		case 'p':
			strcpy(config.port, optarg);
			break;
		case 'b':
			sscanf(optarg, "%u", &config.database_id);
			break;
		case 't':
			sscanf(optarg, "%u", &config.thread_count);
			break;
		case 'i':
			sscanf(optarg, "%u", &config.iteration_count);
			break;
		}
	}

	printf("%d\n", config.type);


	pthread_t visual;
	pthread_create(&visual, NULL, visual_routine, NULL);

	nosql_t *nosql_for_latency = nosql_connect(config.type, config.host, config.port, 0);
	pthread_t latency;
	pthread_create(&latency, NULL, latency_routine, nosql_for_latency);

	pthread_barrier_init(&barrier, NULL, config.thread_count + 1);

	pid = getpid();
	nosql = malloc(config.thread_count * sizeof(nosql_t*));
	if(!nosql) { fprintf(stderr, "Cannot alloc connection array!\n"); return 1; }
	for(unsigned int i = 0; i < config.thread_count; i++) {
		if(config.type == NOSQL_TYPE_TARANTOOL) {
			nosql[i] = nosql_connect(config.type, config.host, config.port, i);
		} else {
			nosql[i] = nosql_connect(config.type, config.host, config.port, i);
		}
		if(!nosql[i]) {
			fprintf(stderr, "Cannot alloc connection! [%d]\n", i);
			return 1;
		}
		nosql_auto_read(nosql[i]);
	}

	benchmark = malloc(config.thread_count * sizeof(pthread_t));
	for(unsigned int i = 0; i < config.thread_count; i++) {
		pthread_create(&benchmark[i], NULL, benchmark_routine, nosql[i]);
	}

	struct {
		unsigned long long int last;
		unsigned int limit;
	} break_context;

	break_context.last = 0;
	break_context.limit = 10;
	while(1) {
		if(stat.set.recv >= config.thread_count * config.iteration_count) {
			break;
		}
		if(stat.set.recv == break_context.last) {
			if(break_context.limit) {
				break_context.limit--;
				sleep(1);
			} else {
				break;
			}
		} else {
			break_context.last = stat.set.recv;
			break_context.limit = 10;
		}
	}
	
	pthread_barrier_wait(&barrier);

	break_context.last = 0;
	break_context.limit = 10;
	while(1) {
		if(stat.get.recv >= config.thread_count * config.iteration_count) {
			break;
		}
		if((stat.get.recv == break_context.last)&&(break_context.last != 0)) {
			if(break_context.limit) {
				break_context.limit--;
				sleep(1);
			} else {
				break;
			}
		} else {
			break_context.last = stat.get.recv;
			break_context.limit = 10;
		}
	}

	return 0;
}
