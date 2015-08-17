#include "nosql-benchmark.h"

void nosql_tarantool_connect(nosql_t *nosql) {
	char buffer[128];
	unsigned int read_count = 0;
	while(read_count < 128) {
		int read_result = recv(nosql->socket_fd, buffer + read_count, 128 - read_count, MSG_NOSIGNAL);
		if(read_result > 0) read_count += read_result;
	}
}

void nosql_tarantool_insert(nosql_t *nosql, const char *key, unsigned int key_size, const char *value, unsigned int value_size) {
	char buffer[1048576], *pbh, *pb;
	pbh = buffer; pb = pbh + 5;

	pb = mp_encode_map   (pb, 2); 
	pb = mp_encode_uint  (pb, 0x00); // code
	pb = mp_encode_uint  (pb, 0x02); // insert
	pb = mp_encode_uint  (pb, 0x01); // sync
	pb = mp_encode_uint  (pb, 0x01); // "yes"
	pb = mp_encode_map   (pb, 2);
	pb = mp_encode_uint  (pb, 0x10); // space-id
	pb = mp_encode_uint  (pb, nosql->tarantool.space_id);	
	pb = mp_encode_uint  (pb, 0x21); // tuple
	pb = mp_encode_array (pb, 2);
	pb = mp_encode_str   (pb, key, key_size);
	pb = mp_encode_str   (pb, value, value_size);
	mp_encode_uint32(pbh, pb - buffer - 5);
	pbh = pb;

	send(nosql->socket_fd, buffer, pb - buffer, MSG_NOSIGNAL);
	__sync_add_and_fetch(&stat.set.send, 1);
	__sync_fetch_and_add(&nosql->tarantool.stat.set.send, 1);				
}

void nosql_tarantool_select(nosql_t *nosql, const char *key, unsigned int key_size) {
	char buffer[1048576], *pbh, *pb;
	pbh = buffer; pb = pbh + 5;

	pb = mp_encode_map   (pb, 2); 
	pb = mp_encode_uint  (pb, 0x00); // code
	pb = mp_encode_uint  (pb, 0x01); // select
	pb = mp_encode_uint  (pb, 0x01); // sync
	pb = mp_encode_uint  (pb, 0x01); // "yes"
	pb = mp_encode_map   (pb, 6);
	pb = mp_encode_uint  (pb, 0x10); // space-id
	pb = mp_encode_uint  (pb, nosql->tarantool.space_id);	
	pb = mp_encode_uint  (pb, 0x11);
	pb = mp_encode_uint  (pb, 0);
	pb = mp_encode_uint  (pb, 0x12);
	pb = mp_encode_uint  (pb, 1);
	pb = mp_encode_uint  (pb, 0x13);
	pb = mp_encode_uint  (pb, 0);
	pb = mp_encode_uint  (pb, 0x14);
	pb = mp_encode_uint  (pb, 0);
	pb = mp_encode_uint  (pb, 0x20);
	pb = mp_encode_array (pb, 1);
	pb = mp_encode_str   (pb, key, key_size);
	mp_encode_uint32(pbh, pb - buffer - 5);
	pbh = pb;

	send(nosql->socket_fd, buffer, pb - buffer, MSG_NOSIGNAL);
	__sync_add_and_fetch(&stat.get.send, 1);
	__sync_fetch_and_add(&nosql->tarantool.stat.get.send, 1);				
}

void nosql_tarantool_read(nosql_t *nosql, const unsigned int affect_stat) {
	struct epoll_event event;
	if(epoll_wait(nosql->epoll_fd, &event, 1, -1)) {
		char buffer[1048576];
		unsigned int header_offset = 0;
		while(header_offset < 5) {
			int recv_result = recv(nosql->socket_fd, buffer + header_offset, 5 - header_offset, MSG_NOSIGNAL);
			if(recv_result > 0) header_offset += recv_result;
		}

		const char *cpb = buffer;
		int length = mp_decode_uint(&cpb); // check if length > buffer

		header_offset = 0;
		while(header_offset < length) {
			int recv_result = recv(nosql->socket_fd, buffer + header_offset, length - header_offset, MSG_NOSIGNAL);
			if(recv_result > 0) header_offset += recv_result;
		}

		cpb = buffer;

		mp_decode_map(&cpb);
		mp_decode_uint(&cpb);

		int status = mp_decode_uint(&cpb);

		if(affect_stat) {
			if(status) __sync_fetch_and_add(&stat.error, 1);
			if(nosql->tarantool.stat.set.send > nosql->tarantool.stat.set.recv) {
				__sync_fetch_and_add(&nosql->tarantool.stat.set.recv, 1);				
				__sync_fetch_and_add(&stat.set.recv, 1);				
			} else if(nosql->tarantool.stat.get.send > nosql->tarantool.stat.get.recv) {
				__sync_fetch_and_add(&nosql->tarantool.stat.get.recv, 1);				
				__sync_fetch_and_add(&stat.get.recv, 1);				
			}
		}
	}

}
