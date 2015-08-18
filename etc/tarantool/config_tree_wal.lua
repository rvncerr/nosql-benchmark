#!/usr/bin/tarantool

box.cfg {
	work_dir = ".",
	too_long_threshold = 10, 
	listen = "0.0.0.0:3301",
	slab_alloc_arena = 6,
	slab_alloc_maximal = 6291456,
	wal_mode = "write"
}

nosql_benchmark_s = box.schema.space.create('nosql_benchmark', {
	if_not_exists = true,
	engine = 'memtx'
})

nosql_benchmark_i = nosql_benchmark_s:create_index('primary', {
	if_not_exists = true,
	type = 'TREE',
	parts = {1, 'STR'}
})

box.schema.user.grant('guest', 'read,write,execute', 'universe', '', {if_not_exists = true})

require("console").start()
