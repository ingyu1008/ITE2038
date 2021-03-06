#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include "page.h"
#include <stdint.h>
#include <unordered_map>
#include <pthread.h>
#include <iostream>

#define LOCK_MODE_EXCLUSIVE 1
#define LOCK_MODE_SHARED 0

struct lock_t;
struct Hash;
struct hash_table_entry_t;

struct lock_t{
	lock_t* next;
	lock_t* prev;
	hash_table_entry_t* sentinel;
	pthread_cond_t lock_table_cond;
	int lock_mode;
	int record_id;
	lock_t* trx_next;
	int trx_id;
	lock_t* wait_for;
	char* original_value;
	int original_size;
	uint64_t bitmap; // maximum of 62 records in a page so uint64_t is enough
};

struct Hash{
	size_t operator()(const std::pair<int64_t, int64_t>& p) const {
		return std::hash<int64_t>()(p.first ^ 0x5555555555555555) ^ std::hash<int64_t>()(p.second);
	}
};

struct hash_table_entry_t{
	int64_t table_id;
	int64_t page_id;
	lock_t* tail;
	lock_t* head;
};

typedef struct lock_t lock_t;
typedef struct Hash Hash;
typedef struct hash_table_entry_t hash_table_entry_t;

// Helper Function
void wake_up(hash_table_entry_t* list, lock_t* lock);

/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire_compressed(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id);
int shutdown_lock_table();
lock_t *lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);
bool lock_exist(int64_t table_id, int64_t page_id, int64_t key, int trx_id);

extern std::unordered_map<std::pair<int64_t, int64_t>, hash_table_entry_t*, Hash> lock_table;

#endif /* __LOCK_TABLE_H__ */
