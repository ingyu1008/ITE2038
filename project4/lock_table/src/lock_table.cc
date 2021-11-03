#include "lock_table.h"

struct lock_t {
	lock_t* next;
	lock_t* prev;
	hash_table_entry_t* sentinel;
	pthread_cond_t lock_table_cond;
};

struct Hash {
	size_t operator()(const std::pair<int64_t, int64_t>& p) const {
		return std::hash<int64_t>()(p.first ^ 0x5555555555555555) ^ std::hash<int64_t>()(p.second);
	}
};

struct hash_table_entry_t {
	int64_t table_id;
	int64_t key;
	lock_t* tail;
	lock_t* head;
};

typedef struct lock_t lock_t;
typedef struct Hash Hash;
typedef struct hash_table_entry_t hash_table_entry_t;

pthread_mutex_t lock_table_latch;

std::unordered_map<std::pair<int64_t, int64_t>, hash_table_entry_t*, Hash> lock_table;

int init_lock_table() {
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	return 0;
}

lock_t* lock_acquire(int64_t table_id, int64_t key) {
	pthread_mutex_lock(&lock_table_latch);
	std::pair<int64_t, int64_t> p(table_id, key);
	hash_table_entry_t* list = lock_table[p];
	if (list == NULL) {
		list = new hash_table_entry_t();
		list->table_id = table_id;
		list->key = key;
		list->head = NULL;
		list->tail = NULL;
		lock_table[p] = list;
	}
	lock_t* lock = new lock_t();
	lock->next = NULL;
	lock->prev = list->tail;
	lock->sentinel = list;
	lock->lock_table_cond = PTHREAD_COND_INITIALIZER;
	if (list->tail != NULL) {
		list->tail->next = lock;
	}
	list->tail = lock;
	if (list->head == NULL) {
		list->head = lock;
	}
	while (list->head != lock) {
		pthread_cond_wait(&lock->lock_table_cond, &lock_table_latch);
	}
	pthread_mutex_unlock(&lock_table_latch);
	return lock;

};

int lock_release(lock_t* lock_obj) {
	pthread_mutex_lock(&lock_table_latch);
	lock_t* prev = lock_obj->prev;
	lock_t* next = lock_obj->next;
	hash_table_entry_t* list = lock_obj->sentinel;
	if (prev != NULL) {
		prev->next = next;
	}
	if (next != NULL) {
		next->prev = prev;
	}
	list->head = next;
	if(next == NULL){
		list->tail = NULL;
	}
	if (next != NULL) {
		pthread_cond_signal(&next->lock_table_cond);
	}
	delete lock_obj;
	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}
