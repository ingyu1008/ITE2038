#include "lock_table.h"
#include "trx.h"
#include <set>
#define DEBUG_MODE 0

pthread_mutex_t lock_table_latch;

std::unordered_map<std::pair<int64_t, int64_t>, hash_table_entry_t*, Hash> lock_table;

void wake_up(hash_table_entry_t* list, lock_t* lock) {
	lock_t* cur = list->head;
	int record_id = lock->record_id;
	int trx_id = lock->trx_id;
	int x = 0;
	int y = 0;
	while (cur != nullptr) {
		if ((cur->bitmap & lock->bitmap) == 0 || cur->trx_id == lock->trx_id) {
			cur = cur->next;
			continue;
		}
		if (cur->lock_mode == LOCK_MODE_EXCLUSIVE) {
			if (x == 0 || (y == 0 && x == cur->trx_id)) {
				#if DEBUG_MODE
				std::cout << "[DEBUG] wake up trx_id: " << cur->trx_id << std::endl;
				#endif
				pthread_cond_signal(&cur->lock_table_cond);
			}
			break;
		} else {
			if (x == 0) x = cur->trx_id;
			else if (cur->trx_id != x) y = cur->trx_id;
			pthread_cond_signal(&cur->lock_table_cond);
			cur = cur->next;
		}
	}
};


int init_lock_table() {
	// lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_init(&lock_table_latch, NULL);
	return 0;
}

int shutdown_lock_table() { 
	for(auto it = lock_table.begin(); it != lock_table.end();) {
		hash_table_entry_t* list = it->second;
		// ASSUME THE LIST IS EMPTY
		delete list;
		lock_table.erase(it++);
	}
	pthread_mutex_destroy(&lock_table_latch);
	return 0;
}

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) {
	pthread_mutex_lock(&lock_table_latch);
	std::pair<int64_t, int64_t> p(table_id, page_id);
	hash_table_entry_t* list = lock_table[p];
	if (list == nullptr) {
		list = new hash_table_entry_t();
		list->table_id = table_id;
		list->page_id = page_id;
		list->head = nullptr;
		list->tail = nullptr;
		lock_table[p] = list;
	}
	lock_t* lock = new lock_t();

	lock->next = nullptr;
	lock->prev = list->tail;
	lock->sentinel = list;
	pthread_cond_init(&lock->lock_table_cond, NULL);
	lock->lock_mode = lock_mode;
	lock->record_id = key;
	lock->trx_id = trx_id;
	lock->original_value = nullptr;
	lock->original_size = 0;
	lock->bitmap = (((uint64_t)1) << key);

	if (list->tail != nullptr) {
		list->tail->next = lock;
	}
	list->tail = lock;
	if (list->head == nullptr) {
		list->head = lock;
	}
	
	pthread_mutex_unlock(&lock_table_latch);
	return lock;
};

int lock_release(lock_t* lock_obj) {
	pthread_mutex_lock(&lock_table_latch);
	lock_t* prev = lock_obj->prev;
	lock_t* next = lock_obj->next;
	hash_table_entry_t* list = lock_obj->sentinel;

	wake_up(list, lock_obj);
	
	if (prev != nullptr) {
		prev->next = next;
	}
	if (next != nullptr) {
		next->prev = prev;
	}
	if (list->head == lock_obj) {
		list->head = next;
	}
	if (list->tail == lock_obj) {
		list->tail = prev;
	}

	pthread_cond_destroy(&lock_obj->lock_table_cond);

	delete lock_obj;
	
	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}

bool lock_exist(int64_t table_id, int64_t page_id, int64_t key, int trx_id){
	pthread_mutex_lock(&lock_table_latch);
	std::pair<int64_t, int64_t> p(table_id, page_id);
	hash_table_entry_t* list = lock_table[p];
	if (list == nullptr) {
		pthread_mutex_unlock(&lock_table_latch);
		return false;
	}
	lock_t* cur = list->head;
	while (cur != nullptr) {
		if ((cur->bitmap & (((uint64_t)1) << key)) > 0 /* && cur->trx_id != trx_id */) {
			pthread_mutex_unlock(&lock_table_latch);
			return true;
		}
		cur = cur->next;
	}
	pthread_mutex_unlock(&lock_table_latch);
	return false;
}

lock_t* lock_acquire_compressed(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id) {
	pthread_mutex_lock(&lock_table_latch);

	std::pair<int64_t, int64_t> p(table_id, page_id);
	hash_table_entry_t* list = lock_table[p];

	if (list == nullptr) {
		pthread_mutex_unlock(&lock_table_latch);
		return nullptr;
	}

	lock_t* cur = list->head;
	while (cur != nullptr) {
		// No X lock holding this key exist in the trx (guarenteed by the caller function)
		if(cur->lock_mode == LOCK_MODE_EXCLUSIVE && (cur->bitmap & (((uint64_t)1) << key)) != 0){
			pthread_mutex_unlock(&lock_table_latch);
			return nullptr;
		}
		cur = cur->next;
	}

	cur = list->head;
	lock_t* me = nullptr;
	while (cur != nullptr) {
		if (cur->lock_mode == LOCK_MODE_SHARED && cur->trx_id == trx_id) {
			me = cur;
			break;
		}
		cur = cur->next;
	}

	if(me != nullptr){
		me->bitmap |= (((uint64_t)1) << key);
	}


	pthread_mutex_unlock(&lock_table_latch);
	return me;
}