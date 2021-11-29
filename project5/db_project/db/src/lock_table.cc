#include "lock_table.h"
#define DEBUG_MODE 0

pthread_mutex_t lock_table_latch;

std::unordered_map<std::pair<int64_t, int64_t>, hash_table_entry_t*, Hash> lock_table;


void print_locks(hash_table_entry_t* list) {
	#if DEBUG_MODE
	lock_t* lock = list->head;
	while (lock != NULL) {
		std::cout << "[DEBUG] lock_mode: " << lock->lock_mode << " record_id: " << lock->record_id << " trx_id: " << lock->trx_id << std::endl;
		lock = lock->next;
	}
	#endif
}

void wake_up(hash_table_entry_t* list) {
	//TODO implement properly
	//This implementation just wake up everything
	lock_t* lock = list->head;
	while (lock != NULL) {
		pthread_cond_signal(&lock->lock_table_cond);
		lock = lock->next;
	}
};

bool conflict_exists(hash_table_entry_t* list, lock_t* lock) {
	return false;
	// TODO implement
	lock_t* curr = list->head;
	while (curr != NULL) {
		if (curr->trx_id != lock->trx_id && curr->record_id == lock->record_id && (lock->lock_mode | curr->lock_mode) == 1 && curr != lock) {
			#if DEBUG_MODE
			std::cout << "[DEBUG] conflict!!!" << std::endl;
			#endif
			return true;
		}
		curr = curr->next;
	}
	return false;
}

int init_lock_table() {
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	return 0;
}

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) {
	pthread_mutex_lock(&lock_table_latch);
	std::pair<int64_t, int64_t> p(table_id, page_id);
	hash_table_entry_t* list = lock_table[p];
	if (list == NULL) {
		list = new hash_table_entry_t();
		list->table_id = table_id;
		list->page_id = page_id;
		list->head = NULL;
		list->tail = NULL;
		lock_table[p] = list;
	}
	lock_t* lock = new lock_t();
	lock->next = NULL;
	lock->prev = list->tail;
	lock->sentinel = list;
	lock->lock_table_cond = PTHREAD_COND_INITIALIZER;
	lock->lock_mode = lock_mode;
	lock->record_id = key;
	lock->trx_id = trx_id;
	lock->trx_next = nullptr;

	if (list->tail != NULL) {
		list->tail->next = lock;
	}
	list->tail = lock;
	if (list->head == NULL) {
		list->head = lock;
	}
	while (conflict_exists(list, lock)) {
		pthread_cond_wait(&lock->lock_table_cond, &lock_table_latch);
	}
	print_locks(list);
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
	if (next == NULL) {
		list->tail = NULL;
	}
	if (next != NULL) {
		wake_up(list);
	}
	delete lock_obj;
	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}

