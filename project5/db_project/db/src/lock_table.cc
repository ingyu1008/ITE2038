#include "lock_table.h"

pthread_mutex_t lock_table_latch;

std::unordered_map<std::pair<int64_t, int64_t>, hash_table_entry_t*, Hash> lock_table;

bool conflict_exists(hash_table_entry_t* list, lock_t *lock){
	lock_t* curr = list->head;
	while(curr != NULL){
		if(curr->trx_id != lock->trx_id && curr->record_id == lock->record_id && (lock->lock_mode | curr->lock_mode) == 1 && curr != lock){
			std::cout << "[DUBUG] conflic!!!" << std::endl;
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
	
	std::cout << "asdfdasdf" << std::endl;
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
	std::cout << "asdfdasdf" << std::endl;
	lock_t* lock = new lock_t();
	lock->next = NULL;
	lock->prev = list->tail;
	lock->sentinel = list;
	int err = pthread_cond_init(&lock->lock_table_cond, NULL);
	if (err != 0) {
		pthread_mutex_unlock(&lock_table_latch);
		return nullptr;
	}
	// lock->lock_table_cond = PTHREAD_COND_INITIALIZER;

	std::cout << "asdfdasdf" << std::endl;
	if (list->tail != NULL) {
		list->tail->next = lock;
	}
	list->tail = lock;
	if (list->head == NULL) {
		list->head = lock;
	}
	std::cout << "asdfdasdf" << std::endl;
	while (conflict_exists(list, lock)) {
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
	if (next == NULL) {
		list->tail = NULL;
	}
	if (next != NULL) {
		pthread_cond_signal(&next->lock_table_cond);
	}

	int err = pthread_cond_destroy(&lock_obj->lock_table_cond);
	delete lock_obj;
	pthread_mutex_unlock(&lock_table_latch);
	return err;
}
