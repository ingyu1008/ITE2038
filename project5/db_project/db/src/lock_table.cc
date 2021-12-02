#include "lock_table.h"
#include <set>
#define DEBUG_MODE 0

constexpr int LOCK_MODE_EXCLUSIVE = 1;
constexpr int LOCK_MODE_SHARED = 0;

pthread_mutex_t lock_table_latch;

std::unordered_map<std::pair<int64_t, int64_t>, hash_table_entry_t*, Hash> lock_table;


void print_locks(hash_table_entry_t* list) {
	#if DEBUG_MODE
	std::cout << "[DEBUG] lock list: " << std::endl;
	lock_t* lock = list->head;
	while (lock != NULL) {
		std::cout << "[DEBUG] lock_mode: " << lock->lock_mode << " record_id: " << lock->sentinel->page_id << ", " <<lock->record_id << " trx_id: " << lock->trx_id << std::endl;
		lock = lock->next;
	}
	#endif
}

void wake_up(hash_table_entry_t* list, lock_t* lock) {
	lock_t* cur = list->head;
	int record_id = lock->record_id;
	int trx_id = lock->trx_id;
	int x = 0;
	int y = 0;
	while (cur != nullptr) {
		if (cur->record_id != record_id) {
			// pthread_cond_signal(&cur->lock_table_cond);
			cur = cur->next;
			continue;
		}
		// pthread_cond_signal(&cur->lock_table_cond);
		// break;

		if (cur == lock) {
			cur = cur->next;
			continue;
		}
		if (cur->lock_mode == LOCK_MODE_EXCLUSIVE) {
			if (x == 0 || (y == 0 && x == cur->trx_id)) {
				// std::cout << "[DEBUG] wake up trx_id: " << cur->trx_id << std::endl;
				// print_locks(list);
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

	// //TODO implement properly
	// //This implementation just wake up everything
	// lock_t* lock = list->head;
	// while (lock != NULL) {
	// 	pthread_cond_signal(&lock->lock_table_cond);
	// 	lock = lock->next;
	// }
};

bool conflict_exists(hash_table_entry_t* list, lock_t* lock) {
	// return false;
	// TODO implement
	lock_t* curr = list->head;
	// while (curr != nullptr) {
	// 	if(curr == lock) return false;
	// 	if (curr->record_id != lock->record_id || curr->trx_id == lock->trx_id || (lock->lock_mode | curr->lock_mode) == 0) {
	// 		curr = curr->next;
	// 		continue;
	// 	}
	// 	return true;
	// }
	while (curr != nullptr) {
		if (curr == lock) return false;
		if (curr->trx_id != lock->trx_id && curr->record_id == lock->record_id && (lock->lock_mode | curr->lock_mode) == 1) {
			#if DEBUG_MODE
			std::cout << "[DEBUG] conflict! "<< curr->lock_mode << ", " <<lock->lock_mode << ", " << curr->record_id << ", " << curr->trx_id << ", " << lock->trx_id << std::endl;
			#endif
			print_locks(list);
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
	// std::cout << "[DEBUG] lock_acquire: " << table_id << ", " << page_id << ", " << key << ", " << trx_id << ", " << lock_mode << std::endl;
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
	lock->lock_table_cond = PTHREAD_COND_INITIALIZER;
	lock->lock_mode = lock_mode;
	lock->record_id = key;
	lock->trx_id = trx_id;
	lock->trx_next = nullptr;
	lock->original_value = nullptr;

	if (list->tail != nullptr) {
		list->tail->next = lock;
	}
	list->tail = lock;
	if (list->head == nullptr) {
		list->head = lock;
	}
	while (conflict_exists(list, lock)) {
		// std::cout << "[DEBUG] sleep!" << std::endl;
		pthread_cond_wait(&lock->lock_table_cond, &lock_table_latch);
	}
	// print_locks(list);
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
	if(list->head == lock_obj){
		list->head = next;
	}
	if (list->tail == lock_obj) {
		list->tail = prev;
	}
	wake_up(list, lock_obj);
	delete lock_obj;
	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}

