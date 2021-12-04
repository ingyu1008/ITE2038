#include "trx.h"
#include "buffer.h"
#define DEBUG_MODE 0

#define LOCK_MODE_EXCLUSIVE 1
#define LOCK_MODE_SHARED 0

uint64_t trx_id = 1;

pthread_mutex_t trx_table_latch;

std::unordered_map<uint64_t, trx_entry_t*> trx_table;

bool conflict_exists(hash_table_entry_t* list, lock_t* lock) {
	lock_t* curr = list->head;
	while (curr != nullptr) {
		if (curr == lock) return false;
		if (curr->trx_id != lock->trx_id && curr->record_id == lock->record_id && (lock->lock_mode | curr->lock_mode) == 1) {
			#if DEBUG_MODE
			std::cout << "[DEBUG] conflict! " << curr->lock_mode << ", " << lock->lock_mode << ", " << curr->record_id << ", " << curr->trx_id << ", " << lock->trx_id << std::endl;
			#endif
			return true;
		}
		curr = curr->next;
	}
	return false;
}

void update_wait_for_graph(hash_table_entry_t* list, lock_t* lock) {
	lock_t* cur = lock->prev;
	while (cur != nullptr) {
		if(cur->record_id != lock->record_id) {
			cur = cur->prev;
			continue;
		}

		if(cur->lock_mode == LOCK_MODE_EXCLUSIVE){
			if(cur->trx_id != lock->trx_id) {
				trx_table[lock->trx_id]->wait_for.insert(cur->trx_id);
			}
			break;
		} else if(lock->lock_mode == LOCK_MODE_EXCLUSIVE) {
			if(cur->trx_id != lock->trx_id) {
				trx_table[lock->trx_id]->wait_for.insert(cur->trx_id);
			}
		}
		cur = cur->prev;
	}
}

bool detect_deadlock(int trx_id) {
    std::set<int> visited;
    visited.insert(trx_id);

    std::stack<std::pair<int,int>> dfs_stack;
    dfs_stack.push({trx_id,trx_id});
    while(dfs_stack.size() > 0) {
        int par_trx_id = dfs_stack.top().first;
        int curr_trx_id = dfs_stack.top().second;
        dfs_stack.pop();

        trx_entry_t* par_trx = trx_table[par_trx_id];
        trx_entry_t* curr_trx = trx_table[curr_trx_id];

        if(par_trx == nullptr) continue;

        if(curr_trx == nullptr){
            par_trx->wait_for.erase(curr_trx_id);
            continue;
        }

        if(par_trx_id != curr_trx_id && trx_id == curr_trx_id) return true;

        if(par_trx_id != curr_trx_id && visited.find(curr_trx_id) != visited.end()) continue;

        visited.insert(curr_trx_id);

        for(auto it = curr_trx->wait_for.begin(); it != curr_trx->wait_for.end();it++) dfs_stack.push({curr_trx_id,*it});
    }

	return false;
}

lock_t* trx_get_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, int lock_mode) {
    pthread_mutex_lock(&trx_table_latch);

    lock_t* lock = trx_table[trx_id]->locks[{{table_id, pagenum}, key}];

    pthread_mutex_unlock(&trx_table_latch);
    return lock;
}

lock_t* trx_acquire(uint64_t trx_id, lock_t* lock) {
    pthread_mutex_lock(&trx_table_latch);

    auto it = trx_table.find(trx_id);
    if (it != trx_table.end()) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Found trx with trx_id = " << trx_id << ", record_id = " << key << std::endl;
        #endif
        trx_entry_t* trx_entry = it->second;

        // TODO: change this to a proper list
        lock->trx_next = trx_entry->lock;
        trx_entry->lock = lock;
    }

    it->second->locks[{{lock->sentinel->table_id, lock->sentinel->page_id}, lock->record_id}] = lock;

    hash_table_entry_t* list = lock->sentinel;

    update_wait_for_graph(list, lock);
	if (detect_deadlock(lock->trx_id)) {
        #if DEBUG_MODE
		std::cout << "[DEBUG] deadlock!" << std::endl;
        #endif
		pthread_mutex_unlock(&trx_table_latch);
		return nullptr;
	}

	while (conflict_exists(list, lock)) {
        #if DEBUG_MODE
		std::cout << "[DEBUG] sleep!" << std::endl;
        #endif
		pthread_cond_wait(&lock->lock_table_cond, &trx_table_latch);
	}
    pthread_mutex_unlock(&trx_table_latch);
    return lock;
}

void trx_abort(uint64_t trx_id) {
    pthread_mutex_lock(&trx_table_latch);
    #if DEBUG_MODE
    std::cout << "[ABORT] Aborted " << trx_id << std::endl;
    #endif

    auto it = trx_table.find(trx_id);
    if (it != trx_table.end()) {
        trx_entry_t* trx_entry = it->second;
        lock_t* temp_lock = trx_entry->lock;
        while (temp_lock != nullptr) {
            if(temp_lock->original_value != nullptr) {          
                control_block_t* ctrl_block = buf_read_page(temp_lock->sentinel->table_id, temp_lock->sentinel->page_id);
                slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, temp_lock->record_id);

                #if DEBUG_MODE
                std::cout << "[DEBUG] roll back value: " << temp_lock->original_value << std::endl;
                #endif

                ctrl_block->frame->set_data(temp_lock->original_value, slot.get_offset(), temp_lock->original_size);
                buf_return_ctrl_block(&ctrl_block, 1);
            }

            lock_t* next = temp_lock->trx_next;
                  
            lock_release(temp_lock);
            temp_lock = next;
        }
        
        delete trx_entry;
        trx_table.erase(trx_id);
    }

    pthread_mutex_unlock(&trx_table_latch);
}

int trx_init() {
    int err = pthread_mutex_init(&trx_table_latch, NULL);
    return err;
}

int trx_shutdown() { 
    int err = pthread_mutex_destroy(&trx_table_latch);
    return err;
}

int trx_begin(void) {
    pthread_mutex_lock(&trx_table_latch);
    #if DEBUG_MODE
    std::cout << "[DEBUG] trx_begin trx_id = " << trx_id << std::endl;
    #endif
    trx_entry_t* trx_entry = new trx_entry_t();
    if (trx_entry != nullptr) {
        trx_entry->trx_id = trx_id++;
        trx_entry->lock = nullptr;
        trx_table[trx_entry->trx_id] = trx_entry;
        pthread_mutex_unlock(&trx_table_latch);
        return trx_entry->trx_id;
    }

    pthread_mutex_unlock(&trx_table_latch);
    return 0;
}

int trx_commit(int trx_id) {
    pthread_mutex_lock(&trx_table_latch);
    #if DEBUG_MODE
    std::cout << "[DEBUG] trx_commit trx_id = " << trx_id << std::endl;
    #endif
    auto it = trx_table.find(trx_id);
    if (it != trx_table.end()) {
        trx_entry_t* trx_entry = it->second;
        lock_t* lock = trx_entry->lock;
        while (lock != nullptr) {
            lock_t* tmp = lock->trx_next;

            lock_release(lock);
            lock = tmp;
        }
        delete trx_entry;
        trx_table.erase(trx_id);
    }
    pthread_mutex_unlock(&trx_table_latch);
    return trx_id;
}