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
        if ((cur->bitmap & lock->bitmap) == 0) {
            cur = cur->prev;
            continue;
        }

        if (cur->lock_mode == LOCK_MODE_EXCLUSIVE) {
            if (cur->trx_id != lock->trx_id) {
                trx_table[lock->trx_id]->wait_for.insert(cur->trx_id);
            }
            break;
        } else if (lock->lock_mode == LOCK_MODE_EXCLUSIVE) {
            if (cur->trx_id != lock->trx_id) {
                trx_table[lock->trx_id]->wait_for.insert(cur->trx_id);
            }
        }
        cur = cur->prev;
    }
}

bool detect_deadlock(int trx_id) {
    std::set<int> visited;
    visited.insert(trx_id);

    std::stack<std::pair<int, int>> dfs_stack;
    dfs_stack.push({ trx_id,trx_id });
    while (dfs_stack.size() > 0) {
        int par_trx_id = dfs_stack.top().first;
        int curr_trx_id = dfs_stack.top().second;
        dfs_stack.pop();

        trx_entry_t* par_trx = trx_table[par_trx_id];
        trx_entry_t* curr_trx = trx_table[curr_trx_id];

        if (par_trx == nullptr) continue;

        if (curr_trx == nullptr) {
            par_trx->wait_for.erase(curr_trx_id);
            continue;
        }

        if (par_trx_id != curr_trx_id && trx_id == curr_trx_id) return true;

        if (par_trx_id != curr_trx_id && visited.find(curr_trx_id) != visited.end()) continue;

        visited.insert(curr_trx_id);

        for (auto it = curr_trx->wait_for.begin(); it != curr_trx->wait_for.end();it++) dfs_stack.push({ curr_trx_id,*it });
    }

    return false;
}

void release_locks(trx_entry_t* trx) {
    lock_t* lock = trx->lock;
    while (lock != nullptr) {
        lock_t* tmp = lock->trx_next;
        lock_release(lock);
        lock = tmp;
    }
    trx->locks.clear();
}

void add_to_trx_list(trx_entry_t *trx, hash_table_entry_t *list, lock_t* lock){
    // std::cout << "[DEBUG] lock, trx = " << lock << ", " << trx << std::endl;
    lock->trx_next = trx->lock;
    trx->lock = lock;

    trx->locks[{ {lock->sentinel->table_id, lock->sentinel->page_id}, { lock->record_id, lock->lock_mode }}] = lock;
    
    update_wait_for_graph(list, lock);
}

std::optional<std::pair<uint16_t, char*>> trx_find_log(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id) {
    pthread_mutex_lock(&trx_table_latch);

    auto it = trx_table[trx_id]->logs.find({ {table_id, pagenum}, key });
    if (it == trx_table[trx_id]->logs.end()) {
        pthread_mutex_unlock(&trx_table_latch);
        return std::nullopt;
    }

    auto p = it->second;

    pthread_mutex_unlock(&trx_table_latch);
    return std::optional<std::pair<uint16_t, char*>>(p);
}

void trx_add_log(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, std::pair<uint16_t, char*> log) {
    pthread_mutex_lock(&trx_table_latch);

    trx_table[trx_id]->logs[{ {table_id, pagenum}, key}] = log;

    pthread_mutex_unlock(&trx_table_latch);
}

lock_t* trx_get_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, int lock_mode) {
    pthread_mutex_lock(&trx_table_latch);

    lock_t* lock = nullptr;
    trx_entry_t* trx = trx_table[trx_id];
    if (trx != nullptr) {
        auto it = trx->locks.find({ {table_id, pagenum}, {key, 1} });
        if (it == trx->locks.end() && lock_mode == LOCK_MODE_SHARED) {
            it = trx->locks.find({ {table_id, pagenum}, {key, 0} });
        }
        if (it != trx->locks.end()) {
            lock = it->second;
        }
    }

    pthread_mutex_unlock(&trx_table_latch);
    return lock;
}
void trx_add_to_locks(int trx_id, int64_t key, lock_t* lock){
    pthread_mutex_lock(&trx_table_latch);

    trx_table[trx_id]->locks[{ {lock->sentinel->table_id, lock->sentinel->page_id}, { key, lock->lock_mode }}] = lock;

    pthread_mutex_unlock(&trx_table_latch);
}

void trx_add_to_locks(int trx_id, lock_t* lock){
    pthread_mutex_lock(&trx_table_latch);

    trx_table[trx_id]->locks[{ {lock->sentinel->table_id, lock->sentinel->page_id}, { lock->record_id, lock->lock_mode }}] = lock;

    pthread_mutex_unlock(&trx_table_latch);
}

trx_entry_t* trx_check_active(int trx_id) {
    auto it = trx_table.find(trx_id);
    if (it != trx_table.end()) {
        return it->second;
    }

    return nullptr;
}

int trx_implicit_to_explicit(int64_t table_id, pagenum_t pagenum, int64_t key, int trx_id, int trx_written){
    pthread_mutex_lock(&trx_table_latch);
    trx_entry_t* trx = trx_check_active(trx_written);

    if (trx != nullptr) {
        // implicit to explicit
        lock_t* lock = lock_acquire(table_id, pagenum, key, trx_written, 1);

        if (lock == nullptr) {
            pthread_mutex_unlock(&trx_table_latch);
            return -1;
        }

        add_to_trx_list(trx, lock->sentinel, lock);

        pthread_mutex_unlock(&trx_table_latch);
        return 0;
    } else {
        pthread_mutex_unlock(&trx_table_latch);
        return -1;
    }
}

lock_t* trx_acquire(uint64_t trx_id, lock_t* lock) {
    pthread_mutex_lock(&trx_table_latch);

    auto it = trx_table.find(trx_id);
    // std::cout << "trx_entry == nullptr: " << (it == trx_table.end()) << " trx_id = " << trx_id << std::endl;

    if (it == trx_table.end()) {
        pthread_mutex_unlock(&trx_table_latch);
        return nullptr;
    }

    #if DEBUG_MODE
    // std::cout << "[DEBUG] Found trx with trx_id = " << trx_id << ", record_id = " << key << std::endl;
    #endif
    trx_entry_t* trx_entry = it->second;

    // TODO: change this to a proper list
    
    hash_table_entry_t* list = lock->sentinel;

    add_to_trx_list(trx_entry, list, lock);

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


        for (auto x : trx_entry->logs) {
            auto key = x.first;
            auto log = x.second;

            control_block_t* ctrl_block = buf_read_page(key.first.first, key.first.second);
            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, key.second);

            ctrl_block->frame->set_data(log.second, slot.get_offset(), log.first);
            buf_return_ctrl_block(&ctrl_block, 1);
        }

        release_locks(trx_entry);

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
        release_locks(trx_entry);

        delete trx_entry;
        trx_table.erase(it);
    }
    pthread_mutex_unlock(&trx_table_latch);
    return trx_id;
}