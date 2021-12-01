#include "trx.h"
#define DEBUG_MODE 0

struct trx_entry_t {
    uint64_t trx_id;
    // pthread_mutex_t trx_mutex; // Not needed for project 5
    lock_t* lock;
};
uint64_t trx_id = 1;

pthread_mutex_t trx_table_latch;

std::unordered_map<uint64_t, trx_entry_t*> trx_table;

void print_trx_list(int trx_id) {
    #if DEBUG_MODE
    if (trx_table.find(trx_id) == trx_table.end()) return;

    pthread_mutex_lock(&trx_table_latch);
    std::cout << "[DEBUG] Start trx list" << std::endl;
    lock_t* lock = trx_table[trx_id]->lock;
    while (lock != nullptr) {
        std::cout << "[DEBUG] lock_mode: " << lock->lock_mode << " page_id: " << lock->sentinel->page_id << " record_id: " << lock->record_id << " trx_id: " << lock->trx_id << std::endl;
        lock = lock->trx_next;
    }

    std::cout << "[DEBUG] End of trx list" << std::endl;

    pthread_mutex_unlock(&trx_table_latch);
    #endif
}

lock_t* trx_get_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, int lock_mode) {
    lock_t* lock = nullptr;

    // auto it = trx_table.find(trx_id);
    // if (it != trx_table.end()) {
    //     trx_entry_t* trx_entry = it->second;
    //     lock_t* temp_lock = trx_entry->lock;
    //     while (temp_lock != nullptr) {
    //         if (temp_lock->trx_id == trx_id && temp_lock->sentinel->table_id == table_id && temp_lock->sentinel->page_id == pagenum && temp_lock->record_id == key && temp_lock->lock_mode == lock_mode) {
    //             lock = temp_lock;
    //             break;
    //         }
    //         temp_lock = temp_lock->trx_next;
    //     }
    // }

    return lock;
}

void trx_acquire(uint64_t trx_id, lock_t* lock) {
    pthread_mutex_lock(&trx_table_latch);
    // lock_t* lock = trx_get_lock(table_id, pagenum, key, trx_id, lock_mode);

    // if (lock != nullptr) {
    //     pthread_mutex_unlock(&trx_table_latch);
    //     return lock;
    // }

    auto it = trx_table.find(trx_id);
    if (it != trx_table.end()) {
        //     #if DEBUG_MODE
        //     std::cout << "[DEBUG] Found trx with trx_id = " << trx_id << ", record_id = " << key << std::endl;
        //     #endif
        trx_entry_t* trx_entry = it->second;

        //     pthread_mutex_unlock(&trx_table_latch);
        //     lock = lock_acquire(table_id, pagenum, key, trx_id, lock_mode);
        //     pthread_mutex_lock(&trx_table_latch);
        //     if (lock != nullptr) {
        lock->trx_next = trx_entry->lock;
        trx_entry->lock = lock;
        //     } else {
        //         #if DEBUG_MODE
        //         std::cout << "[ERROR] Failed to acquire lock" << std::endl;
        //         #endif
        //     }
    }
    pthread_mutex_unlock(&trx_table_latch);
    // return lock;
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
            // TODO implement roll back
            lock_t* next = temp_lock->trx_next;
            // pthread_mutex_unlock(&trx_table_latch);
            lock_release(temp_lock);
            // pthread_mutex_lock(&trx_table_latch);
            temp_lock = next;
        }
        trx_table.erase(trx_id);
    }


    pthread_mutex_unlock(&trx_table_latch);
}

int trx_init() {
    int err = pthread_mutex_init(&trx_table_latch, NULL);
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
        // int err = pthread_mutex_init(trx_entry->trx_mutex, NULL);
        // if (err == 0) {
        //     trx_table[trx_entry->trx_id] = trx_entry;
        //     pthread_mutex_unlock(&trx_table_latch);
        //     return trx_id;
        // }
        trx_table[trx_entry->trx_id] = trx_entry;
        pthread_mutex_unlock(&trx_table_latch);
        return trx_entry->trx_id;
    }

    pthread_mutex_unlock(&trx_table_latch);
    return 0;
}

int trx_commit(int trx_id) {
    // print_trx_list(trx_id);
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
        trx_table.erase(trx_id);
        // pthread_mutex_destroy(trx_entry->trx_mutex); 
    }
    pthread_mutex_unlock(&trx_table_latch);
    return trx_id;
}