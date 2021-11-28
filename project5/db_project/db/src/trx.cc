#include "trx.h"

struct trx_entry_t {
    uint64_t trx_id;
    // pthread_mutex_t trx_mutex; // Not needed for project 5
    lock_t* lock;
};

pthread_mutex_t trx_table_latch;

std::unordered_map<uint64_t, trx_entry_t*> trx_table;

lock_t* trx_acquire(int64_t table_id, pagenum_t pagenum, int64_t key, uint64_t trx_id, int lock_mode) {
    lock_t* lock = nullptr;

    pthread_mutex_lock(&trx_table_latch);
    auto it = trx_table.find(trx_id);
    if (it != trx_table.end()) {
        std::cout << "[DEBUG] Found trx with trx_id = " << trx_id << std::endl;
        trx_entry_t* trx_entry = it->second;

        lock = lock_acquire(table_id, pagenum, key, trx_id, lock_mode);
        if (lock != nullptr) {
            lock->trx_next = trx_entry->lock;
            trx_entry->lock = lock;
        }
    }
    pthread_mutex_unlock(&trx_table_latch);
    return lock;
}

void trx_abort(uint64_t trx_id) {
    std::cout << "[ABORT] Aborted " << trx_id << std::endl;
}

int trx_init() {
    int err = pthread_mutex_init(&trx_table_latch, NULL);
    return err;
}

int trx_begin(void) {
    static uint64_t trx_id = 1;


    pthread_mutex_lock(&trx_table_latch);
    trx_entry_t* trx_entry = new trx_entry_t();
    if (trx_entry != nullptr) {
        trx_entry->trx_id = trx_id++;
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
    pthread_mutex_lock(&trx_table_latch);
    auto it = trx_table.find(trx_id);
    if (it != trx_table.end()) {
        trx_entry_t* trx_entry = it->second;
        lock_t* lock = trx_entry->lock;
        while (lock != NULL) {
            lock_t* tmp = lock->trx_next;
            lock_release(lock);
            lock = tmp;
        }
        // pthread_mutex_destroy(trx_entry->trx_mutex); 
    }
    pthread_mutex_unlock(&trx_table_latch);
    return trx_id;
}