#ifndef __TRX_H__
#define __TRX_H__

#include "lock_table.h"
#include <pthread.h>
#include <unordered_map>

struct trx_entry_t{
    uint64_t trx_id;
    // pthread_mutex_t trx_mutex; // Not needed for project 5
    lock_t* lock;
};

lock_t* trx_get_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, int lock_mode);

// void trx_acquire(int64_t table_id, pagenum_t pagenum, int64_t key, uint64_t trx_id, int lock_mode);
void trx_acquire(uint64_t trx_id, lock_t* lock);
void trx_abort(uint64_t trx_id);

int trx_init();
int trx_begin(void);
int trx_commit(int trx_id);

extern std::unordered_map<uint64_t, trx_entry_t*> trx_table;
extern pthread_mutex_t trx_table_latch;

#endif //__TRX_H__