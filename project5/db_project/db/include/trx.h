#ifndef __TRX_H__
#define __TRX_H__

#include "lock_table.h"
#include <pthread.h>
#include <unordered_map>
#include <set>
#include <stack>

struct trx_entry_t{
    uint64_t trx_id;
    // pthread_mutex_t trx_mutex; // Not needed for project 5, only one thread per trx
    lock_t* lock;
    std::set<uint64_t> wait_for;
};

// Helper Functions
bool conflict_exists(hash_table_entry_t* list, lock_t* lock);
void update_wait_for_graph(hash_table_entry_t* list, lock_t* lock);
bool detect_deadlock(int trx_id);
lock_t* trx_get_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, int lock_mode);


// API Supported
lock_t* trx_acquire(uint64_t trx_id, lock_t* lock);
void trx_abort(uint64_t trx_id);

int trx_init();
int trx_shutdown();
int trx_begin(void);
int trx_commit(int trx_id);

extern std::unordered_map<uint64_t, trx_entry_t*> trx_table;
extern pthread_mutex_t trx_table_latch;

#endif //__TRX_H__