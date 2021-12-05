#ifndef __TRX_H__
#define __TRX_H__

#include "lock_table.h"
#include <pthread.h>
#include <unordered_map>
#include <set>
#include <map>
#include <stack>
#include <optional>

struct trx_entry_t{
    uint64_t trx_id;
    // pthread_mutex_t trx_mutex; // Not needed for project 5, only one thread per trx
    lock_t* lock;
    std::set<uint64_t> wait_for;
    std::map<std::pair<std::pair<int64_t, pagenum_t>, std::pair<int64_t, int>>, lock_t*> locks;
    std::map<std::pair<std::pair<int64_t, pagenum_t>, int64_t>, std::pair<uint16_t, char*>> logs;
};

// Helper Functions
bool conflict_exists(hash_table_entry_t* list, lock_t* lock);
void update_wait_for_graph(hash_table_entry_t* list, lock_t* lock);
bool detect_deadlock(int trx_id);
void release_locks(trx_entry_t* trx);
void add_to_trx_list(trx_entry_t *trx, hash_table_entry_t *list, lock_t* lock);

// API Supported
void trx_add_to_locks(int trx_id, lock_t* lock);
std::optional<std::pair<uint16_t, char*>> trx_find_log(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id);
void trx_add_log(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, std::pair<uint16_t, char*> log);
lock_t* trx_get_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int64_t trx_id, int lock_mode);
lock_t* trx_acquire(uint64_t trx_id, lock_t* lock);
void trx_abort(uint64_t trx_id);
trx_entry_t* trx_check_active(int trx_id);
int trx_implicit_to_explicit(int64_t table_id, pagenum_t pagenum, int64_t key, int trx_id, int trx_written);


int trx_init();
int trx_shutdown();
int trx_begin(void);
int trx_commit(int trx_id);

extern std::unordered_map<uint64_t, trx_entry_t*> trx_table;
extern pthread_mutex_t trx_table_latch;

#endif //__TRX_H__