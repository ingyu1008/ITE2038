#ifndef __TRX_H__
#define __TRX_H__

#include "lock_table.h"
#include <pthread.h>
#include <unordered_map>

struct trx_entry_t;

lock_t *trx_acquire(int64_t table_id, pagenum_t pagenum, int64_t key, uint64_t trx_id, int lock_mode);
void trx_abort(uint64_t trx_id);

int trx_init();
int trx_begin(void);
int trx_commit(int trx_id);

#endif //__TRX_H__