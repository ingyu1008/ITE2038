#include "lock_table.h"

struct lock_t {
  /* GOOD LOCK :) */
};

typedef struct lock_t lock_t;

int init_lock_table() {
  return 0;
}

lock_t* lock_acquire(int table_id, int64_t ket) {
  return nullptr;
};

int lock_release(lock_t* lock_obj) {
  return 0;
}
