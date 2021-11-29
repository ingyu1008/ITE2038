#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "file.h"
#include "page.h"
#include <vector>
#include <map>

// TODO: Encapsulate This Structure (Probably after finish implementing everything)
struct control_block_t {
    page_t* frame;
    int64_t table_id;
    pagenum_t pagenum;
    int is_dirty;
    pthread_mutex_t page_latch;
    int is_pinned; // Not deleted for simpler implementation: not modifying internal logic
    control_block_t* next;
    control_block_t* prev;
};

void return_ctrl_block(control_block_t** ctrl_block, int is_dirty = 0);

// Simple
int64_t buf_open_table_file(const char* pathname);

// Straight Forward
control_block_t* buf_read_page(int64_t table_id, pagenum_t page_number);

// Complicated
pagenum_t buf_alloc_page(int64_t table_id);
void buf_free_page(int64_t table_id, pagenum_t page_number);

int buf_init_db(int num_buf);
int buf_shutdown_db();

#endif //__BUFFER_H__