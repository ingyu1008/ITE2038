#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "file.h"
#include "page.h"
#include <vector>
#include <map>
#include <unordered_map>

extern std::unordered_map<int64_t, int64_t> table_id_map;

// TODO: Encapsulate This Structure (Probably after finish implementing everything)
struct control_block_t {
    page_t* frame;
    int64_t table_id;
    pagenum_t pagenum;
    int is_dirty;
    pthread_mutex_t page_latch;
    control_block_t* next;
    control_block_t* prev;
};

// Helper Functions
void move_to_beg_of_list(control_block_t* cur);
control_block_t* find_buffer(int64_t table_id, pagenum_t page_number);
control_block_t* find_victim();
control_block_t* add_new_page(int64_t table_id, pagenum_t page_number);
void free_page(int64_t table_id, pagenum_t page_number);

// APIs
int64_t buf_open_table_file(const char* pathname, int64_t tid);
void buf_return_ctrl_block(control_block_t** ctrl_block, int is_dirty = 0);
control_block_t* buf_read_page(int64_t table_id, pagenum_t page_number);
pagenum_t buf_alloc_page(int64_t table_id);
void buf_free_page(int64_t table_id, pagenum_t page_number);

int buf_init_db(int num_buf);
int buf_shutdown_db();

#endif //__BUFFER_H__