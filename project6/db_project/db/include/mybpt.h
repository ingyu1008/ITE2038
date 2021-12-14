#ifndef __MYBPT_H__
#define __MYBPT_H__

#include "page.h"
#include "file.h"
#include "buffer.h"
#include <set>


constexpr uint64_t NODE_MAX_KEYS = (PAGE_SIZE - PH_SIZE) / BRANCH_FACTOR_SIZE;
constexpr uint16_t MAX_VAL_SIZE = 112;
constexpr uint64_t THRESHHOLD = 2500;

// Functions

// Find Operations

pagenum_t find_leaf(int64_t table_id, pagenum_t root_pagenum, int64_t key);
int find(int64_t table_id, pagenum_t root_pagenum, int64_t key, char* ret_val, uint16_t* val_size, int trx_id);

// Insertion

pagenum_t make_node(int64_t table_id);
pagenum_t make_leaf(int64_t table_id);
int get_left_index(int64_t table_id, pagenum_t parent_pagenum, pagenum_t left_pagenum);
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
pagenum_t insert_into_node(int64_t table_id, pagenum_t root_pagenum, pagenum_t parent_pagenum, int left_index, int64_t key, pagenum_t right_pagenum);
pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root_pagenum, pagenum_t old_node_pagenum, int left_index, int64_t key, pagenum_t right_pagenum);
pagenum_t insert_into_parent(int64_t table_id, pagenum_t root_pagenum, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
pagenum_t insert_into_leaf(int64_t table_id, pagenum_t leaf_pagenum, int64_t key, const char* data, uint16_t size);
pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root_pagenum, pagenum_t leaf_pagenum, int64_t key, const char* data, uint16_t data_size);
pagenum_t start_new_tree(int64_t table_id, int64_t key, const char* data, uint16_t size);
pagenum_t insert(int64_t table_id, pagenum_t root_pagenum, int64_t key, const char* data, uint16_t sz);

// Deletion

int get_neighbor_index(int64_t table_id, pagenum_t pagenum);
pagenum_t remove_entry_from_internal(int64_t table_id, pagenum_t internal_pagenum, int64_t key);
pagenum_t remove_entry_from_leaf(int64_t table_id, pagenum_t leaf_pagenum, int64_t key);
pagenum_t adjust_root(int64_t table_id, pagenum_t root_pagenum);
pagenum_t merge_internal(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int64_t key);
pagenum_t merge_leaf(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int64_t key);
pagenum_t redistribute_internal(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int key_index, int64_t key);
pagenum_t redistribute_leaf(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int key_index, int64_t key);
pagenum_t delete_entry(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, int64_t key);
pagenum_t _delete(int64_t table_id, pagenum_t root_pagenum, int64_t key);

// API
int64_t open_table(char* pathname);
int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size);
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size);
int db_delete(int64_t table_id, int64_t key);
int init_db(int num_buf);
int shutdown_db();

void db_print_tree(int64_t table_id);

// Newly Added Helper Function
int update(int64_t table_id, pagenum_t root_pagenum, int64_t key, char* value, uint16_t val_size, uint16_t* old_val_size, int trx_id);
int acquire_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int trx_id, int lock_mode);

// Newly Added API from Project 5
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id);
int db_update(int64_t table_id, int64_t key, char* value, uint16_t val_size, uint16_t* old_val_size, int trx_id);


#endif // __MYBPT_H__