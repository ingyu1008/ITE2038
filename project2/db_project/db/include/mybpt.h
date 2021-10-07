#ifndef __MYBPT_H__
#define __MYBPT_H__

#include "page.h"
#include "file.h"

constexpr uint64_t NODE_MAX_KEYS = (PAGE_SIZE - PH_SIZE) / BRANCH_FACTOR_SIZE;
constexpr uint16_t MAX_VAL_SIZE = 112;

// Functions

// Find Operations

pagenum_t find_leaf(int64_t table_id, pagenum_t root_pagenum, int64_t key);
int find(int64_t table_id, pagenum_t root_pagenum, int64_t key, char* ret_val, uint16_t* val_size);

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

// TODO : implementation


// API
int64_t open_table(char* pathname);

int db_insert(int64_t table_id, int64_t key, char *value, uint16_t val_size);
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t *val_size);


#endif // __MYBPT_H__