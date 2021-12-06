#include "mybpt.h"
#include "page.h"
#include "buffer.h"
#include "lock_table.h"
#include "trx.h"
#define DEBUG_MODE 0

// Find Operations

/* Finds the leaf node's pagenum containing given key. */
pagenum_t find_leaf(int64_t table_id, pagenum_t root_pagenum, int64_t key) {
    pagenum_t cur = root_pagenum;

    if (cur == 0) {
        return cur;
    }

    control_block_t* ctrl_block = buf_read_page(table_id, cur);

    while (PageIO::BPT::get_is_leaf(ctrl_block->frame) == 0) // While the page is internal
    {
        int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
        int i = num_keys;

        int lo = 0;
        int hi = num_keys - 1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (key < PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, mid).get_key()) {
                hi = mid - 1;
                i = mid;
            } else {
                lo = mid + 1;
            }
        }

        if (i == 0) {
            cur = PageIO::BPT::InternalPage::get_leftmost_pagenum(ctrl_block->frame);
        } else {
            cur = PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, i - 1).get_pagenum();
        }

        buf_return_ctrl_block(&ctrl_block);

        ctrl_block = buf_read_page(table_id, cur);
        // file_read_page(table_id, cur, &page);
    }
    buf_return_ctrl_block(&ctrl_block);
    return cur;
}

int find(int64_t table_id, pagenum_t root_pagenum, int64_t key, char* ret_val, uint16_t* val_size, int trx_id = 0) {
    #if DEBUG_MODE
    std::cout << "[INFO] find() called. key = " << key << std::endl;
    #endif
    * val_size = 0;
    pagenum_t leaf = find_leaf(table_id, root_pagenum, key);

    if (leaf == 0) return 1;

    control_block_t* ctrl_block = buf_read_page(table_id, leaf);
    // file_read_page(table_id, leaf, &page);

    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    int i = num_keys;
    slot_t slot;
    // for (i = 0; i < num_keys; i++) {
    //     slot = PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, i);
    //     if (slot.get_key() == key) break;
    // }

    int lo = 0;
    int hi = num_keys - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        slot = PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, mid);
        if (slot.get_key() == key) {
            i = mid;
            break;
        } else if (slot.get_key() > key) {
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }

    if (i == num_keys) {
        buf_return_ctrl_block(&ctrl_block);
        return 1;
    }


    if (trx_id > 0) {
        // buf_return_ctrl_block(&ctrl_block);

        if(!lock_exist(table_id, leaf, i, trx_id)){
            // ctrl_block = buf_read_page(table_id, leaf);
            int trx_written = slot.get_trx_id();
            
            trx_implicit_to_explicit(table_id, leaf, i, trx_id, trx_written);
        }
        
        buf_return_ctrl_block(&ctrl_block);

        int res = acquire_lock(table_id, leaf, i, trx_id, 0);
        if(res < 0 ) return -1;

        ctrl_block = buf_read_page(table_id, leaf);
    }

    *val_size = slot.get_size();
    ctrl_block->frame->get_data(ret_val, slot.get_offset(), slot.get_size());

    buf_return_ctrl_block(&ctrl_block);
    return 0;
}

// Insertion

/* Allocates a page, which can be adapted
 * to serve as either a leaf or an internal page.
 */
pagenum_t make_node(int64_t table_id) {
    pagenum_t pagenum = buf_alloc_page(table_id);

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    // file_read_page(table_id, pagenum, &page);

    PageIO::BPT::set_parent_pagenum(ctrl_block->frame, 0);
    PageIO::BPT::set_is_leaf(ctrl_block->frame, 0);
    PageIO::BPT::set_num_keys(ctrl_block->frame, 0);

    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, pagenum, &page);
    return pagenum;
}

/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
pagenum_t make_leaf(int64_t table_id) {
    pagenum_t pagenum = make_node(table_id);

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    // file_read_page(table_id, pagenum, &page);
    PageIO::BPT::set_is_leaf(ctrl_block->frame, 1);
    PageIO::BPT::LeafPage::set_amount_free_space(ctrl_block->frame, INITIAL_FREE_SPACE);
    buf_return_ctrl_block(&ctrl_block, 1);
    return pagenum;
}

/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to
 * the node to the left of the key to be inserted.
 */
int get_left_index(int64_t table_id, pagenum_t parent_pagenum, pagenum_t left_pagenum) {
    // page_t page;
    // file_read_page(table_id, parent_pagenum, &page);
    control_block_t* ctrl_block = buf_read_page(table_id, parent_pagenum);
    if (PageIO::BPT::InternalPage::get_leftmost_pagenum(ctrl_block->frame) == left_pagenum) {
        buf_return_ctrl_block(&ctrl_block);
        return -1;
    }
    int ret = 0;
    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    while (ret < num_keys && PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, ret).get_pagenum() != left_pagenum) {
        ret++;
    }
    buf_return_ctrl_block(&ctrl_block);
    return ret;
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {
    pagenum_t root_pagenum = make_node(table_id);
    // page_t root;
    // file_read_page(table_id, root_pagenum, &root);
    control_block_t* ctrl_block = buf_read_page(table_id, root_pagenum);
    PageIO::BPT::InternalPage::set_leftmost_pagenum(ctrl_block->frame, left_pagenum);
    branch_factor_t temp;
    temp.set_key(key);
    temp.set_pagenum(right_pagenum);
    PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, 0, temp);
    PageIO::BPT::set_num_keys(ctrl_block->frame, 1);
    buf_return_ctrl_block(&ctrl_block, 1);

    ctrl_block = buf_read_page(table_id, left_pagenum);
    // page_t page;
    // file_read_page(table_id, left_pagenum, &page);
    PageIO::BPT::set_parent_pagenum(ctrl_block->frame, root_pagenum);
    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, left_pagenum, &page);

    ctrl_block = buf_read_page(table_id, right_pagenum);
    // file_read_page(table_id, right_pagenum, &page);
    PageIO::BPT::set_parent_pagenum(ctrl_block->frame, root_pagenum);
    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, right_pagenum, &page);

    return root_pagenum;
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
pagenum_t insert_into_node(int64_t table_id, pagenum_t root_pagenum, pagenum_t parent_pagenum, int left_index, int64_t key, pagenum_t right_pagenum) {
    control_block_t* ctrl_block = buf_read_page(table_id, parent_pagenum);
    // page_t page;
    // file_read_page(table_id, parent_pagenum, &page);
    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    PageIO::BPT::set_num_keys(ctrl_block->frame, num_keys + 1);


    for (int i = num_keys; i > left_index; i--) {
        PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, i, PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, i - 1));
    }

    branch_factor_t new_branch_factor;
    new_branch_factor.set_key(key);
    new_branch_factor.set_pagenum(right_pagenum);

    PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, left_index + 1, new_branch_factor);
    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, parent_pagenum, &page);

    return root_pagenum;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root_pagenum, pagenum_t old_node_pagenum, int left_index, int64_t key, pagenum_t right_pagenum) {
    std::vector<std::pair<int64_t, pagenum_t>> branch_factors;

    control_block_t* ctrl_block = buf_read_page(table_id, old_node_pagenum);
    // page_t page;
    // file_read_page(table_id, old_node_pagenum, &page);
    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);

    int i, j;
    for (i = 0; i < num_keys; i++) {
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, i);
        branch_factors.emplace_back(branch_factor.get_key(), branch_factor.get_pagenum());
    }
    branch_factors.emplace_back(key, right_pagenum);
    std::sort(branch_factors.begin(), branch_factors.end());

    if (branch_factors.size() != NODE_MAX_KEYS + 1) {
        // This should never happen
        std::cout << "[FATAL] node split when its not supposed to, the node size equals " << branch_factors.size() << std::endl;
        exit(1);
    }

    int split = (num_keys + 1) / 2;

    PageIO::BPT::set_num_keys(ctrl_block->frame, split);
    for (i = 0; i < split; i++) {
        branch_factor_t branch_factor;
        branch_factor.set_key(branch_factors[i].first);
        branch_factor.set_pagenum(branch_factors[i].second);
        PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, i, branch_factor);
    }

    pagenum_t par_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
    int64_t prop_key = branch_factors[split].first;

    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, old_node_pagenum, &page);


    pagenum_t new_node_pagenum = make_node(table_id);
    ctrl_block = buf_read_page(table_id, new_node_pagenum);
    // file_read_page(table_id, new_node_pagenum, &page);

    PageIO::BPT::InternalPage::set_leftmost_pagenum(ctrl_block->frame, branch_factors[split].second);

    control_block_t* child_ctrl_block = buf_read_page(table_id, branch_factors[split].second);
    // page_t child;
    // file_read_page(table_id, branch_factors[split].second, &child);
    PageIO::BPT::set_parent_pagenum(child_ctrl_block->frame, new_node_pagenum);
    buf_return_ctrl_block(&child_ctrl_block, 1);
    // file_write_page(table_id, branch_factors[split].second, &child);

    PageIO::BPT::set_parent_pagenum(ctrl_block->frame, par_pagenum);
    PageIO::BPT::set_num_keys(ctrl_block->frame, num_keys - split);

    int st = ++i;

    for (i = st, j = 0; i < branch_factors.size(); i++, j++) {
        branch_factor_t branch_factor;
        branch_factor.set_key(branch_factors[i].first);
        branch_factor.set_pagenum(branch_factors[i].second);
        PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, j, branch_factor);

        // child_ctrl_block = buf_read_page(table_id, branch_factors[i].second);
        // // file_read_page(table_id, branch_factors[i].second, &child);
        // PageIO::BPT::set_parent_pagenum(child_ctrl_block->frame, new_node_pagenum);
        // buf_return_ctrl_block(&child_ctrl_block, 1);
        // // file_write_page(table_id, branch_factors[i].second, &child);
    }
    buf_return_ctrl_block(&ctrl_block, 1);

    for (i = st, j = 0; i < branch_factors.size(); i++, j++) {
        // branch_factor_t branch_factor;
        // branch_factor.set_key(branch_factors[i].first);
        // branch_factor.set_pagenum(branch_factors[i].second);
        // PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, j, branch_factor);

        child_ctrl_block = buf_read_page(table_id, branch_factors[i].second);
        // file_read_page(table_id, branch_factors[i].second, &child);
        PageIO::BPT::set_parent_pagenum(child_ctrl_block->frame, new_node_pagenum);
        buf_return_ctrl_block(&child_ctrl_block, 1);
        // file_write_page(table_id, branch_factors[i].second, &child);
    }
    // file_write_page(table_id, new_node_pagenum, ctrl_block->frame);

    return insert_into_parent(table_id, root_pagenum, old_node_pagenum, prop_key, new_node_pagenum);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(int64_t table_id, pagenum_t root_pagenum, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {
    // page_t left;
    // file_read_page(table_id, left_pagenum, &left);
    control_block_t* ctrl_block = buf_read_page(table_id, left_pagenum);
    pagenum_t par_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
    buf_return_ctrl_block(&ctrl_block);

    if (par_pagenum == 0) {
        return insert_into_new_root(table_id, left_pagenum, key, right_pagenum);
    }

    int left_index = get_left_index(table_id, par_pagenum, left_pagenum);

    // page_t page;
    // file_read_page(table_id, par_pagenum, &page);
    ctrl_block = buf_read_page(table_id, par_pagenum);
    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    buf_return_ctrl_block(&ctrl_block);

    if (num_keys < NODE_MAX_KEYS) {
        return insert_into_node(table_id, root_pagenum, par_pagenum, left_index, key, right_pagenum);
    }

    return insert_into_node_after_splitting(table_id, root_pagenum, par_pagenum, left_index, key, right_pagenum);
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
pagenum_t insert_into_leaf(int64_t table_id, pagenum_t leaf_pagenum, int64_t key, const char* data, uint16_t size) {
    control_block_t* ctrl_block = buf_read_page(table_id, leaf_pagenum);
    // page_t leaf;
    // file_read_page(table_id, leaf_pagenum, &leaf);
    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);

    int insertion_point = 0;
    // while (insertion_point < num_keys && PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, insertion_point).get_key() < key) {
    //     insertion_point++;
    // }
    int lo = 0;
    int hi = num_keys - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, mid).get_key() < key) {
            lo = mid + 1;
            insertion_point = mid + 1;
        } else {
            hi = mid - 1;
        }
    }

    uint64_t amount_free_space = PageIO::BPT::LeafPage::get_amount_free_space(ctrl_block->frame);
    uint16_t offset = amount_free_space + PH_SIZE + num_keys * SLOT_SIZE - size;

    PageIO::BPT::set_num_keys(ctrl_block->frame, num_keys + 1);
    for (int i = num_keys; i > insertion_point; i--) {
        PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, i, PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, i - 1));
    }

    PageIO::BPT::LeafPage::set_amount_free_space(ctrl_block->frame, amount_free_space - SLOT_SIZE - size);

    slot_t slot;
    slot.set_key(key);
    slot.set_offset(offset);
    slot.set_size(size);
    slot.set_trx_id(0);
    PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, insertion_point, slot);

    ctrl_block->frame->set_data(data, offset, size);
    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, leaf_pagenum, &leaf);

    return leaf_pagenum;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root_pagenum, pagenum_t leaf_pagenum, int64_t key, const char* data, uint16_t data_size) {
    control_block_t* ctrl_block = buf_read_page(table_id, leaf_pagenum);
    page_t leaf;
    leaf.set_data(reinterpret_cast<char*>(ctrl_block->frame), 0, PAGE_SIZE);

    // page_t left, leaf;
    // file_read_page(table_id, leaf_pagenum, &left);
    // file_read_page(table_id, leaf_pagenum, &leaf);

    pagenum_t right_pagenum = make_leaf(table_id);
    control_block_t* right_ctrl_block = buf_read_page(table_id, right_pagenum);
    // page_t right;
    // file_read_page(table_id, right_pagenum, &right);

    PageIO::BPT::LeafPage::set_right_sibling_pagenum(right_ctrl_block->frame, PageIO::BPT::LeafPage::get_right_sibling_pagenum(&leaf));
    PageIO::BPT::LeafPage::set_right_sibling_pagenum(ctrl_block->frame, right_pagenum);
    PageIO::BPT::set_parent_pagenum(right_ctrl_block->frame, PageIO::BPT::get_parent_pagenum(&leaf));

    int num_keys = PageIO::BPT::get_num_keys(&leaf);

    std::vector<std::pair<uint64_t, int>> keys; // key, slot index in original node, negative index for new key

    int i, j;
    for (i = 0; i < num_keys; i++) {
        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&leaf, i);
        keys.emplace_back(slot.get_key(), i);
    }
    keys.emplace_back(key, -1);
    std::sort(keys.begin(), keys.end());

    // Does the left part
    uint64_t free_space = INITIAL_FREE_SPACE;
    for (i = 0; i < keys.size(); i++) {
        uint16_t size;
        char buffer[MAX_VAL_SIZE];

        if (keys[i].second >= 0) {
            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&leaf, keys[i].second);
            size = slot.get_size();
            leaf.get_data(buffer, slot.get_offset(), size);
        } else {
            size = data_size;
            memcpy(buffer, data, size);
        }

        if (free_space - SLOT_SIZE - size <= INITIAL_FREE_SPACE / 2) {
            PageIO::BPT::set_num_keys(ctrl_block->frame, i);
            PageIO::BPT::LeafPage::set_amount_free_space(ctrl_block->frame, free_space);
            buf_return_ctrl_block(&ctrl_block, 1);
            // file_write_page(table_id, leaf_pagenum, &left);
            break;
        }

        uint16_t offset = free_space + PH_SIZE + i * SLOT_SIZE - size;
        slot_t slot;
        slot.set_key(keys[i].first);
        slot.set_offset(offset);
        slot.set_size(size);
        slot.set_trx_id(0);
        PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, i, slot);
        ctrl_block->frame->set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }

    // Does the right part

    free_space = INITIAL_FREE_SPACE;
    for (j = 0; i < keys.size(); i++, j++) {
        uint16_t size;
        char buffer[MAX_VAL_SIZE];

        if (keys[i].second >= 0) {
            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&leaf, keys[i].second);
            size = slot.get_size();
            leaf.get_data(buffer, slot.get_offset(), size);
        } else {
            size = data_size;
            memcpy(buffer, data, size);
        }

        uint16_t offset = free_space + PH_SIZE + j * SLOT_SIZE - size;
        slot_t slot;
        slot.set_key(keys[i].first);
        slot.set_offset(offset);
        slot.set_size(size);
        slot.set_trx_id(0);
        PageIO::BPT::LeafPage::set_nth_slot(right_ctrl_block->frame, j, slot);
        right_ctrl_block->frame->set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }

    PageIO::BPT::set_num_keys(right_ctrl_block->frame, j);
    PageIO::BPT::LeafPage::set_amount_free_space(right_ctrl_block->frame, free_space);

    key = PageIO::BPT::LeafPage::get_nth_slot(right_ctrl_block->frame, 0).get_key();

    buf_return_ctrl_block(&right_ctrl_block, 1);
    // file_write_page(table_id, right_pagenum, &right);

    return insert_into_parent(table_id, root_pagenum, leaf_pagenum, key, right_pagenum);
}

/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(int64_t table_id, int64_t key, const char* data, uint16_t size) {
    pagenum_t root_pagenum = make_leaf(table_id);

    control_block_t* ctrl_block = buf_read_page(table_id, root_pagenum);
    // page_t root;
    // file_read_page(table_id, root_pagenum, &root);
    PageIO::BPT::set_num_keys(ctrl_block->frame, 1);
    PageIO::BPT::set_parent_pagenum(ctrl_block->frame, 0);

    uint16_t offset = INITIAL_FREE_SPACE + PH_SIZE - size;
    slot_t slot;
    slot.set_key(key);
    slot.set_offset(offset);
    slot.set_size(size);
    slot.set_trx_id(0);
    PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, 0, slot);
    ctrl_block->frame->set_data(data, offset, size);
    PageIO::BPT::LeafPage::set_amount_free_space(ctrl_block->frame, INITIAL_FREE_SPACE - size - SLOT_SIZE);
    PageIO::BPT::LeafPage::set_right_sibling_pagenum(ctrl_block->frame, 0);
    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, root_pagenum, &root);

    #if DEBUG_MODE
    std::cout << "[DEBUG] start_new_tree() returns: " << root_pagenum << std::endl;
    #endif
    return root_pagenum;
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
pagenum_t insert(int64_t table_id, pagenum_t root_pagenum, int64_t key, const char* data, uint16_t sz) {
    char buffer[MAX_VAL_SIZE];
    uint16_t size;

    /* The current implementation ignores
     * duplicates.
     *
     * This was dealt with wrapper funciton
     */
     // if (find(table_id, root_pagenum, key, buffer, &size) == 0) {
     //     // This can be checked beforehand in db_insert().
     //     return root_pagenum;
     // }

     /* Case: the tree does not exist yet.
      * Start a new tree.
      */
    if (root_pagenum == 0) {
        return start_new_tree(table_id, key, data, sz);
    }

    /* Case: the tree already exists.
     * (Rest of function body.)
     */
    pagenum_t leaf_pagenum = find_leaf(table_id, root_pagenum, key);

    control_block_t* ctrl_block = buf_read_page(table_id, leaf_pagenum);
    // page_t leaf;
    // file_read_page(table_id, leaf_pagenum, &leaf);

    if (PageIO::BPT::LeafPage::get_amount_free_space(ctrl_block->frame) > SLOT_SIZE + sz) {
        buf_return_ctrl_block(&ctrl_block);
        insert_into_leaf(table_id, leaf_pagenum, key, data, sz);
        return root_pagenum;
    }
    buf_return_ctrl_block(&ctrl_block);
    /* Case:  leaf must be split.
     */
    return insert_into_leaf_after_splitting(table_id, root_pagenum, leaf_pagenum, key, data, sz);
}

// Deletion

int get_neighbor_index(int64_t table_id, pagenum_t pagenum) {

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    // page_t page;
    // file_read_page(table_id, pagenum, &page);

    pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
    buf_return_ctrl_block(&ctrl_block);

    control_block_t* par_ctrl_block = buf_read_page(table_id, parent_pagenum);
    // page_t parent;
    // file_read_page(table_id, parent_pagenum, &parent);
    if (PageIO::BPT::InternalPage::get_leftmost_pagenum(par_ctrl_block->frame) == pagenum) {
        int num_keys = PageIO::BPT::get_num_keys(par_ctrl_block->frame);
        buf_return_ctrl_block(&par_ctrl_block);
        return -1; // Special Case: Leftmost page
    } else {
        int num_keys = PageIO::BPT::get_num_keys(par_ctrl_block->frame);

        for (int i = 0; i < num_keys; i++) {
            if (PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, i).get_pagenum() == pagenum) {
                buf_return_ctrl_block(&par_ctrl_block);
                return i;
            }
        }
    }

    buf_return_ctrl_block(&par_ctrl_block);
    throw std::invalid_argument("Invalid Argument at get_neighbor_index()");
}

pagenum_t remove_entry_from_internal(int64_t table_id, pagenum_t internal_pagenum, int64_t key) {
    control_block_t* ctrl_block = buf_read_page(table_id, internal_pagenum);
    // page_t page;
    // file_read_page(table_id, internal_pagenum, &page);

    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);

    for (int i = 0; i < num_keys; i++) {
        branch_factor_t ith = PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, i);
        if (ith.get_key() <= key) {
            continue;
        }

        PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, i - 1, ith);
    }
    PageIO::BPT::set_num_keys(ctrl_block->frame, num_keys - 1);
    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, internal_pagenum, &page);
    return internal_pagenum;
}

pagenum_t remove_entry_from_leaf(int64_t table_id, pagenum_t leaf_pagenum, int64_t key) {
    control_block_t* ctrl_block = buf_read_page(table_id, leaf_pagenum);
    page_t copy;
    copy.set_data(reinterpret_cast<char*>(ctrl_block->frame), 0, PAGE_SIZE);

    // page_t page, copy;
    // file_read_page(table_id, leaf_pagenum, &page);
    // file_read_page(table_id, leaf_pagenum, &copy);

    // Almost copy and paste part from insert_into_leaf_after_splitting()
    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);

    std::vector<std::pair<uint64_t, int>> keys; // key, slot index

    int i;
    for (i = 0; i < num_keys; i++) {
        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&copy, i);
        if (slot.get_key() == key) continue; // skip the key deleting
        keys.emplace_back(slot.get_key(), i);
    }

    uint64_t free_space = INITIAL_FREE_SPACE;
    for (i = 0; i < keys.size(); i++) {
        uint16_t size;
        char buffer[MAX_VAL_SIZE];

        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&copy, keys[i].second);
        size = slot.get_size();
        copy.get_data(buffer, slot.get_offset(), size);

        uint16_t offset = free_space + PH_SIZE + i * SLOT_SIZE - size;
        slot.set_offset(offset);
        PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, i, slot);
        ctrl_block->frame->set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }

    PageIO::BPT::LeafPage::set_amount_free_space(ctrl_block->frame, free_space);
    PageIO::BPT::set_num_keys(ctrl_block->frame, num_keys - 1);
    buf_return_ctrl_block(&ctrl_block, 1);
    // file_write_page(table_id, leaf_pagenum, &page);
    return leaf_pagenum;
}

pagenum_t adjust_root(int64_t table_id, pagenum_t root_pagenum) {
    control_block_t* ctrl_block = buf_read_page(table_id, root_pagenum);
    // page_t root;
    // file_read_page(table_id, root_pagenum, &root);

    // Nothing to do
    if (PageIO::BPT::get_num_keys(ctrl_block->frame) > 0) {
        buf_return_ctrl_block(&ctrl_block);
        return root_pagenum;
    }

    pagenum_t new_root_pagenum = 0;

    if (PageIO::BPT::get_is_leaf(ctrl_block->frame) == 0) {
        new_root_pagenum = PageIO::BPT::InternalPage::get_leftmost_pagenum(ctrl_block->frame);
        buf_return_ctrl_block(&ctrl_block);
        ctrl_block = buf_read_page(table_id, new_root_pagenum);
        PageIO::BPT::set_parent_pagenum(ctrl_block->frame, 0);
        buf_return_ctrl_block(&ctrl_block, 1);
        // file_write_page(table_id, new_root_pagenum, &root);
    } else {
        buf_return_ctrl_block(&ctrl_block);
    }

    buf_free_page(table_id, root_pagenum);
    // file_free_page(table_id, root_pagenum);
    return new_root_pagenum;
}

pagenum_t merge_internal(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int64_t key) {
    if (neighbor_index == -1) {
        std::swap(pagenum, neighbor_pagenum);
    }

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    control_block_t* neighbor_ctrl_block = buf_read_page(table_id, neighbor_pagenum);
    // page_t page, neighbor;
    // file_read_page(table_id, pagenum, &page);
    // file_read_page(table_id, neighbor_pagenum, &neighbor);

    int neighbor_insertion_index = PageIO::BPT::get_num_keys(neighbor_ctrl_block->frame);

    branch_factor_t branch_factor;
    branch_factor.set_key(key);
    branch_factor.set_pagenum(PageIO::BPT::InternalPage::get_leftmost_pagenum(ctrl_block->frame));

    PageIO::BPT::InternalPage::set_nth_branch_factor(neighbor_ctrl_block->frame, neighbor_insertion_index, branch_factor);

    int i, j, page_num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    for (i = neighbor_insertion_index + 1, j = 0; j < page_num_keys; i++, j++) {
        PageIO::BPT::InternalPage::set_nth_branch_factor(neighbor_ctrl_block->frame, i, PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, j));
    }

    PageIO::BPT::set_num_keys(neighbor_ctrl_block->frame, i);


    control_block_t* child_ctrl_block;
    pagenum_t child_pagenum;
    for (j = 0; j < i; j++) {
        child_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(neighbor_ctrl_block->frame, j).get_pagenum();
        child_ctrl_block = buf_read_page(table_id, child_pagenum);
        // file_read_page(table_id, child_pagenum, &child);
        PageIO::BPT::set_parent_pagenum(child_ctrl_block->frame, neighbor_pagenum);
        buf_return_ctrl_block(&child_ctrl_block, 1);
        // file_write_page(table_id, child_pagenum, &child);
    }

    buf_return_ctrl_block(&neighbor_ctrl_block, 1);
    // file_write_page(table_id, neighbor_pagenum, &neighbor);
    pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
    buf_return_ctrl_block(&ctrl_block);
    root_pagenum = delete_entry(table_id, root_pagenum, parent_pagenum, key);

    buf_free_page(table_id, pagenum);
    return root_pagenum;
}

pagenum_t merge_leaf(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int64_t key) {
    // Almost copy and paste from merge_internal()
    if (neighbor_index == -1) {
        std::swap(pagenum, neighbor_pagenum);
    }

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    control_block_t* neighbor_ctrl_block = buf_read_page(table_id, neighbor_pagenum);

    // page_t page, neighbor;
    // file_read_page(table_id, pagenum, &page);
    // file_read_page(table_id, neighbor_pagenum, &neighbor);

    int neighbor_insertion_index = PageIO::BPT::get_num_keys(neighbor_ctrl_block->frame);

    int i, j, page_num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);

    uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(neighbor_ctrl_block->frame);

    for (i = neighbor_insertion_index, j = 0; j < page_num_keys; i++, j++) {
        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, j);
        char buffer[MAX_VAL_SIZE];
        uint16_t size;

        size = slot.get_size();
        ctrl_block->frame->get_data(buffer, slot.get_offset(), size);

        uint16_t offset = free_space + PH_SIZE + i * SLOT_SIZE - size;
        slot.set_offset(offset);
        PageIO::BPT::LeafPage::set_nth_slot(neighbor_ctrl_block->frame, i, slot);
        neighbor_ctrl_block->frame->set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }
    PageIO::BPT::set_num_keys(neighbor_ctrl_block->frame, i);
    PageIO::BPT::LeafPage::set_amount_free_space(neighbor_ctrl_block->frame, free_space);
    buf_return_ctrl_block(&neighbor_ctrl_block, 1);
    // file_write_page(table_id, neighbor_pagenum, &neighbor);

    pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
    buf_return_ctrl_block(&ctrl_block);
    root_pagenum = delete_entry(table_id, root_pagenum, parent_pagenum, key);

    buf_free_page(table_id, pagenum);
    // file_free_page(table_id, pagenum);
    return root_pagenum;
}

pagenum_t redistribute_internal(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int key_index, int64_t key) {
    // Note that there is no change in the tree structure

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    control_block_t* neighbor_ctrl_block = buf_read_page(table_id, neighbor_pagenum);
    // page_t page, neighbor;
    // file_read_page(table_id, pagenum, &page);
    // file_read_page(table_id, neighbor_pagenum, &neighbor);

    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    int neighbor_num_keys = PageIO::BPT::get_num_keys(neighbor_ctrl_block->frame);

    if (neighbor_index >= 0) {
        // Case where neighbor is on the node's left
        // Pull the last one from the left
        for (int i = num_keys; i > 0; i--) {
            PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, i, PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, i - 1));
        }
        branch_factor_t branch_factor;
        branch_factor.set_key(key);
        branch_factor.set_pagenum(PageIO::BPT::InternalPage::get_leftmost_pagenum(ctrl_block->frame));
        PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, 0, branch_factor);

        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(neighbor_ctrl_block->frame, neighbor_num_keys - 1);

        PageIO::BPT::InternalPage::set_leftmost_pagenum(ctrl_block->frame, branch_factor.get_pagenum());

        // page_t child;
        control_block_t* child_ctrl_block = buf_read_page(table_id, branch_factor.get_pagenum());
        // file_read_page(table_id, branch_factor.get_pagenum(), &child);
        PageIO::BPT::set_parent_pagenum(child_ctrl_block->frame, pagenum);
        buf_return_ctrl_block(&child_ctrl_block, 1);
        // file_write_page(table_id, branch_factor.get_pagenum(), &child);

        int64_t key_prime = branch_factor.get_key();

        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
        // page_t parent;

        control_block_t* par_ctrl_block = buf_read_page(table_id, parent_pagenum);
        // file_read_page(table_id, parent_pagenum, &parent);
        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, key_index);
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(par_ctrl_block->frame, key_index, branch_factor);
        buf_return_ctrl_block(&par_ctrl_block, 1);
        // file_write_page(table_id, parent_pagenum, &parent);
    } else {
        // Case where neighbor is on the node's right
        // Pull the first one from the right 

        branch_factor_t branch_factor;
        branch_factor.set_key(key);
        branch_factor.set_pagenum(PageIO::BPT::InternalPage::get_leftmost_pagenum(neighbor_ctrl_block->frame));
        PageIO::BPT::InternalPage::set_nth_branch_factor(ctrl_block->frame, num_keys, branch_factor);


        // page_t child;
        control_block_t* child_ctrl_block = buf_read_page(table_id, branch_factor.get_pagenum());
        // file_read_page(table_id, branch_factor.get_pagenum(), &child);
        PageIO::BPT::set_parent_pagenum(child_ctrl_block->frame, pagenum);
        buf_return_ctrl_block(&child_ctrl_block, 1);
        // file_write_page(table_id, branch_factor.get_pagenum(), &child);

        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(neighbor_ctrl_block->frame, 0);

        int64_t key_prime = branch_factor.get_key();
        PageIO::BPT::InternalPage::set_leftmost_pagenum(neighbor_ctrl_block->frame, branch_factor.get_pagenum());


        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
        control_block_t* par_ctrl_block = buf_read_page(table_id, parent_pagenum);
        // page_t parent;
        // file_read_page(table_id, parent_pagenum, &parent);
        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, key_index);
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(par_ctrl_block->frame, key_index, branch_factor);
        buf_return_ctrl_block(&par_ctrl_block, 1);
        // file_write_page(table_id, parent_pagenum, &parent);

        for (int i = 1; i < neighbor_num_keys; i++) {
            PageIO::BPT::InternalPage::set_nth_branch_factor(neighbor_ctrl_block->frame, i - 1, PageIO::BPT::InternalPage::get_nth_branch_factor(neighbor_ctrl_block->frame, i));
        }
    }

    PageIO::BPT::set_num_keys(ctrl_block->frame, num_keys + 1);
    PageIO::BPT::set_num_keys(neighbor_ctrl_block->frame, neighbor_num_keys - 1);

    buf_return_ctrl_block(&ctrl_block, 1);
    buf_return_ctrl_block(&neighbor_ctrl_block, 1);
    // file_write_page(table_id, pagenum, &page);
    // file_write_page(table_id, neighbor_pagenum, &neighbor);

    return root_pagenum;
}

pagenum_t redistribute_leaf(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int key_index, int64_t key) {
    // Almost copy and paste from redistribute_internal()

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    control_block_t* neighbor_ctrl_block = buf_read_page(table_id, neighbor_pagenum);

    page_t /*page, neighbor,*/ neighbor_copy;
    // file_read_page(table_id, pagenum, &page);
    // file_read_page(table_id, neighbor_pagenum, &neighbor);
    // file_read_page(table_id, neighbor_pagenum, &neighbor_copy);
    neighbor_copy.set_data(reinterpret_cast<char*>(neighbor_ctrl_block->frame), 0, PAGE_SIZE);

    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    int neighbor_num_keys = PageIO::BPT::get_num_keys(neighbor_ctrl_block->frame);

    if (neighbor_index >= 0) {
        // Case where neighbor is on the node's left
        // Pull the last one from the left
        for (int i = num_keys; i > 0; i--) {
            PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, i, PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, i - 1));
        }

        {
            // Scoping this part to avoid variable name collision

            uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(ctrl_block->frame);

            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(neighbor_ctrl_block->frame, neighbor_num_keys - 1);
            char buffer[MAX_VAL_SIZE];
            uint16_t size;

            size = slot.get_size();
            neighbor_ctrl_block->frame->get_data(buffer, slot.get_offset(), size);

            uint16_t offset = free_space + PH_SIZE + num_keys * SLOT_SIZE - size;
            slot.set_offset(offset);
            PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, 0, slot);
            ctrl_block->frame->set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;

            PageIO::BPT::LeafPage::set_amount_free_space(ctrl_block->frame, free_space);
            // Note that values does not need to be sorted.
        }


        // This part sorts the neighbor page's values
        // It is neccessary becausse this ensures that the neighbor page is packed
        // Almost Copy-paste from insert_into_leaf_after_splitting()
        uint64_t free_space = INITIAL_FREE_SPACE;
        for (int i = 0; i < neighbor_num_keys - 1; i++) {
            uint16_t size;
            char buffer[MAX_VAL_SIZE];

            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&neighbor_copy, i);
            size = slot.get_size();
            neighbor_copy.get_data(buffer, slot.get_offset(), size);

            uint16_t offset = free_space + PH_SIZE + i * SLOT_SIZE - size;
            slot.set_offset(offset);
            PageIO::BPT::LeafPage::set_nth_slot(neighbor_ctrl_block->frame, i, slot);
            neighbor_ctrl_block->frame->set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;
        }

        PageIO::BPT::LeafPage::set_amount_free_space(neighbor_ctrl_block->frame, free_space);

        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, 0);

        int64_t key_prime = slot.get_key();

        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);

        control_block_t* par_ctrl_block = buf_read_page(table_id, parent_pagenum);
        // page_t parent;
        // file_read_page(table_id, parent_pagenum, &parent);
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, key_index);
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(par_ctrl_block->frame, key_index, branch_factor);
        buf_return_ctrl_block(&par_ctrl_block, 1);
        // file_write_page(table_id, parent_pagenum, &parent);
    } else {
        // Case where neighbor is on the node's right
        // Pull the first one from the right 

        {
            // Scoping this part to avoid variable name collision

            uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(ctrl_block->frame);

            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(neighbor_ctrl_block->frame, 0);
            char buffer[MAX_VAL_SIZE];
            uint16_t size;

            size = slot.get_size();
            neighbor_ctrl_block->frame->get_data(buffer, slot.get_offset(), size);

            uint16_t offset = free_space + PH_SIZE + num_keys * SLOT_SIZE - size;
            slot.set_offset(offset);
            PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, num_keys, slot);
            ctrl_block->frame->set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;

            PageIO::BPT::LeafPage::set_amount_free_space(ctrl_block->frame, free_space);
        }

        // This part sorts the neighbor page's values
        // It is neccessary becausse this ensures that the neighbor page is packed
        // Almost Copy-paste from insert_into_leaf_after_splitting()
        uint64_t free_space = INITIAL_FREE_SPACE;
        for (int i = 1; i < neighbor_num_keys; i++) {
            uint16_t size;
            char buffer[MAX_VAL_SIZE];

            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&neighbor_copy, i);
            size = slot.get_size();
            neighbor_copy.get_data(buffer, slot.get_offset(), size);

            uint16_t offset = free_space + PH_SIZE + (i - 1) * SLOT_SIZE - size;
            slot.set_offset(offset);
            PageIO::BPT::LeafPage::set_nth_slot(neighbor_ctrl_block->frame, i - 1, slot);
            neighbor_ctrl_block->frame->set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;
        }

        PageIO::BPT::LeafPage::set_amount_free_space(neighbor_ctrl_block->frame, free_space);

        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(neighbor_ctrl_block->frame, 0);

        int64_t key_prime = slot.get_key();

        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);

        control_block_t* par_ctrl_block = buf_read_page(table_id, parent_pagenum);

        // page_t parent;
        // file_read_page(table_id, parent_pagenum, &parent);
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, key_index);
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(par_ctrl_block->frame, key_index, branch_factor);
        buf_return_ctrl_block(&par_ctrl_block, 1);
        // file_write_page(table_id, parent_pagenum, &parent);
    }

    PageIO::BPT::set_num_keys(ctrl_block->frame, num_keys + 1);
    PageIO::BPT::set_num_keys(neighbor_ctrl_block->frame, neighbor_num_keys - 1);

    buf_return_ctrl_block(&ctrl_block, 1);
    buf_return_ctrl_block(&neighbor_ctrl_block, 1);

    // file_write_page(table_id, pagenum, &page);
    // file_write_page(table_id, neighbor_pagenum, &neighbor);

    return root_pagenum;
}

pagenum_t delete_entry(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, int64_t key) {
    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);
    // page_t page;
    // file_read_page(table_id, pagenum, &page);

    int is_leaf = PageIO::BPT::get_is_leaf(ctrl_block->frame);
    buf_return_ctrl_block(&ctrl_block);

    if (is_leaf) {
        pagenum = remove_entry_from_leaf(table_id, pagenum, key);
    } else {
        pagenum = remove_entry_from_internal(table_id, pagenum, key);
    }

    if (pagenum == root_pagenum) {
        return adjust_root(table_id, root_pagenum);
    }

    std::cout << "A" << std::endl;
    ctrl_block = buf_read_page(table_id, pagenum);
    // file_read_page(table_id, pagenum, &page);
    is_leaf = PageIO::BPT::get_is_leaf(ctrl_block->frame);

    if (is_leaf) {
        uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(ctrl_block->frame);

        buf_return_ctrl_block(&ctrl_block);
        if (free_space < THRESHHOLD) {
            return root_pagenum;
        }

        int neighbor_index = get_neighbor_index(table_id, pagenum);
        int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;

        ctrl_block = buf_read_page(table_id, pagenum);
        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
        control_block_t* par_ctrl_block = buf_read_page(table_id, parent_pagenum);
        // page_t parent;
        // file_read_page(table_id, parent_pagenum, &parent);
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, k_prime_index);
        int64_t k_prime = branch_factor.get_key();


        pagenum_t neighbor_pagenum;
        if (neighbor_index == -1) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, 0).get_pagenum();
        } else if (neighbor_index == 0) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_leftmost_pagenum(par_ctrl_block->frame);
        } else {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, neighbor_index - 1).get_pagenum();
        }

        buf_return_ctrl_block(&ctrl_block);
        buf_return_ctrl_block(&par_ctrl_block);

        control_block_t* neighbor_ctrl_block = buf_read_page(table_id, neighbor_pagenum);
        // page_t neighbor;
        // file_read_page(table_id, neighbor_pagenum, &neighbor);

        if (PageIO::BPT::LeafPage::get_amount_free_space(neighbor_ctrl_block->frame) + free_space >= INITIAL_FREE_SPACE) {
            buf_return_ctrl_block(&neighbor_ctrl_block);
            return merge_leaf(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime);
        } else {
            buf_return_ctrl_block(&neighbor_ctrl_block);
            do {
                buf_return_ctrl_block(&ctrl_block);
                par_ctrl_block = buf_read_page(table_id, parent_pagenum);
                // file_read_page(table_id, parent_pagenum, &parent);
                branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, k_prime_index);
                k_prime = branch_factor.get_key();
                buf_return_ctrl_block(&par_ctrl_block);

                root_pagenum = redistribute_leaf(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime_index, k_prime);
                ctrl_block = buf_read_page(table_id, pagenum);
                // file_read_page(table_id, pagenum, &check);
            } while (PageIO::BPT::LeafPage::get_amount_free_space(ctrl_block->frame) >= THRESHHOLD);

            buf_return_ctrl_block(&ctrl_block);
            return root_pagenum;
        }
    } else {
        int min_keys = NODE_MAX_KEYS / 2;
        int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);

        buf_return_ctrl_block(&ctrl_block);
        if (num_keys >= min_keys) {
            return root_pagenum;
        }
        // page_t parent;
        // file_read_page(table_id, parent_pagenum, &parent);

        int neighbor_index = get_neighbor_index(table_id, pagenum);
        int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;

        ctrl_block = buf_read_page(table_id, pagenum);
        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(ctrl_block->frame);
        control_block_t* par_ctrl_block = buf_read_page(table_id, parent_pagenum);

        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, k_prime_index);
        int64_t k_prime = branch_factor.get_key();
        pagenum_t neighbor_pagenum;
        if (neighbor_index == -1) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, 0).get_pagenum();
        } else if (neighbor_index == 0) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_leftmost_pagenum(par_ctrl_block->frame);
        } else {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(par_ctrl_block->frame, neighbor_index - 1).get_pagenum();
        }

        buf_return_ctrl_block(&ctrl_block);
        buf_return_ctrl_block(&par_ctrl_block);

        std::cout << "B" << std::endl;
        control_block_t* neighbor_ctrl_block = buf_read_page(table_id, neighbor_pagenum);
        // page_t neighbor;
        // file_read_page(table_id, neighbor_pagenum, &neighbor);

        if (PageIO::BPT::get_num_keys(neighbor_ctrl_block->frame) + num_keys < NODE_MAX_KEYS) {
            buf_return_ctrl_block(&neighbor_ctrl_block);
            return merge_internal(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime);
        } else {
            buf_return_ctrl_block(&neighbor_ctrl_block);
            return redistribute_internal(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime_index, k_prime);
        }
    }
}

pagenum_t _delete(int64_t table_id, pagenum_t root_pagenum, int64_t key) {
    char buffer[MAX_VAL_SIZE];
    uint16_t size;
    int err = find(table_id, root_pagenum, key, buffer, &size);
    pagenum_t leaf_pagenum = find_leaf(table_id, root_pagenum, key);

    if (err == 0 && leaf_pagenum != 0) {
        root_pagenum = delete_entry(table_id, root_pagenum, leaf_pagenum, key);
        return root_pagenum;
    }
    return -1;
}

// =================================================================================================
// API
namespace Util {
    std::set<std::string> opened_tables;
}

int64_t open_table(char* pathname) {
    if (Util::opened_tables.find(std::string(pathname)) != Util::opened_tables.end()) {
        return -1;
    }
    if (Util::opened_tables.size() == 19) { // Total number of tables is less than 20
        return -1;
    }

    int64_t table_id = buf_open_table_file(pathname);

    if (table_id < 0) return -1;

    Util::opened_tables.insert(std::string(pathname));

    return table_id;
}

int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    control_block_t* header_ctrl_block = buf_read_page(table_id, 0);

    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(header_ctrl_block->frame);
    buf_return_ctrl_block(&header_ctrl_block);

    char buffer[MAX_VAL_SIZE];
    uint16_t size;
    if (find(table_id, root_pagenum, key, buffer, &size) == 0) {
        return -1;
    }
    root_pagenum = insert(table_id, root_pagenum, key, value, val_size);

    header_ctrl_block = buf_read_page(table_id, 0);
    PageIO::HeaderPage::set_root_pagenum(header_ctrl_block->frame, root_pagenum);
    buf_return_ctrl_block(&header_ctrl_block, 1);

    return 0;
}

int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size) {
    control_block_t* header_ctrl_block = buf_read_page(table_id, 0);
    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(header_ctrl_block->frame);
    buf_return_ctrl_block(&header_ctrl_block);
    return find(table_id, root_pagenum, key, ret_val, val_size);
}

int db_delete(int64_t table_id, int64_t key) {
    control_block_t* header_ctrl_block = buf_read_page(table_id, 0);
    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(header_ctrl_block->frame);
    buf_return_ctrl_block(&header_ctrl_block);
    root_pagenum = _delete(table_id, root_pagenum, key);

    if (root_pagenum < 0) return -1;

    header_ctrl_block = buf_read_page(table_id, 0);
    PageIO::HeaderPage::set_root_pagenum(header_ctrl_block->frame, root_pagenum);
    buf_return_ctrl_block(&header_ctrl_block, 1);
    return 0;
}

int init_db(int num_buf) {
    if (num_buf < 3) num_buf = 3;
    int err = 0;
    err += buf_init_db(num_buf);
    err += init_lock_table();
    err += trx_init();
    return 0;
}

int shutdown_db() {
    Util::opened_tables.clear();
    buf_shutdown_db();
    shutdown_lock_table();
    trx_shutdown();
    return 0;
}

void print_tree(int64_t table_id, pagenum_t pagenum) {
    std::cout << std::endl;
    static int tabs = 0;
    for (int i = 0; i < tabs; i++) {
        std::cout << "\t";
    }

    control_block_t* ctrl_block = buf_read_page(table_id, pagenum);

    std::cout << "[Current=" << pagenum << "]" << " [Parent=" << PageIO::BPT::get_parent_pagenum(ctrl_block->frame) << "]" << " " << std::endl;

    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    int is_leaf = PageIO::BPT::get_is_leaf(ctrl_block->frame);

    tabs++;

    if (is_leaf) {
        for (int i = 0; i < num_keys; i++) {
            std::cout << PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, i).get_key() << " ";
        }
    } else {
        print_tree(table_id, PageIO::BPT::InternalPage::get_leftmost_pagenum(ctrl_block->frame));
        for (int i = 0; i < num_keys; i++) {
            print_tree(table_id, PageIO::BPT::InternalPage::get_nth_branch_factor(ctrl_block->frame, i).get_pagenum());
        }
    }
    tabs--;
    std::cout << std::endl;
    buf_return_ctrl_block(&ctrl_block);
}

/******************************************************************************/
/*** Newly Implemented from Project 5 ****************************************/
/******************************************************************************/

int acquire_lock(int64_t table_id, pagenum_t pagenum, int64_t key, int trx_id, int lock_mode){
    lock_t* lock = trx_get_lock(table_id, pagenum, key, trx_id, lock_mode);

    if (lock == nullptr) {
        // Lock compression // This does not work correctly
        // TODO: Debug
        // if(lock_mode == 0){
        //     lock = lock_acquire_compressed(table_id, pagenum, key, trx_id);
        //     if(lock != nullptr){
        //         trx_add_to_locks(trx_id, key, lock);
        //         return 0;
        //     }
        // }
        lock = lock_acquire(table_id, pagenum, key, trx_id, lock_mode);
        lock = trx_acquire(trx_id, lock);
        if (lock == nullptr)
            return -1;   
    }
    return 0;
}

int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id) {
    control_block_t* header_ctrl_block = buf_read_page(table_id, 0);
    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(header_ctrl_block->frame);
    buf_return_ctrl_block(&header_ctrl_block);
    int err = find(table_id, root_pagenum, key, ret_val, val_size, trx_id);

    if (err < 0) trx_abort(trx_id);
    return err;
}

int update(int64_t table_id, pagenum_t root_pagenum, int64_t key, char* value, uint16_t val_size, uint16_t* old_val_size, int trx_id) {
    pagenum_t leaf = find_leaf(table_id, root_pagenum, key);

    *old_val_size = 0;
    if (leaf == 0) return 1;

    control_block_t* ctrl_block = buf_read_page(table_id, leaf);

    int num_keys = PageIO::BPT::get_num_keys(ctrl_block->frame);
    int i = num_keys;
    slot_t slot;

    int lo = 0;
    int hi = num_keys - 1;

    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        slot = PageIO::BPT::LeafPage::get_nth_slot(ctrl_block->frame, mid);
        if (slot.get_key() == key) {
            i = mid;
            break;
        } else if (slot.get_key() < key) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    buf_return_ctrl_block(&ctrl_block);

    if (i == num_keys) {
        return 1;
    }

    if(lock_exist(table_id, leaf, i, trx_id)){
        int res = acquire_lock(table_id, leaf, i, trx_id, 1);
        if(res < 0){
            return -1;
        }
    } else {
        // TODO: implicit locking
        int trx_written = slot.get_trx_id();
        
        int res = trx_implicit_to_explicit(table_id, leaf, i, trx_id, trx_written);

        if(res){
            // Implicit to Explicit Failed
            // Create New Implicit Lock
            ctrl_block = buf_read_page(table_id, leaf);
            slot.set_trx_id(trx_id);
            PageIO::BPT::LeafPage::set_nth_slot(ctrl_block->frame, i, slot);
            buf_return_ctrl_block(&ctrl_block, 1);
        } else {
            // Implicit to Explicit Worked
            res = acquire_lock(table_id, leaf, i, trx_id, 1);
            if(res < 0){
                return -1;
            }
        }
    }

    auto opt = trx_find_log(table_id, leaf, i, trx_id);

    ctrl_block = buf_read_page(table_id, leaf);
    *old_val_size = slot.get_size();

    if(!opt.has_value()) {
        std::pair<uint16_t, char*> log;
        log.first = *old_val_size;
        log.second = (char*)malloc(log.first);
        ctrl_block->frame->get_data(log.second, slot.get_offset(), *old_val_size);
        buf_return_ctrl_block(&ctrl_block);

        trx_add_log(table_id, leaf, i, trx_id, log);
        ctrl_block = buf_read_page(table_id, leaf);
    }

    ctrl_block->frame->set_data(value, slot.get_offset(), val_size);

    buf_return_ctrl_block(&ctrl_block, 1);
    return 0;
}

int db_update(int64_t table_id, int64_t key, char* value, uint16_t val_size, uint16_t* old_val_size, int trx_id) {
    control_block_t* header_ctrl_block = buf_read_page(table_id, 0);

    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(header_ctrl_block->frame);
    buf_return_ctrl_block(&header_ctrl_block);
    int err = update(table_id, root_pagenum, key, value, val_size, old_val_size, trx_id);

    if (err < 0) trx_abort(trx_id);
    return err;
}
