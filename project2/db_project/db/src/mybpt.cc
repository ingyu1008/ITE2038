#include "mybpt.h"
#include "file.h"
#include <deque>

// Find Operations

/* Finds the leaf node's pagenum containing given key. */
pagenum_t find_leaf(int64_t table_id, pagenum_t root_pagenum, int64_t key) {
    pagenum_t cur = root_pagenum;

    if (cur == 0) {
        std::cout << "[DEBUG] Tree is Empty" << std::endl;
        return cur;
    }

    page_t page;
    file_read_page(table_id, cur, &page);

    while (PageIO::BPT::get_is_leaf(&page) == 0) // While the page is internal
    {
        int num_keys = PageIO::BPT::get_num_keys(&page);
        int i;
        for (i = 0; i < num_keys; i++) {
            if (key < PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i).get_key()) {
                break;
            }
        }
        if (i == 0) {
            cur = PageIO::BPT::InternalPage::get_leftmost_pagenum(&page);
        } else {
            cur = PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i - 1).get_pagenum();
        }
        file_read_page(table_id, cur, &page);
    }
    return cur;
}

int find(int64_t table_id, pagenum_t root_pagenum, int64_t key, char* ret_val, uint16_t* val_size) {
    // std::cout << "[DEBUG] Finding k"
    pagenum_t leaf = find_leaf(table_id, root_pagenum, key);
    // std::cout << "[DEBUG] leaf page number: " << leaf << std::endl;
    if (leaf == 0) return 1;

    page_t page;
    file_read_page(table_id, leaf, &page);

    int num_keys = PageIO::BPT::get_num_keys(&page);
    int i;
    slot_t slot;
    for (i = 0; i < num_keys; i++) {
        slot = PageIO::BPT::LeafPage::get_nth_slot(&page, i);
        if (slot.get_key() == key) break;
    }
    if (i == num_keys) {
        // std::cout << "[DEBUG] Could not find given key: " << key << std::endl;
        return 1;
    } else {
        *val_size = slot.get_size();
        page.get_data(ret_val, slot.get_offset(), *val_size);
    }
    return 0;
}

// Insertion

/* Allocates a page, which can be adapted
 * to serve as either a leaf or an internal page.
 */
pagenum_t make_node(int64_t table_id) {
    pagenum_t pagenum = file_alloc_page(table_id);
    // std::cout << "[DEBUG] Made node with pagenum " << pagenum << std::endl;
    page_t page;
    file_read_page(table_id, pagenum, &page);
    PageIO::BPT::set_parent_pagenum(&page, 0);
    PageIO::BPT::set_is_leaf(&page, 0);
    PageIO::BPT::set_num_keys(&page, 0);
    file_write_page(table_id, pagenum, &page);
    return pagenum;
}

/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
pagenum_t make_leaf(int64_t table_id) {
    pagenum_t pagenum = make_node(table_id);
    page_t page;
    file_read_page(table_id, pagenum, &page);
    PageIO::BPT::set_is_leaf(&page, 1);
    PageIO::BPT::LeafPage::set_amount_free_space(&page, INITIAL_FREE_SPACE);
    file_write_page(table_id, pagenum, &page);
    return pagenum;
}

/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to
 * the node to the left of the key to be inserted.
 */
int get_left_index(int64_t table_id, pagenum_t parent_pagenum, pagenum_t left_pagenum) {
    page_t page;
    file_read_page(table_id, parent_pagenum, &page);
    if (PageIO::BPT::InternalPage::get_leftmost_pagenum(&page) == left_pagenum) {
        return -1;
    }
    int ret = 0;
    int num_keys = PageIO::BPT::get_num_keys(&page);
    while (ret < num_keys && PageIO::BPT::InternalPage::get_nth_branch_factor(&page, ret).get_pagenum() != left_pagenum) {
        ret++;
    }
    return ret;
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {
    // std::cout << "[DEBUG] insert_into_new_root()" << std::endl;
    pagenum_t root_pagenum = make_node(table_id);
    page_t root;
    file_read_page(table_id, root_pagenum, &root);
    PageIO::BPT::InternalPage::set_leftmost_pagenum(&root, left_pagenum);
    branch_factor_t temp;
    temp.set_key(key);
    temp.set_pagenum(right_pagenum);
    PageIO::BPT::InternalPage::set_nth_branch_factor(&root, 0, temp);
    PageIO::BPT::set_num_keys(&root, 1);
    file_write_page(table_id, root_pagenum, &root);

    page_t page;
    file_read_page(table_id, left_pagenum, &page);
    PageIO::BPT::set_parent_pagenum(&page, root_pagenum);
    file_write_page(table_id, left_pagenum, &page);

    file_read_page(table_id, right_pagenum, &page);
    PageIO::BPT::set_parent_pagenum(&page, root_pagenum);
    file_write_page(table_id, right_pagenum, &page);

    return root_pagenum;
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
pagenum_t insert_into_node(int64_t table_id, pagenum_t root_pagenum, pagenum_t parent_pagenum, int left_index, int64_t key, pagenum_t right_pagenum) {
    // std::cout << "[DEBUG] insert_into_node()" << std::endl;

    page_t page;
    file_read_page(table_id, parent_pagenum, &page);
    int num_keys = PageIO::BPT::get_num_keys(&page);
    PageIO::BPT::set_num_keys(&page, num_keys + 1);


    for (int i = num_keys; i > left_index; i--) {
        // branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i - 1);
        // PageIO::BPT::InternalPage::set_nth_branch_factor(&page, i, branch_factor);
        PageIO::BPT::InternalPage::set_nth_branch_factor(&page, i, PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i - 1));
    }

    branch_factor_t new_branch_factor;
    new_branch_factor.set_key(key);
    new_branch_factor.set_pagenum(right_pagenum);

    PageIO::BPT::InternalPage::set_nth_branch_factor(&page, left_index + 1, new_branch_factor);
    file_write_page(table_id, parent_pagenum, &page);

    return root_pagenum;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root_pagenum, pagenum_t old_node_pagenum, int left_index, int64_t key, pagenum_t right_pagenum) {
    // std::cout << "[DEBUG] insert_into_node_after_splitting()" << std::endl;

    std::vector<std::pair<int64_t, pagenum_t>> branch_factors;

    page_t page;
    file_read_page(table_id, old_node_pagenum, &page);
    int num_keys = PageIO::BPT::get_num_keys(&page);

    int i, j;
    for (i = 0; i < num_keys; i++) {
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i);
        branch_factors.emplace_back(branch_factor.get_key(), branch_factor.get_pagenum());
    }
    branch_factors.emplace_back(key, right_pagenum);
    std::sort(branch_factors.begin(), branch_factors.end());

    if (branch_factors.size() != NODE_MAX_KEYS + 1) {
        // This should never happen
        std::cout << "[FATAL] node split when its not supposed to, the node size equals " << branch_factors.size() << std::endl;
    }

    int split = (num_keys + 1) / 2;

    PageIO::BPT::set_num_keys(&page, split);
    for (i = 0; i < split; i++) {
        branch_factor_t branch_factor;
        branch_factor.set_key(branch_factors[i].first);
        branch_factor.set_pagenum(branch_factors[i].second);
        PageIO::BPT::InternalPage::set_nth_branch_factor(&page, i, branch_factor);
    }
    file_write_page(table_id, old_node_pagenum, &page);
    // {
    //     std::cout << "[DEBUG] Left Node Contains: ";
    //     for (int i = 0; i < split; i++) {
    //         std::cout << PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i).get_key() << " ";
    //     }
    //     std::cout << std::endl;
    //     std::cout << "[DEBUG] Left Node is_leaf: " << PageIO::BPT::get_is_leaf(&page) << std::endl;
    // }
    pagenum_t par_pagenum = PageIO::BPT::get_parent_pagenum(&page);
    int64_t prop_key = branch_factors[split].first;

    pagenum_t new_node_pagenum = make_node(table_id);
    file_read_page(table_id, new_node_pagenum, &page);

    PageIO::BPT::InternalPage::set_leftmost_pagenum(&page, branch_factors[split].second);
    PageIO::BPT::set_parent_pagenum(&page, par_pagenum);
    PageIO::BPT::set_num_keys(&page, num_keys - split);

    page_t child;
    for (++i, j = 0; i < branch_factors.size(); i++, j++) {
        branch_factor_t branch_factor;
        branch_factor.set_key(branch_factors[i].first);
        branch_factor.set_pagenum(branch_factors[i].second);
        PageIO::BPT::InternalPage::set_nth_branch_factor(&page, j, branch_factor);

        file_read_page(table_id, branch_factors[i].second, &child);
        PageIO::BPT::set_parent_pagenum(&child, new_node_pagenum);
        file_write_page(table_id, branch_factors[i].second, &child);
    }
    file_write_page(table_id, new_node_pagenum, &page);
    // {
    //     std::cout << "[DEBUG] Right Node Contains: ";
    //     for (int i = 0; i < num_keys - split; i++) {
    //         std::cout << PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i).get_key() << " ";
    //     }
    //     std::cout << std::endl;
    //     std::cout << "[DEBUG] Right Node is_leaf: " << PageIO::BPT::get_is_leaf(&page) << std::endl;
    // }

    return insert_into_parent(table_id, root_pagenum, old_node_pagenum, prop_key, new_node_pagenum);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(int64_t table_id, pagenum_t root_pagenum, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {
    // std::cout << "[DEBUG] insert_into_parent()" << std::endl;

    page_t left;
    file_read_page(table_id, left_pagenum, &left);
    pagenum_t par_pagenum = PageIO::BPT::get_parent_pagenum(&left);

    if (par_pagenum == 0) {
        return insert_into_new_root(table_id, left_pagenum, key, right_pagenum);
    }

    int left_index = get_left_index(table_id, par_pagenum, left_pagenum);

    page_t page;
    file_read_page(table_id, par_pagenum, &page);
    int num_keys = PageIO::BPT::get_num_keys(&page);

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
    // std::cout << "[DEBUG] insert_into_leaf()" << std::endl;

    page_t leaf;
    file_read_page(table_id, leaf_pagenum, &leaf);
    int num_keys = PageIO::BPT::get_num_keys(&leaf);

    int insertion_point = 0;
    while (insertion_point < num_keys && PageIO::BPT::LeafPage::get_nth_slot(&leaf, insertion_point).get_key() < key) {
        insertion_point++;
    }

    uint64_t amount_free_space = PageIO::BPT::LeafPage::get_amount_free_space(&leaf);
    uint16_t offset = amount_free_space + PH_SIZE + num_keys * SLOT_SIZE - size;

    PageIO::BPT::set_num_keys(&leaf, num_keys + 1);
    for (int i = num_keys; i > insertion_point; i--) {
        // slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&leaf, i-1);
        // PageIO::BPT::LeafPage::set_nth_slot(&leaf, i, slot);
        PageIO::BPT::LeafPage::set_nth_slot(&leaf, i, PageIO::BPT::LeafPage::get_nth_slot(&leaf, i - 1));
    }

    PageIO::BPT::LeafPage::set_amount_free_space(&leaf, amount_free_space - SLOT_SIZE - size);

    slot_t slot;
    slot.set_key(key);
    slot.set_offset(offset);
    slot.set_size(size);
    PageIO::BPT::LeafPage::set_nth_slot(&leaf, insertion_point, slot);

    leaf.set_data(data, offset, size);
    file_write_page(table_id, leaf_pagenum, &leaf);

    return leaf_pagenum;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root_pagenum, pagenum_t leaf_pagenum, int64_t key, const char* data, uint16_t data_size) {
    // std::cout << "[DEBUG] insert_into_leaf_after_splitting()" << std::endl;

    page_t left, leaf;
    file_read_page(table_id, leaf_pagenum, &left);
    file_read_page(table_id, leaf_pagenum, &leaf);

    // std::cout << "[FATAL] Why is this node not leaf???????????????????" << PageIO::BPT::get_is_leaf(&leaf) << std::endl;

    pagenum_t right_pagenum = make_leaf(table_id);
    page_t right;
    file_read_page(table_id, right_pagenum, &right);

    PageIO::BPT::LeafPage::set_right_sibling_pagenum(&right, PageIO::BPT::LeafPage::get_right_sibling_pagenum(&leaf));
    PageIO::BPT::LeafPage::set_right_sibling_pagenum(&left, right_pagenum);
    PageIO::BPT::set_parent_pagenum(&right, PageIO::BPT::get_parent_pagenum(&leaf));

    int num_keys = PageIO::BPT::get_num_keys(&leaf);

    std::vector<std::pair<uint64_t, int>> keys; // key, slot index in original node, negative index for new key

    int i, j;
    for (i = 0; i < num_keys; i++) {
        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&leaf, i);
        keys.emplace_back(slot.get_key(), i);
    }
    keys.emplace_back(key, -1);
    std::sort(keys.begin(), keys.end());

    // std::cout << "[DEBUG] keys in this node(" << leaf_pagenum << "): ";
    // for (int i = 0; i < keys.size(); i++) {
    //     std::cout << keys[i].first << " ";
    // }
    // std::cout << std::endl;

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
            PageIO::BPT::set_num_keys(&left, i);
            PageIO::BPT::LeafPage::set_amount_free_space(&left, free_space);
            file_write_page(table_id, leaf_pagenum, &left);
            break;
        }

        uint16_t offset = free_space + PH_SIZE + i * SLOT_SIZE - size;
        slot_t slot;
        slot.set_key(keys[i].first);
        slot.set_offset(offset);
        slot.set_size(size);
        PageIO::BPT::LeafPage::set_nth_slot(&left, i, slot);
        left.set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }
    // {
    //     std::cout << "*******************************************\n";
    //     std::cout << "[DEBUG] Left Node Contains: ";
    //     int num_keys = PageIO::BPT::get_num_keys(&left);
    //     for (j = 0; j < i; j++) {
    //         std::cout << PageIO::BPT::LeafPage::get_nth_slot(&left, j).get_key() << " ";
    //     }
    //     std::cout << std::endl;
    //     std::cout << "*******************************************\n";
    // }

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
        PageIO::BPT::LeafPage::set_nth_slot(&right, j, slot);
        right.set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }

    PageIO::BPT::set_num_keys(&right, j);
    PageIO::BPT::LeafPage::set_amount_free_space(&right, free_space);
    // {
    //     std::cout << "[DEBUG] Right Node Contains: ";
    //     int num_keys = PageIO::BPT::get_num_keys(&right);
    //     for (i = 0; i < j; i++) {
    //         std::cout << PageIO::BPT::LeafPage::get_nth_slot(&right, i).get_key() << " ";
    //     }
    //     std::cout << std::endl;
    //     std::cout << "*******************************************\n";
    // }

    file_write_page(table_id, right_pagenum, &right);

    key = PageIO::BPT::LeafPage::get_nth_slot(&right, 0).get_key();
    return insert_into_parent(table_id, root_pagenum, leaf_pagenum, key, right_pagenum);
}

/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(int64_t table_id, int64_t key, const char* data, uint16_t size) {
    std::cout << "[DEBUG] start_new_tree()" << std::endl;

    pagenum_t root_pagenum = make_leaf(table_id);
    page_t root;
    file_read_page(table_id, root_pagenum, &root);
    PageIO::BPT::set_num_keys(&root, 1);
    PageIO::BPT::set_parent_pagenum(&root, 0);

    uint16_t offset = INITIAL_FREE_SPACE + PH_SIZE - size;
    slot_t slot;
    slot.set_key(key);
    slot.set_offset(offset);
    slot.set_size(size);
    PageIO::BPT::LeafPage::set_nth_slot(&root, 0, slot);
    root.set_data(data, offset, size);
    PageIO::BPT::LeafPage::set_amount_free_space(&root, INITIAL_FREE_SPACE - size - SLOT_SIZE);
    PageIO::BPT::LeafPage::set_right_sibling_pagenum(&root, 0);
    file_write_page(table_id, root_pagenum, &root);
    return root_pagenum;
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
pagenum_t insert(int64_t table_id, pagenum_t root_pagenum, int64_t key, const char* data, uint16_t sz) {
    char buffer[112];
    uint16_t size;

    /* The current implementation ignores
     * duplicates.
     */
    if (find(table_id, root_pagenum, key, buffer, &size) == 0) {
        // This can be checked beforehand in db_insert().
        return root_pagenum;
    }

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

    page_t leaf;
    file_read_page(table_id, leaf_pagenum, &leaf);

    // std::cout << "[DEBUG] Descibing a Node containing the key(1) " << key << ": ";
    // int num_keys = PageIO::BPT::get_num_keys(&leaf);
    // for (int i = 0; i < num_keys; i++) {
    //     std::cout << PageIO::BPT::LeafPage::get_nth_slot(&leaf, i).get_key() << " ";
    // }
    // std::cout << std::endl;

    if (PageIO::BPT::LeafPage::get_amount_free_space(&leaf) > SLOT_SIZE + sz) {
        leaf_pagenum = insert_into_leaf(table_id, leaf_pagenum, key, data, sz);
        file_read_page(table_id, leaf_pagenum, &leaf);

        // std::cout << "[DEBUG] Descibing a Node containing the key(2) " << key << ": ";
        // int num_keys = PageIO::BPT::get_num_keys(&leaf);
        // for (int i = 0; i < num_keys; i++) {
        //     std::cout << PageIO::BPT::LeafPage::get_nth_slot(&leaf, i).get_key() << " ";
        // }
        // std::cout << std::endl;

        return root_pagenum;
    }

    /* Case:  leaf must be split.
     */
    return insert_into_leaf_after_splitting(table_id, root_pagenum, leaf_pagenum, key, data, sz);
}

// Deletion

std::deque<std::string> dq;

int get_neighbor_index(int64_t table_id, pagenum_t pagenum) {
    dq.push_back(__func__);
    page_t page;
    file_read_page(table_id, pagenum, &page);

    pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(&page);

    // std::cout << parent_pagenum << " " << pagenum << "!!!!" << std::endl;
    page_t parent;
    file_read_page(table_id, parent_pagenum, &parent);
    if (PageIO::BPT::InternalPage::get_leftmost_pagenum(&parent) == pagenum) {
        int num_keys = PageIO::BPT::get_num_keys(&parent);
#ifdef DEBUG
        std::cout << "[DEBUG] " << pagenum << ": " << PageIO::BPT::InternalPage::get_leftmost_pagenum(&parent) << " ";
        for (int i = 0; i < num_keys; i++) {
            std::cout << PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, i).get_pagenum() << " ";
        }
        std::cout << std::endl;
#endif
        dq.pop_back();
        return -1; // Special Case: Leftmost page
    } else {
        int num_keys = PageIO::BPT::get_num_keys(&parent);
#ifdef DEBUG
        std::cout << "[DEBUG] " << pagenum << ": " << PageIO::BPT::InternalPage::get_leftmost_pagenum(&parent) << " ";
#endif
        for (int i = 0; i < num_keys; i++) {
#ifdef DEBUG
            std::cout << PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, i).get_pagenum() << " ";
#endif
            if (PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, i).get_pagenum() == pagenum) {
                dq.pop_back();
                return i;
            }
        }

#ifdef DEBUG
        std::cout << std::endl;
#endif
    }
    throw std::invalid_argument("Invalid Argument at get_neighbor_index()");
}

pagenum_t remove_entry_from_internal(int64_t table_id, pagenum_t internal_pagenum, int64_t key) {
    dq.push_back(__func__);
    remove_entry_internal++;
    page_t page;
    file_read_page(table_id, internal_pagenum, &page);

    int num_keys = PageIO::BPT::get_num_keys(&page);

    for (int i = 0; i < num_keys; i++) {
        branch_factor_t ith = PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i);
        if (ith.get_key() <= key) {
            continue;
        }

        PageIO::BPT::InternalPage::set_nth_branch_factor(&page, i - 1, ith);
    }
    PageIO::BPT::set_num_keys(&page, num_keys - 1);
    file_write_page(table_id, internal_pagenum, &page);
    dq.pop_back();
    return internal_pagenum;
}

pagenum_t remove_entry_from_leaf(int64_t table_id, pagenum_t leaf_pagenum, int64_t key) {
    dq.push_back(__func__);
    remove_entry_leaf++;
    page_t page, copy;
    file_read_page(table_id, leaf_pagenum, &page);
    file_read_page(table_id, leaf_pagenum, &copy);

    // Almost copy and paste part from insert_into_leaf_after_splitting()
    int num_keys = PageIO::BPT::get_num_keys(&page);

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
        PageIO::BPT::LeafPage::set_nth_slot(&page, i, slot);
        page.set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }

    PageIO::BPT::LeafPage::set_amount_free_space(&page, free_space);
    PageIO::BPT::set_num_keys(&page, num_keys - 1);
    file_write_page(table_id, leaf_pagenum, &page);
    dq.pop_back();
    return  leaf_pagenum;
}

pagenum_t adjust_root(int64_t table_id, pagenum_t root_pagenum) {
    dq.push_back(__func__);
    page_t root;
    file_read_page(table_id, root_pagenum, &root);

    // Nothing to do
    if (PageIO::BPT::get_num_keys(&root) > 0) {
        dq.pop_back();
        return root_pagenum;
    }

    pagenum_t new_root_pagenum = 0;

    if (PageIO::BPT::get_is_leaf(&root) == 0) {
        new_root_pagenum = PageIO::BPT::InternalPage::get_leftmost_pagenum(&root);
        file_read_page(table_id, new_root_pagenum, &root);
        PageIO::BPT::set_parent_pagenum(&root, 0);
        file_write_page(table_id, new_root_pagenum, &root);
    }

    file_free_page(table_id, root_pagenum);

    dq.pop_back();
    return new_root_pagenum;
}

pagenum_t merge_internal(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int64_t key) {
    dq.push_back(__func__);
    merge_int++;
    if (neighbor_index == -1) {
        std::swap(pagenum, neighbor_pagenum);
    }

    page_t page, neighbor;
    file_read_page(table_id, pagenum, &page);
    file_read_page(table_id, neighbor_pagenum, &neighbor);

    int neighbor_insertion_index = PageIO::BPT::get_num_keys(&neighbor);

    branch_factor_t branch_factor;
    branch_factor.set_key(key);
    branch_factor.set_pagenum(PageIO::BPT::InternalPage::get_leftmost_pagenum(&page));

    PageIO::BPT::InternalPage::set_nth_branch_factor(&neighbor, neighbor_insertion_index, branch_factor);

    int i, j, page_num_keys = PageIO::BPT::get_num_keys(&page);
    for (i = neighbor_insertion_index + 1, j = 0; j < page_num_keys; i++, j++) {
        PageIO::BPT::InternalPage::set_nth_branch_factor(&neighbor, i, PageIO::BPT::InternalPage::get_nth_branch_factor(&page, j));
    }

    PageIO::BPT::set_num_keys(&neighbor, i);

    file_write_page(table_id, neighbor_pagenum, &neighbor);

    page_t child;
    pagenum_t child_pagenum;
    for (j = 0; j < i; j++) {
        child_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(&neighbor, j).get_pagenum();
        file_read_page(table_id, child_pagenum, &child);
        PageIO::BPT::set_parent_pagenum(&child, neighbor_pagenum);
        file_write_page(table_id, child_pagenum, &child);
    }

    root_pagenum = delete_entry(table_id, root_pagenum, PageIO::BPT::get_parent_pagenum(&page), key);
    file_free_page(table_id, pagenum);
    dq.pop_back();
    return root_pagenum;
}

pagenum_t merge_leaf(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int64_t key) {
    dq.push_back(__func__);
    merge_lf++;
    // Almost copy and paste from merge_internal()

    if (neighbor_index == -1) {
        std::swap(pagenum, neighbor_pagenum);
    }

    page_t page, neighbor;
    file_read_page(table_id, pagenum, &page);
    file_read_page(table_id, neighbor_pagenum, &neighbor);

    int neighbor_insertion_index = PageIO::BPT::get_num_keys(&neighbor);

    int i, j, page_num_keys = PageIO::BPT::get_num_keys(&page);

    uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(&neighbor);

    for (i = neighbor_insertion_index, j = 0; j < page_num_keys; i++, j++) {
        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&page, j);
        char buffer[MAX_VAL_SIZE];
        uint16_t size;

        size = slot.get_size();
        page.get_data(buffer, slot.get_offset(), size);

        uint16_t offset = free_space + PH_SIZE + i * SLOT_SIZE - size;
        slot.set_offset(offset);
        PageIO::BPT::LeafPage::set_nth_slot(&neighbor, i, slot);
        neighbor.set_data(buffer, offset, size);
        free_space -= SLOT_SIZE + size;
    }
    PageIO::BPT::set_num_keys(&neighbor, i);
    PageIO::BPT::LeafPage::set_amount_free_space(&neighbor, free_space);
    file_write_page(table_id, neighbor_pagenum, &neighbor);

    root_pagenum = delete_entry(table_id, root_pagenum, PageIO::BPT::get_parent_pagenum(&page), key);
    file_free_page(table_id, pagenum);
    dq.pop_back();
    return root_pagenum;
}

pagenum_t redistribute_internal(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int key_index, int64_t key) {
    dq.push_back(__func__);
    redist_internal++;
    // Note that there is no change in the tree structure

    // std::cout << "[DEEEBUG] " << pagenum << " " << neighbor_pagenum << " " << neighbor_index << " " << key_index << " " << key << std::endl;

    page_t page, neighbor;
    file_read_page(table_id, pagenum, &page);
    file_read_page(table_id, neighbor_pagenum, &neighbor);

    int num_keys = PageIO::BPT::get_num_keys(&page);
    int neighbor_num_keys = PageIO::BPT::get_num_keys(&neighbor);

    if (neighbor_index >= 0) {
        // Case where neighbor is on the node's left
        // Pull the last one from the left
        for (int i = num_keys; i > 0; i--) {
            PageIO::BPT::InternalPage::set_nth_branch_factor(&page, i, PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i - 1));
        }
        branch_factor_t branch_factor;
        branch_factor.set_key(key);
        branch_factor.set_pagenum(PageIO::BPT::InternalPage::get_leftmost_pagenum(&page));
        PageIO::BPT::InternalPage::set_nth_branch_factor(&page, 0, branch_factor);

        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&neighbor, neighbor_num_keys - 1);

        page_t child;
        file_read_page(table_id, branch_factor.get_pagenum(), &child);
        PageIO::BPT::set_parent_pagenum(&child, pagenum);
        file_write_page(table_id, branch_factor.get_pagenum(), &child);

        PageIO::BPT::InternalPage::set_leftmost_pagenum(&page, branch_factor.get_pagenum());

        int64_t key_prime = branch_factor.get_key();

        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(&page);
        page_t parent;
        file_read_page(table_id, parent_pagenum, &parent);
        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, key_index);
        // std::cout << branch_factor.get_key() << "\t" << key << "\n";
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(&parent, key_index, branch_factor);
        file_write_page(table_id, parent_pagenum, &page);
    } else {
        // Case where neighbor is on the node's right
        // Pull the first one from the right 

        branch_factor_t branch_factor;
        branch_factor.set_key(key);
        branch_factor.set_pagenum(PageIO::BPT::InternalPage::get_leftmost_pagenum(&neighbor));
        PageIO::BPT::InternalPage::set_nth_branch_factor(&page, num_keys, branch_factor);

        page_t child;
        file_read_page(table_id, branch_factor.get_pagenum(), &child);
        PageIO::BPT::set_parent_pagenum(&child, pagenum);
        file_write_page(table_id, branch_factor.get_pagenum(), &child);

        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&neighbor, 0);

        int64_t key_prime = branch_factor.get_key();
        PageIO::BPT::InternalPage::set_leftmost_pagenum(&neighbor, branch_factor.get_pagenum());


        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(&page);
        page_t parent;
        file_read_page(table_id, parent_pagenum, &parent);
        branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, key_index);
        // std::cout << "## " << branch_factor.get_key() << "\t" << key << "\n";
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(&parent, key_index, branch_factor);
        file_write_page(table_id, parent_pagenum, &parent);

        for (int i = 1; i < neighbor_num_keys; i++) {
            PageIO::BPT::InternalPage::set_nth_branch_factor(&neighbor, i - 1, PageIO::BPT::InternalPage::get_nth_branch_factor(&neighbor, i));
        }
    }

    // TODO: Adjust numkeys
    PageIO::BPT::set_num_keys(&page, num_keys + 1);
    PageIO::BPT::set_num_keys(&neighbor, neighbor_num_keys - 1);

    file_write_page(table_id, pagenum, &page);
    file_write_page(table_id, neighbor_pagenum, &neighbor);

    dq.pop_back();
    return root_pagenum;
}

pagenum_t redistribute_leaf(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, pagenum_t neighbor_pagenum, int neighbor_index, int key_index, int64_t key) {
    dq.push_back(__func__);
    redist_leaf++;
    // Almost copy and paste from redistribute_internal()
    page_t page, neighbor, neighbor_copy;
    file_read_page(table_id, pagenum, &page);
    file_read_page(table_id, neighbor_pagenum, &neighbor);
    file_read_page(table_id, neighbor_pagenum, &neighbor_copy);

    int num_keys = PageIO::BPT::get_num_keys(&page);
    int neighbor_num_keys = PageIO::BPT::get_num_keys(&neighbor);

    if (neighbor_index >= 0) {
        // Case where neighbor is on the node's left
        // Pull the last one from the left
        for (int i = num_keys; i > 0; i--) {
            PageIO::BPT::LeafPage::set_nth_slot(&page, i, PageIO::BPT::LeafPage::get_nth_slot(&page, i - 1));
        }

        {
            // Scoping this part to avoid variable name collision

            uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(&page);

            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&neighbor, neighbor_num_keys - 1);
            char buffer[MAX_VAL_SIZE];
            uint16_t size;

            size = slot.get_size();
            neighbor.get_data(buffer, slot.get_offset(), size);

            uint16_t offset = free_space + PH_SIZE + num_keys * SLOT_SIZE - size;
            slot.set_offset(offset);
            PageIO::BPT::LeafPage::set_nth_slot(&page, 0, slot);
            page.set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;

            PageIO::BPT::LeafPage::set_amount_free_space(&page, free_space);
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
            PageIO::BPT::LeafPage::set_nth_slot(&neighbor, i, slot);
            neighbor.set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;
        }

        PageIO::BPT::LeafPage::set_amount_free_space(&neighbor, free_space);

        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&page, 0);

        int64_t key_prime = slot.get_key();

        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(&page);
        page_t parent;
        file_read_page(table_id, parent_pagenum, &parent);
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, key_index);
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(&parent, key_index, branch_factor);
        file_write_page(table_id, parent_pagenum, &parent);
    } else {
        // Case where neighbor is on the node's right
        // Pull the first one from the right 

        {
            // Scoping this part to avoid variable name collision

            uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(&page);

            slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&neighbor, 0);
            char buffer[MAX_VAL_SIZE];
            uint16_t size;

            size = slot.get_size();
            neighbor.get_data(buffer, slot.get_offset(), size);

            uint16_t offset = free_space + PH_SIZE + num_keys * SLOT_SIZE - size;
            slot.set_offset(offset);
            PageIO::BPT::LeafPage::set_nth_slot(&page, num_keys, slot);
            page.set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;

            PageIO::BPT::LeafPage::set_amount_free_space(&page, free_space);
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
            PageIO::BPT::LeafPage::set_nth_slot(&neighbor, i - 1, slot);
            neighbor.set_data(buffer, offset, size);
            free_space -= SLOT_SIZE + size;
        }

        PageIO::BPT::LeafPage::set_amount_free_space(&neighbor, free_space);

        slot_t slot = PageIO::BPT::LeafPage::get_nth_slot(&neighbor, 0);

        int64_t key_prime = slot.get_key();

        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(&page);
        page_t parent;
        file_read_page(table_id, parent_pagenum, &parent);
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, key_index);
        branch_factor.set_key(key_prime);
        PageIO::BPT::InternalPage::set_nth_branch_factor(&parent, key_index, branch_factor);
        file_write_page(table_id, parent_pagenum, &parent);
    }

    // TODO: Adjust numkeys
    PageIO::BPT::set_num_keys(&page, num_keys + 1);
    PageIO::BPT::set_num_keys(&neighbor, neighbor_num_keys - 1);

    file_write_page(table_id, pagenum, &page);
    file_write_page(table_id, neighbor_pagenum, &neighbor);

    dq.pop_back();
    return root_pagenum;
}

pagenum_t delete_entry(int64_t table_id, pagenum_t root_pagenum, pagenum_t pagenum, int64_t key) {
    dq.push_back(__func__);
    // std::cout << "@@@" << "delete entry on pagenum = " << pagenum << " / key = " << key << std::endl;
    page_t page;
    file_read_page(table_id, pagenum, &page);

    int is_leaf = PageIO::BPT::get_is_leaf(&page);

    // std::cout << "@#@#" << pagenum << ": is_leaf =" << is_leaf << std::endl;

    if (is_leaf) {
        pagenum = remove_entry_from_leaf(table_id, pagenum, key);
    } else {
        pagenum = remove_entry_from_internal(table_id, pagenum, key);
    }

    if (pagenum == root_pagenum) {
        dq.pop_back();
        return adjust_root(table_id, root_pagenum);
    }

    // std::cout << "#@@@" << pagenum << std::endl;
    file_read_page(table_id, pagenum, &page);
    is_leaf = PageIO::BPT::get_is_leaf(&page);

    if (is_leaf) {
        uint64_t free_space = PageIO::BPT::LeafPage::get_amount_free_space(&page);

        if (free_space < THRESHHOLD) {
            dq.pop_back();
            return root_pagenum;
        }

        int neighbor_index = get_neighbor_index(table_id, pagenum);
        int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;

        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(&page);
        page_t parent;
        file_read_page(table_id, parent_pagenum, &parent);
        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, k_prime_index);
        int64_t k_prime = branch_factor.get_key();
        pagenum_t neighbor_pagenum;
        if (neighbor_index == -1) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, 0).get_pagenum();
        } else if (neighbor_index == 0) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_leftmost_pagenum(&parent);
        } else {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, neighbor_index - 1).get_pagenum();
        }

        page_t neighbor;
        file_read_page(table_id, neighbor_pagenum, &neighbor);

        if (PageIO::BPT::LeafPage::get_amount_free_space(&neighbor) + free_space >= INITIAL_FREE_SPACE) {
            dq.pop_back();
            return merge_leaf(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime);
        } else {
            dq.pop_back();
            return redistribute_leaf(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime_index, k_prime);
        }
    } else {
        int min_keys = NODE_MAX_KEYS / 2;
        int num_keys = PageIO::BPT::get_num_keys(&page);

        if (num_keys >= min_keys) {
            return root_pagenum;
        }
        pagenum_t parent_pagenum = PageIO::BPT::get_parent_pagenum(&page);
        page_t parent;
        file_read_page(table_id, parent_pagenum, &parent);

        int neighbor_index = get_neighbor_index(table_id, pagenum);
        int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;



        branch_factor_t branch_factor = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, k_prime_index);
        int64_t k_prime = branch_factor.get_key();
        pagenum_t neighbor_pagenum;
        if (neighbor_index == -1) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, 0).get_pagenum();
        } else if (neighbor_index == 0) {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_leftmost_pagenum(&parent);
        } else {
            neighbor_pagenum = PageIO::BPT::InternalPage::get_nth_branch_factor(&parent, neighbor_index - 1).get_pagenum();
        }

        page_t neighbor;
        file_read_page(table_id, neighbor_pagenum, &neighbor);

        if (PageIO::BPT::get_num_keys(&neighbor) + num_keys < NODE_MAX_KEYS) {
            dq.pop_back();
            return merge_internal(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime);
        } else {
            dq.pop_back();
            return redistribute_internal(table_id, root_pagenum, pagenum, neighbor_pagenum, neighbor_index, k_prime_index, k_prime);
        }
    }
}

pagenum_t _delete(int64_t table_id, pagenum_t root_pagenum, int64_t key) {
    dq.push_back(__func__);
    char buffer[MAX_VAL_SIZE];
    uint16_t size;
    int err = find(table_id, root_pagenum, key, buffer, &size);
    pagenum_t leaf_pagenum = find_leaf(table_id, root_pagenum, key);

    if (err == 0 && leaf_pagenum != 0) {
        root_pagenum = delete_entry(table_id, root_pagenum, leaf_pagenum, key);
    }

    dq.pop_back();
    return root_pagenum;
}

int remove_entry_internal = 0;
int remove_entry_leaf = 0;
int merge_int = 0;
int merge_lf = 0;
int redist_internal = 0;
int redist_leaf = 0;

void printTotalCalls() {
    std::cout << "[DEBUG] remove_entry_internal = " << remove_entry_internal << std::endl;
    std::cout << "[DEBUG] remove_entry_leaf = " << remove_entry_leaf << std::endl;
    std::cout << "[DEBUG] merge_int = " << merge_int << std::endl;
    std::cout << "[DEBUG] merge_lf = " << merge_lf << std::endl;
    std::cout << "[DEBUG] redist_internal = " << redist_internal << std::endl;
    std::cout << "[DEBUG] redist_leaf = " << redist_leaf << std::endl;
}

// =================================================================================================
// API

int64_t open_table(char* pathname) {
    int fd = file_open_database_file(pathname);
    return fd;
}

int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size) {
    page_t header;
    file_read_page(table_id, 0, &header);
    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(&header);

    char buffer[MAX_VAL_SIZE];
    uint16_t size;
    if (find(table_id, root_pagenum, key, buffer, &size) == 0) {
        return -1;
    }
    root_pagenum = insert(table_id, root_pagenum, key, value, val_size);

    // std::cout << "[DEBUG] root_pagenum = " << root_pagenum << " after insert." << std::endl;
    // page_t root;
    // file_read_page(table_id, root_pagenum, &root);
    // int num_keys = PageIO::BPT::get_num_keys(&root);
    // std::cout << "[DEBUG] keys are: ";
    // for (int i = 0; i < num_keys; i++) {
    //     std::cout << PageIO::BPT::InternalPage::get_nth_branch_factor(&root, i).get_key() << "(" << PageIO::BPT::InternalPage::get_nth_branch_factor(&root, i).get_pagenum() << ") ";
    // }
    // std::cout << std::endl;

    file_read_page(table_id, 0, &header);
    PageIO::HeaderPage::set_root_pagenum(&header, root_pagenum);
    file_write_page(table_id, 0, &header);

    return 0;
}

int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size) {
    page_t header;
    file_read_page(table_id, 0, &header);
    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(&header);
    return find(table_id, root_pagenum, key, ret_val, val_size);
}

int db_delete(int64_t table_id, int64_t key) {
    dq.push_back(__func__);
    page_t header;
    file_read_page(table_id, 0, &header);
    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(&header);
    root_pagenum = _delete(table_id, root_pagenum, key);

    file_read_page(table_id, 0, &header);
    PageIO::HeaderPage::set_root_pagenum(&header, root_pagenum);
    file_write_page(table_id, 0, &header);

    dq.pop_back();
    return 0;
}

int init_db() {
    return 0;
}

int shutdown_db() {
#ifdef DEBUG
    printTotalCalls();
#endif
    file_close_database_file();
    return 0;
}

void print_tree(int64_t table_id, pagenum_t pagenum) {
    std::cout << std::endl;
    static int tabs = 0;
    for (int i = 0; i < tabs; i++) {
        std::cout << "\t";
    }
    std::cout << "[Current=" << pagenum << "]" << " ";
    page_t page;
    file_read_page(table_id, pagenum, &page);

    int num_keys = PageIO::BPT::get_num_keys(&page);
    int is_leaf = PageIO::BPT::get_is_leaf(&page);

    tabs++;

    if (is_leaf) {
        for (int i = 0; i < num_keys; i++) {
            std::cout << PageIO::BPT::LeafPage::get_nth_slot(&page, i).get_key() << " ";
        }
    } else {
        print_tree(table_id, PageIO::BPT::InternalPage::get_leftmost_pagenum(&page));
        for (int i = 0; i < num_keys; i++) {
            print_tree(table_id, PageIO::BPT::InternalPage::get_nth_branch_factor(&page, i).get_pagenum());
        }
    }
    tabs--;
    std::cout << std::endl;
}

void db_print_tree(int64_t table_id) {
    page_t header, root;
    file_read_page(table_id, 0, &header);
    pagenum_t root_pagenum = PageIO::HeaderPage::get_root_pagenum(&header);

    print_tree(table_id, root_pagenum);
}

void printCallStack() {
    int tabs = 0;
    while (!dq.empty()) {
        for (int i = 0; i < tabs; i++) {
            std::cout << "    ";
        }
        std::cout << "At " << dq.front() << "()" << std::endl;
        dq.pop_front();
        tabs++;
    }
}