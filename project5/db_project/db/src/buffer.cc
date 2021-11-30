#include "buffer.h"
#define DEBUG_MODE 0

int buf_size;
std::vector<control_block_t*> buffer_ctrl_blocks;
std::vector<page_t*> buffer;

control_block_t* victim = nullptr; // tail of the linked list, first one on the list is the most recent;
std::map<std::pair<int64_t, pagenum_t>, control_block_t*> pagemap;

//pthread_mutex_t buffer_manager_latch;

int cache_hit = 0;
int tot_read = 0;

void print_buffer_info() {
    #if DEBUG_MODE
    for (auto p : pagemap) {
        if(p.second->is_pinned && p.second->pagenum%100 == 0)
        std::cout << "pagenum = " << p.second->pagenum << ", pin_count = " << p.second->is_pinned << std::endl;
    }
    #endif
}

void double_buffer(int num_buf) {
    std::cout << "[DEBUG] Double Buffer Deprecated" << std::endl;
    // buffer.resize(num_buf * 2);
    // buffer_ctrl_blocks.resize(num_buf * 2);
    // for (int i = num_buf; i < 2 * num_buf; i++) {
    //     buffer[i] = (page_t*)malloc(sizeof(page_t));
    //     buffer_ctrl_blocks[i] = (control_block_t*)malloc(sizeof(control_block_t));

    //     if (buffer[i] == nullptr || buffer_ctrl_blocks[i] == nullptr) {
    //         std::cout << "[FATAL] Memory Allocation Failed at " << __func__ << std::endl;
    //         exit(EXIT_FAILURE);
    //     }
    // }
    // for (int i = num_buf; i < 2 * num_buf; i++) {
    //     buffer_ctrl_blocks[i]->frame = buffer[i];
    //     buffer_ctrl_blocks[i]->table_id = -1;
    //     buffer_ctrl_blocks[i]->pagenum = 0;
    //     buffer_ctrl_blocks[i]->is_dirty = 0;
    //     buffer_ctrl_blocks[i]->is_pinned = 0;
    //     buffer_ctrl_blocks[i]->page_latch = PTHREAD_MUTEX_INITIALIZER;
    //     buffer_ctrl_blocks[i]->next = buffer_ctrl_blocks[(i + 2 * num_buf - 1) % (2 * num_buf)];
    //     buffer_ctrl_blocks[i]->prev = buffer_ctrl_blocks[(i + 1) % (2 * num_buf)];
    // }

    // #if DEBUG_MODE
    // std::cout << "Doubled!" << std::endl;
    // #endif
    // buffer_ctrl_blocks[num_buf]->next = victim->next;
    // victim->next->prev = buffer_ctrl_blocks[num_buf];

    // buffer_ctrl_blocks[num_buf * 2 - 1]->prev = victim;
    // victim->next = buffer_ctrl_blocks[num_buf * 2 - 1];


    // victim = buffer_ctrl_blocks[num_buf];
    // num_buf *= 2;
}

void move_to_beg_of_list(control_block_t* cur) {
    if (cur == victim) {
        victim = victim->prev;
    } else {
        // deletion of node from linked list
        cur->next->prev = cur->prev;
        cur->prev->next = cur->next;

        //insertion in the beginning
        cur->next = victim->next;
        cur->prev = victim;

        cur->next->prev = cur;
        cur->prev->next = cur;
    }
}

control_block_t* find_buffer(int64_t table_id, pagenum_t page_number) {
    auto it = pagemap.find(std::make_pair(table_id, page_number));
    if (it == pagemap.end()) {
        return nullptr;
    }
    return it->second;
}

// Search for eviction victim.
// Skip the ones that are pinned.
control_block_t* find_victim() {
    control_block_t* cur = victim;
    //pthread_mutex_lock(&cur->page_latch);
    // while (cur->is_pinned > 0) {
    //     cur = cur->prev;
    //     if (cur == victim) {

    //         print_buffer_info();
            
    //         #if DEBUG_MODE
    //         std::cout << "[FATAL] Buffer Full" << std::endl;
    //         #endif
    //         // exit(EXIT_FAILURE);
    //         double_buffer(buffer.size());
    //         return victim;
    //     }
    // }
    return cur;
}

// Add a new page to the buffer
// If the buffer is full, replace the least recently used page
control_block_t* add_new_page(int64_t table_id, pagenum_t page_number) {
    control_block_t* cur = find_victim();

    if (cur->is_dirty) {
        file_write_page(cur->table_id, cur->pagenum, cur->frame);
    }
    if (cur->table_id >= 0)
        pagemap.erase(std::make_pair(cur->table_id, cur->pagenum));
    move_to_beg_of_list(cur);
    //pthread_mutex_unlock(&buffer_manager_latch);

    file_read_page(table_id, page_number, cur->frame);
    pagemap.emplace(std::make_pair(table_id, page_number), cur);
    cur->table_id = table_id;
    cur->pagenum = page_number;
    cur->is_pinned++;
    cur->is_dirty = 0;
    return cur;
}

// Frees page from buffered header
// Call file_free_page if the header is not present in the buffer
void free_page(int64_t table_id, pagenum_t page_number) {
    control_block_t* header_ctrl_block = find_buffer(table_id, 0);
    if (header_ctrl_block == nullptr) {
        file_free_page(table_id, page_number);
        return;
    }
    
    //pthread_mutex_lock(&header_ctrl_block->page_latch);
    page_t free_page;
    PageIO::FreePage::set_next_free_pagenum(&free_page, PageIO::HeaderPage::get_free_pagenum(header_ctrl_block->frame));
    file_write_page(table_id, page_number, &free_page);
    PageIO::HeaderPage::set_free_pagenum(header_ctrl_block->frame, page_number);
    header_ctrl_block->is_dirty |= 1;
    //pthread_mutex_unlock(&header_ctrl_block->page_latch);
}

void return_ctrl_block(control_block_t** ctrl_block, int is_dirty) {
    if (ctrl_block == nullptr || (*ctrl_block) == nullptr) return;
    (*ctrl_block)->is_pinned--;
    (*ctrl_block)->is_dirty |= is_dirty;
    control_block_t* tmp = *ctrl_block;
    (*ctrl_block) = nullptr;
    //pthread_mutex_unlock(&(tmp->page_latch));
}

/* Calls file_open_table_file and maps table_id with table index.
 */
int64_t buf_open_table_file(const char* pathname) {
    int64_t table_id = file_open_table_file(pathname);
    if (table_id < 0) return -1;
    return table_id;
}


/* Returns the pointer to the control block with given table_id and page_number.
 * Eviction of victim page can occur if page required is not on the buffer.
 */
control_block_t* buf_read_page(int64_t table_id, pagenum_t page_number) {
    //pthread_mutex_lock(&buffer_manager_latch);
    #if DEBUG_MODE
    // std::cout << "Read Page: " << table_id << " " << page_number << std::endl;
    print_buffer_info();
    #endif
    print_buffer_info();
    tot_read++;

    control_block_t* cur = find_buffer(table_id, page_number);//pagemap[std::make_pair(table_id, page_number)];

    if (cur == nullptr) {
        return add_new_page(table_id, page_number);
    }


    cache_hit++;
    move_to_beg_of_list(cur);

    //pthread_mutex_lock(&cur->page_latch);
    cur->is_pinned++;
    //pthread_mutex_unlock(&buffer_manager_latch);
    return cur;
}


pagenum_t buf_alloc_page(int64_t table_id) {
    //pthread_mutex_lock(&buffer_manager_latch);
    control_block_t* header_ctrl_block = find_buffer(table_id, 0);

    if (header_ctrl_block == nullptr) {
        pagenum_t pagenum = file_alloc_page(table_id);
        //pthread_mutex_unlock(&buffer_manager_latch);
        return pagenum;
    }

    pagenum_t pagenum = PageIO::HeaderPage::get_free_pagenum(header_ctrl_block->frame);
    if (pagenum == 0) {
        pagenum_t next_num = PageIO::HeaderPage::get_num_pages(header_ctrl_block->frame);
        pagenum_t next_size = next_num * 2;
        while (next_num < next_size)
        {
            page_t free_page;
            PageIO::FreePage::set_next_free_pagenum(&free_page, pagenum);
            pagenum = next_num;
            file_write_page(table_id, next_num, &free_page);;
            next_num++;
        }
        PageIO::HeaderPage::set_free_pagenum(header_ctrl_block->frame, pagenum);
        PageIO::HeaderPage::set_num_pages(header_ctrl_block->frame, next_size);
    }

    page_t page;
    file_read_page(table_id, pagenum, &page);

    PageIO::HeaderPage::set_free_pagenum(header_ctrl_block->frame, PageIO::FreePage::get_next_free_pagenum(&page));

    header_ctrl_block->is_dirty |= 1;
    //pthread_mutex_unlock(&buffer_manager_latch);
    return pagenum;
}

void buf_free_page(int64_t table_id, pagenum_t page_number)
{
    //pthread_mutex_lock(&buffer_manager_latch);
    control_block_t* cur = find_buffer(table_id, page_number); //pagemap[std::make_pair(table_id, page_number)];
    if (cur == nullptr) {
        // Simple case: just free it
        free_page(table_id, page_number);
        //pthread_mutex_unlock(&buffer_manager_latch);
        return;
    }

    // page already on the buffer
    if (cur->is_pinned) {
        std::cout << "[FATAL] Attempt to free page in use. The pin count = " << cur->is_pinned << std::endl;
        exit(EXIT_FAILURE);
    }

    pagemap.erase(std::make_pair(table_id, page_number));

    move_to_beg_of_list(cur);
    victim = cur; // Empty the buffer

    std::memset(cur->frame, 0, PAGE_SIZE);
    cur->table_id = -1;
    cur->pagenum = 0;
    cur->is_dirty = 0;
    cur->is_pinned = 0;

    free_page(table_id, page_number);
    //pthread_mutex_unlock(&buffer_manager_latch);
}

/* Initialzer for buffer and buffer control blocks.
 */
int buf_init_db(int num_buf) {
    buf_size = num_buf;
    buffer.clear();
    buffer_ctrl_blocks.clear();
    pagemap.clear();
    buffer.resize(num_buf);
    buffer_ctrl_blocks.resize(num_buf);

    for (int i = 0; i < num_buf; i++) {
        buffer[i] = (page_t*)malloc(sizeof(page_t));
        buffer_ctrl_blocks[i] = (control_block_t*)malloc(sizeof(control_block_t));

        if (buffer[i] == nullptr || buffer_ctrl_blocks[i] == nullptr) {
            std::cout << "[FATAL] Memory Allocation Failed at " << __func__ << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    for (int i = 0; i < num_buf; i++) {
        buffer_ctrl_blocks[i]->frame = buffer[i];
        buffer_ctrl_blocks[i]->table_id = -1;
        buffer_ctrl_blocks[i]->pagenum = 0;
        buffer_ctrl_blocks[i]->is_dirty = 0;
        buffer_ctrl_blocks[i]->is_pinned = 0;
        // buffer_ctrl_blocks[i]->page_latch = PTHREAD_MUTEX_INITIALIZER;
        buffer_ctrl_blocks[i]->next = buffer_ctrl_blocks[(i + num_buf - 1) % num_buf];
        buffer_ctrl_blocks[i]->prev = buffer_ctrl_blocks[(i + 1) % num_buf];
    }

    victim = buffer_ctrl_blocks[0];
    // buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;
    return 0;
}


int buf_shutdown_db() {
    int total = 0;
    int final_buffer_size = 0;

    for (int i = 0; i < buf_size; i++) {
        control_block_t* cur = buffer_ctrl_blocks[i];
        total += cur->is_pinned;
        if (cur->is_dirty > 0) {
            file_write_page(cur->table_id, cur->pagenum, cur->frame);
        }
        free(cur->frame);
        free(cur);
    }
    file_close_database_file();

    #if DEBUG_MODE
    std::cout << "[DEBUG] Total pin count on shutdown_db() = " << total << std::endl;
    std::cout << "[DEBUG] " << cache_hit << " hits out of " << tot_read << " read operations." << std::endl;
    #endif
    return total == 0;
}

