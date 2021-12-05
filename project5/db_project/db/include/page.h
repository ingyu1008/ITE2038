#ifndef __PAGE_H__
#define __PAGE_H__

#include <cstdint>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <vector>

typedef uint64_t pagenum_t;

constexpr uint64_t INITIAL_SIZE = 10485760; // 10 MiB
constexpr uint64_t PAGE_SIZE = 4096;
constexpr uint64_t SLOT_SIZE = 16;
constexpr uint64_t BRANCH_FACTOR_SIZE = 16;
constexpr pagenum_t INITIAL_FREE_PAGES = (INITIAL_SIZE / PAGE_SIZE) - 1;
constexpr uint64_t INITIAL_FREE_SPACE = PAGE_SIZE - 128;

constexpr uint64_t HEADER_FREE_OFFSET = 0;
constexpr uint64_t HEADER_NUMPAGE_OFFSET = 8;
constexpr uint64_t HEADER_ROOT_PAGENUM_OFFSET = 16;
constexpr uint64_t FREE_FREE_OFFSET = 0;
constexpr uint64_t LEAF_AMOUNT_FREE_SPACE_OFFSET = 112;
constexpr uint64_t LEAF_RIGHT_SIB_PNUM_OFFSET = 120;
constexpr uint64_t LEAF_SLOT_OFFSET = 128;

constexpr uint64_t SLOT_KEY_OFFSET = 0;
constexpr uint64_t SLOT_SIZE_OFFSET = 8;
constexpr uint64_t SLOT_OFFSET_OFFSET = 10;
constexpr uint64_t SLOT_TRX_ID_OFFSET = 12;

constexpr uint64_t BF_KEY_OFFSET = 0;
constexpr uint64_t BF_PAGENUM_OFFSET = 8;

constexpr uint64_t PH_PARENT_PAGENUM_OFFSET = 0;
constexpr uint64_t PH_IS_LEAF_OFFSET = 8;
constexpr uint64_t PH_NUM_KEYS_OFFSET = 12;
constexpr uint64_t PH_SIZE = 128;

constexpr uint64_t INTERNAL_LFT_PAGENUM_OFFSET = 120;
constexpr uint64_t INTERNAL_BRANCH_FACTOR_OFFSET = 128;



class page_t
{
private:
    char data[PAGE_SIZE];
    template<class T>
    void offsetCheck(uint16_t offset);
public:
    page_t();
    template<class T>
    void get_data(T* dest, uint16_t offset);
    template<class T>
    T get_data(uint16_t offset);
    void get_data(char* dest, uint16_t offset, uint16_t size);
    template<class T>
    void set_data(const T src, uint16_t offset);
    void set_data(const char* src, uint16_t offset, uint16_t size);
};

class slot_t {
private:
    char data[SLOT_SIZE];
public:
    slot_t();
    int64_t get_key() const;
    uint16_t get_size() const;
    uint16_t get_offset() const;
    int get_trx_id() const;
    void set_key(int64_t key);
    void set_size(uint16_t size);
    void set_offset(uint16_t offset);
    void set_trx_id(int trx_id);
};

class branch_factor_t{
private:
    char data[BRANCH_FACTOR_SIZE];
public:
    branch_factor_t();
    int64_t get_key() const;
    pagenum_t get_pagenum() const;
    void set_key(int64_t key);
    void set_pagenum(pagenum_t pagenum);
};

namespace PageIO {
    namespace HeaderPage {
        pagenum_t get_free_pagenum(page_t* page);
        uint64_t get_num_pages(page_t* page);
        pagenum_t get_root_pagenum(page_t* page);
        void set_free_pagenum(page_t* page, pagenum_t free_pagenum);
        void set_num_pages(page_t* page, uint64_t num_pages);
        void set_root_pagenum(page_t* page, pagenum_t root_pagenum);
    }
    namespace FreePage {
        pagenum_t get_next_free_pagenum(page_t* page);
        void set_next_free_pagenum(page_t* page, pagenum_t next_free_pagenum);
    }
    namespace BPT {
        pagenum_t get_parent_pagenum(page_t* page);
        int get_is_leaf(page_t* page);
        int get_num_keys(page_t* page);
        void set_parent_pagenum(page_t* page, pagenum_t parent_pagenum);
        void set_is_leaf(page_t* page, int is_leaf);
        void set_num_keys(page_t* page, int num_keys);

        namespace InternalPage {
            pagenum_t get_leftmost_pagenum(page_t* page);
            branch_factor_t get_nth_branch_factor(page_t* page, int n);
            void set_leftmost_pagenum(page_t* page, pagenum_t leftmost_pagenum);
            void set_nth_branch_factor(page_t* page, int n, branch_factor_t branch_factor);
        }
        namespace LeafPage {
            uint64_t get_amount_free_space(page_t* page);
            pagenum_t get_right_sibling_pagenum(page_t* page);
            slot_t get_nth_slot(page_t* page, int n);
            void set_amount_free_space(page_t* page, uint64_t amount_free_space);
            void set_right_sibling_pagenum(page_t* page, pagenum_t right_sibling_pagenum);
            void set_nth_slot(page_t* page, int n, slot_t slot);

            // void get_data_at(page_t* page, int64_t offset, char *dest, int16_t size);
            // void set_data_at(page_t* page, int64_t offset, const char *src, int16_t size);
            
        }
    }
}

#endif //__PAGE_H__