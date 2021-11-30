#include "page.h"

template<class T>
void page_t::offsetCheck(uint16_t offset) {
    if (offset + sizeof(T) > PAGE_SIZE) {
        std::cout << "[ERROR]: Invalid Offset at get_data() offset = " << offset << std::endl;
        throw std::invalid_argument("Offset is out of range.");
    }
}
page_t::page_t() { std::fill_n(data, PAGE_SIZE, '\0'); };
template<class T>
T page_t::get_data(uint16_t offset) {
    offsetCheck<T>(offset);
    T ret;
    std::memcpy(&ret, data + offset, sizeof(T));
    return ret;
}
template<class T>
void page_t::get_data(T* dest, uint16_t offset) {
    offsetCheck<T>(offset);
    std::memcpy(dest, data + offset, sizeof(T));
}

void page_t::get_data(char* dest, uint16_t offset, uint16_t size) {
    std::memcpy(dest, data + offset, size);
}

template<class T>
void page_t::set_data(const T src, uint16_t offset) {
    offsetCheck<T>(offset);
    std::memcpy(data + offset, &src, sizeof(T));
}

void page_t::set_data(const char* src, uint16_t offset, uint16_t size) {
    std::memcpy(data + offset, src, size);
}

slot_t::slot_t() { std::fill_n(data, SLOT_SIZE, '\0'); }
int64_t slot_t::get_key() const {
    int64_t ret;
    std::memcpy(&ret, data + SLOT_KEY_OFFSET, sizeof(ret));
    return ret;
}
uint16_t slot_t::get_size() const {
    uint16_t ret;
    std::memcpy(&ret, data + SLOT_SIZE_OFFSET, sizeof(ret));
    return ret;
}
uint16_t slot_t::get_offset() const {
    uint16_t ret;
    std::memcpy(&ret, data + SLOT_OFFSET_OFFSET, sizeof(ret));
    return ret;
}
void slot_t::set_key(int64_t key) {
    std::memcpy(data + SLOT_KEY_OFFSET, &key, sizeof(key));
}
void slot_t::set_size(uint16_t size) {
    std::memcpy(data + SLOT_SIZE_OFFSET, &size, sizeof(size));
}
void slot_t::set_offset(uint16_t offset) {
    std::memcpy(data + SLOT_OFFSET_OFFSET, &offset, sizeof(offset));
}

branch_factor_t::branch_factor_t() { std::fill_n(data, BRANCH_FACTOR_SIZE, '\0'); }
int64_t branch_factor_t::get_key() const {
    int64_t ret = 0;
    std::memcpy(&ret, data + BF_KEY_OFFSET, sizeof(ret));
    return ret;
}
pagenum_t branch_factor_t::get_pagenum() const {
    pagenum_t ret = 0;
    std::memcpy(&ret, data + BF_PAGENUM_OFFSET, sizeof(ret));
    return ret;
}
void branch_factor_t::set_key(int64_t key) {
    std::memcpy(data + BF_KEY_OFFSET, &key, sizeof(key));
}
void branch_factor_t::set_pagenum(pagenum_t pagenum) {
    std::memcpy(data + BF_PAGENUM_OFFSET, &pagenum, sizeof(pagenum));
}

pagenum_t PageIO::HeaderPage::get_free_pagenum(page_t* page) {
    return page->get_data<pagenum_t>(HEADER_FREE_OFFSET);
}
uint64_t PageIO::HeaderPage::get_num_pages(page_t* page) {
    return page->get_data<uint64_t>(HEADER_NUMPAGE_OFFSET);
}
pagenum_t PageIO::HeaderPage::get_root_pagenum(page_t* page) {
    return page->get_data<pagenum_t>(HEADER_ROOT_PAGENUM_OFFSET);
}
void PageIO::HeaderPage::set_free_pagenum(page_t* page, pagenum_t free_pagenum) {
    page->set_data(free_pagenum, HEADER_FREE_OFFSET);
}
void PageIO::HeaderPage::set_num_pages(page_t* page, uint64_t num_pages) {
    page->set_data(num_pages, HEADER_NUMPAGE_OFFSET);
}
void PageIO::HeaderPage::set_root_pagenum(page_t* page, pagenum_t root_pagenum) {
    page->set_data(root_pagenum, HEADER_ROOT_PAGENUM_OFFSET);
}

pagenum_t PageIO::FreePage::get_next_free_pagenum(page_t* page) {
    return page->get_data<pagenum_t>(FREE_FREE_OFFSET);
}
void PageIO::FreePage::set_next_free_pagenum(page_t* page, pagenum_t next_free_pagenum) {
    page->set_data(next_free_pagenum, FREE_FREE_OFFSET);
}

pagenum_t PageIO::BPT::get_parent_pagenum(page_t* page) {
    return page->get_data<pagenum_t>(PH_PARENT_PAGENUM_OFFSET);
}
int PageIO::BPT::get_is_leaf(page_t* page) {
    return page->get_data<int>(PH_IS_LEAF_OFFSET);
}
int PageIO::BPT::get_num_keys(page_t* page) {
    return page->get_data<int>(PH_NUM_KEYS_OFFSET);
}
void PageIO::BPT::set_parent_pagenum(page_t* page, pagenum_t parent_pagenum) {
    page->set_data(parent_pagenum, PH_PARENT_PAGENUM_OFFSET);
}
void PageIO::BPT::set_is_leaf(page_t* page, int is_leaf) {
    page->set_data(is_leaf, PH_IS_LEAF_OFFSET);
}
void PageIO::BPT::set_num_keys(page_t* page, int num_keys) {
    page->set_data(num_keys, PH_NUM_KEYS_OFFSET);
}

pagenum_t PageIO::BPT::InternalPage::get_leftmost_pagenum(page_t* page) {
    return page->get_data<pagenum_t>(INTERNAL_LFT_PAGENUM_OFFSET);
}
branch_factor_t PageIO::BPT::InternalPage::get_nth_branch_factor(page_t* page, int n) {
    branch_factor_t ret;
    page->get_data(&ret, INTERNAL_BRANCH_FACTOR_OFFSET + n * sizeof(branch_factor_t));
    return ret;
}
void PageIO::BPT::InternalPage::set_leftmost_pagenum(page_t* page, pagenum_t pagenum) {
    page->set_data(pagenum, INTERNAL_LFT_PAGENUM_OFFSET);
}
void PageIO::BPT::InternalPage::set_nth_branch_factor(page_t* page, int n, branch_factor_t branch_factor) {
    page->set_data(branch_factor, INTERNAL_BRANCH_FACTOR_OFFSET + n * sizeof(branch_factor_t));
}

uint64_t PageIO::BPT::LeafPage::get_amount_free_space(page_t* page) {
    return page->get_data<uint64_t>(LEAF_AMOUNT_FREE_SPACE_OFFSET);
}
pagenum_t PageIO::BPT::LeafPage::get_right_sibling_pagenum(page_t* page) {
    return page->get_data<pagenum_t>(LEAF_RIGHT_SIB_PNUM_OFFSET);
}
slot_t PageIO::BPT::LeafPage::get_nth_slot(page_t* page, int n) {
    slot_t ret;
    page->get_data(&ret, LEAF_SLOT_OFFSET + n * sizeof(slot_t));
    return ret;
}
void PageIO::BPT::LeafPage::set_amount_free_space(page_t* page, uint64_t amount_free_space) {
    page->set_data(amount_free_space, LEAF_AMOUNT_FREE_SPACE_OFFSET);
}
void PageIO::BPT::LeafPage::set_right_sibling_pagenum(page_t* page, pagenum_t right_sibling_pagenum) {
    page->set_data(right_sibling_pagenum, LEAF_RIGHT_SIB_PNUM_OFFSET);
}
void PageIO::BPT::LeafPage::set_nth_slot(page_t* page, int n, slot_t slot) {
    page->set_data(slot, LEAF_SLOT_OFFSET + n * sizeof(slot_t));
}
