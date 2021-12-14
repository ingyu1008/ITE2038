#include "recovery.h"

log_entry_t::log_entry_t() {
    data = new char[LOG_ENTRY_SIZE];
    memset(data, 0, LOG_ENTRY_SIZE);
    *(int*)data = LOG_ENTRY_SIZE;
}

log_entry_t::log_entry_t(int data_length, int compensate) {
    int size = LOG_ENTRY_EXT_SIZE + 2 * data_length + 8 * compensate;
    data = new char[size];
    memset(data, 0, size);
    *(int*)data = size;
}

log_entry_t::log_entry_t(int size) {
    data = new char[size];
    memset(data, 0, size);
    *(int*)data = size;
}

log_entry_t::~log_entry_t() {
    delete[] data;
}

int log_entry_t::get_log_size() const {
    return *(int*)data;
}

uint64_t log_entry_t::get_lsn() const {
    return *(uint64_t*)(data + 4);
}
uint64_t log_entry_t::get_prev_lsn() const {
    return *(uint64_t*)(data + 12);
}
int log_entry_t::get_trx_id() const {
    return *(int*)(data + 20);
}
int log_entry_t::get_type() const {
    return *(int*)(data + 24);
}
int64_t log_entry_t::get_table_id() const {
    return *(int64_t*)(data + 28);
}
pagenum_t log_entry_t::get_pagenum() const {
    return *(pagenum_t*)(data + 36);
}
uint16_t log_entry_t::get_offset() const {
    return *(uint16_t*)(data + 44);
}
uint16_t log_entry_t::get_length() const {
    return *(uint16_t*)(data + 46);
}
void log_entry_t::get_old_image(char* dest) const {
    memcpy(dest, data + 48, get_length());
} // caller allocates
void log_entry_t::get_new_image(char* dest) const {
    memcpy(dest, data + 48 + get_length(), get_length());
} // caller allocates
uint64_t log_entry_t::get_next_undo_lsn() const {
    return *(uint64_t*)(data + 48 + 2 * get_length());
}

void log_entry_t::set_lsn(uint64_t lsn){
    *(uint64_t*)(data + 4) = lsn;
}
void log_entry_t::set_prev_lsn(uint64_t prev_lsn){
    *(uint64_t*)(data + 12) = prev_lsn;
}
void log_entry_t::set_trx_id(int trx_id){
    *(int*)(data + 20) = trx_id;
}
void log_entry_t::set_type(int type){
    *(int*)(data + 24) = type;
}
void log_entry_t::set_table_id(int64_t table_id){
    *(int64_t*)(data + 28) = table_id;
}
void log_entry_t::set_pagenum(pagenum_t pagenum){
    *(pagenum_t*)(data + 36) = pagenum;
}
void log_entry_t::set_offset(uint16_t offset){
    *(uint16_t*)(data + 44) = offset;
}
void log_entry_t::set_length(uint16_t length){
    *(uint16_t*)(data + 46) = length;
}
void log_entry_t::set_old_image(const char* src){
    memcpy(data + 48, src, get_length());
}
void log_entry_t::set_new_image(const char* src){
    memcpy(data + 48 + get_length(), src, get_length());
}
void log_entry_t::set_next_undo_lsn(uint64_t next_undo_lsn){
    *(uint64_t*)(data + 48 + 2 * get_length()) = next_undo_lsn;
}