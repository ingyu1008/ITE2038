#ifndef __RECOVERY_H__
#define __RECOVERY_H__

#include "page.h"
#include <cstdio>

constexpr int LOG_ENTRY_SIZE = 28;
constexpr int LOG_ENTRY_EXT_SIZE = 48;

class log_entry_t{
private:
    char *data;
public:
    log_entry_t();
    log_entry_t(int data_length, int compensate);
    log_entry_t(int size);
    ~log_entry_t();

    // getters
    // [CAUTION] does not check if data array is big enough
    int get_log_size() const;
    uint64_t get_lsn() const;
    uint64_t get_prev_lsn() const;
    int get_trx_id() const;
    int get_type() const;
    int64_t get_table_id() const;
    pagenum_t get_pagenum() const;
    uint16_t get_offset() const;
    uint16_t get_length() const;
    void get_old_image(char *dest) const; // caller allocates
    void get_new_image(char *dest) const; // caller allocates
    uint64_t get_next_undo_lsn() const;

    // setters
    void set_lsn(uint64_t lsn);
    void set_prev_lsn(uint64_t prev_lsn);
    void set_trx_id(int trx_id);
    void set_type(int type);
    void set_table_id(int64_t table_id);
    void set_pagenum(pagenum_t pagenum);
    void set_offset(uint16_t offset);
    void set_length(uint16_t length);
    void set_old_image(const char *src);
    void set_new_image(const char *src);
    void set_next_undo_lsn(uint64_t next_undo_lsn);
};

#endif // __RECOVERY_H__