#ifndef __RECOVERY_H__
#define __RECOVERY_H__

#include "page.h"
#include <cstdio>
#include <vector>
#include <set>

#define LOG_BEGIN 0
#define LOG_UPDATE 1
#define LOG_COMMIT 2
#define LOG_ROLLBACK 3
#define LOG_COMPENSATE 4
#define LOG_BUFFER_SIZE 64

constexpr int LOG_ENTRY_SIZE = 28;
constexpr int LOG_ENTRY_EXT_SIZE = 48;

class log_entry_t{
public:
    char *data;
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

log_entry_t* create_begin_log(int trx_id);
log_entry_t* create_update_log(int trx_id, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t length, const char *old, const char *new_);
log_entry_t* create_commit_log(int trx_id);
log_entry_t* create_rollback_log(int trx_id);
log_entry_t* create_compensate_log(int trx_id, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t length, const char *old, const char *new_, uint64_t next_undo_lsn);

uint64_t add_to_log_buffer(log_entry_t *log);
void log_write(log_entry_t *log);
void log_flush();
int init_recovery(char * log_path);
int shutdown_recovery();

void recover_main(char* logmsg_path, int flag, int log_num)

#endif // __RECOVERY_H__