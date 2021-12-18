#include "recovery.h"
#include "mybpt.h"
#include "buffer.h"
#include "page.h"
#include "trx.h"

pthread_mutex_t log_buffer_mutex;

FILE* log_file;

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

void log_entry_t::set_lsn(uint64_t lsn) {
    *(uint64_t*)(data + 4) = lsn;
}
void log_entry_t::set_prev_lsn(uint64_t prev_lsn) {
    *(uint64_t*)(data + 12) = prev_lsn;
}
void log_entry_t::set_trx_id(int trx_id) {
    *(int*)(data + 20) = trx_id;
}
void log_entry_t::set_type(int type) {
    *(int*)(data + 24) = type;
}
void log_entry_t::set_table_id(int64_t table_id) {
    *(int64_t*)(data + 28) = table_id;
}
void log_entry_t::set_pagenum(pagenum_t pagenum) {
    *(pagenum_t*)(data + 36) = pagenum;
}
void log_entry_t::set_offset(uint16_t offset) {
    *(uint16_t*)(data + 44) = offset;
}
void log_entry_t::set_length(uint16_t length) {
    *(uint16_t*)(data + 46) = length;
}
void log_entry_t::set_old_image(const char* src) {
    memcpy(data + 48, src, get_length());
}
void log_entry_t::set_new_image(const char* src) {
    memcpy(data + 48 + get_length(), src, get_length());
}
void log_entry_t::set_next_undo_lsn(uint64_t next_undo_lsn) {
    *(uint64_t*)(data + 48 + 2 * get_length()) = next_undo_lsn;
}

std::vector<log_entry_t*> log_buffer;

uint64_t next_lsn = 0;

log_entry_t* create_begin_log(int trx_id) {
    log_entry_t* entry = new log_entry_t();
    entry->set_trx_id(trx_id);
    entry->set_type(LOG_BEGIN);

    return entry;
}

log_entry_t* create_update_log(int trx_id, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t length, const char* old, const char* new_) {
    log_entry_t* entry = new log_entry_t(length, 0);
    entry->set_trx_id(trx_id);
    entry->set_type(LOG_UPDATE);
    entry->set_table_id(table_id);
    entry->set_pagenum(pagenum);
    entry->set_offset(offset);
    entry->set_length(length);
    entry->set_old_image(old);
    entry->set_new_image(new_);

    return entry;
}

log_entry_t* create_commit_log(int trx_id) {
    log_entry_t* entry = new log_entry_t();
    entry->set_trx_id(trx_id);
    entry->set_type(LOG_COMMIT);

    return entry;
}

log_entry_t* create_rollback_log(int trx_id) {
    log_entry_t* entry = new log_entry_t();
    entry->set_trx_id(trx_id);
    entry->set_type(LOG_ROLLBACK);

    return entry;
}

log_entry_t* create_compensate_log(int trx_id, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t length, const char* old, const char* new_, uint64_t next_undo_lsn) {
    log_entry_t* entry = new log_entry_t(length, 1);
    entry->set_trx_id(trx_id);
    entry->set_type(LOG_COMPENSATE);
    entry->set_table_id(table_id);
    entry->set_pagenum(pagenum);
    entry->set_offset(offset);
    entry->set_length(length);
    entry->set_old_image(old);
    entry->set_new_image(new_);
    entry->set_next_undo_lsn(next_undo_lsn);

    return entry;
}

void _log_flush() {
    for (int i = 0; i < log_buffer.size(); i++) {
        log_entry_t* log = log_buffer[i];
        log_write(log);
    }
    log_buffer.clear();
    fflush(log_file);
}

uint64_t add_to_log_buffer(log_entry_t* log) {
    pthread_mutex_lock(&log_buffer_mutex);
    if (log_buffer.size() == LOG_BUFFER_SIZE) {
        _log_flush();
    }
    log->set_lsn(next_lsn);
    log->set_prev_lsn(trx_table[log->get_trx_id()]->last_lsn);
    next_lsn += log->get_log_size();
    trx_table[log->get_trx_id()]->last_lsn = log->get_lsn();
    log_buffer.push_back(log);
    pthread_mutex_unlock(&log_buffer_mutex);
    return log->get_lsn();
}

void log_write(log_entry_t* log) {
    int sz = log->get_log_size();
    for (int i = 0; i < sz; i++) {
        fprintf(log_file, "%c", log->data[i]);
    }
    // fwrite(const_cast<char*>(log->data), 1, log->get_log_size(), log_file);
}

void log_flush() {
    pthread_mutex_lock(&log_buffer_mutex);
    _log_flush();
    pthread_mutex_unlock(&log_buffer_mutex);
}

int init_recovery(char* log_path) {
    log_file = fopen(log_path, "a+");
    pthread_mutex_init(&log_buffer_mutex, NULL);
    return 0;
}

int shutdown_recovery() {
    fclose(log_file);
    pthread_mutex_destroy(&log_buffer_mutex);
    return 0;
}


void recover_main(char* logmsg_path, int flag, int log_num) {
    FILE* logmsg_file = fopen(logmsg_path, "w");
    rewind(log_file);
    // Analysis Pass
    std::set<int> winners, opened_tables;
    std::map<int, uint64_t> losers;

    fprintf(logmsg_file, "[ANALYSIS] Analysis pass start\n");
    while (true) {
        int sz;
        if (fread(&sz, sizeof(int), 1, log_file) != 1) break;
        log_entry_t* log = new log_entry_t(sz);
        fseek(log_file, -sizeof(int), SEEK_CUR);
        fread(log->data, 1, sz, log_file);
        losers[log->get_trx_id()] = log->get_lsn();
        if (log->get_type() == LOG_COMMIT || log->get_type() == LOG_ROLLBACK) {
            winners.insert(log->get_trx_id());
            losers.erase(log->get_trx_id());
        }
        next_lsn += log->get_log_size();

        delete log;
    }
    fprintf(logmsg_file, "[ANALYSIS] Analysis success. Winner: ");
    for (auto it = winners.begin();it != winners.end();) {
        fprintf(logmsg_file, "%d", *it);
        it++;
        if (it == winners.end()) break;
        fprintf(logmsg_file, " ");
    }
    fprintf(logmsg_file, ", Loser: ");
    for (auto it = losers.begin();it != losers.end();) {
        fprintf(logmsg_file, "%d", it->first);
        it++;
        if (it == losers.end()) break;
        fprintf(logmsg_file, " ");
    }
    fprintf(logmsg_file, "\n");
    // Analysis Pass Done

    // Redo Pass
    fprintf(logmsg_file, "[REDO] Redo pass start\n");

    rewind(log_file);
    int redo = 0;
    while (flag != 1 || redo < log_num) {
        redo++;
        int sz;
        if (fread(&sz, sizeof(int), 1, log_file) != 1) break;
        log_entry_t* log = new log_entry_t(sz);
        fseek(log_file, -sizeof(int), SEEK_CUR);
        fread(log->data, 1, sz, log_file);


        // if (log->get_type() == LOG_BEGIN) {
        //     fprintf(logmsg_file, "LSN %llu [BEGIN] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        // } else if(log->get_type() == LOG_COMMIT){
        //     fprintf(logmsg_file, "LSN %llu [COMMIT] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        // } else if(log->get_type() == LOG_ROLLBACK){
        //     fprintf(logmsg_file, "LSN %llu [ROLLBACK] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        // } else if(log->get_type() == LOG_COMPENSATE){
        //     fprintf(logmsg_file, "LSN %llu [CLR] next undo lsn %llu", log->get_lsn(), log->get_next_undo_lsn());
        // } else {
        //     fprintf(logmsg_file, "LSN %llu [UNKNOWN]\n", log->get_lsn());
        // }


        if (log->get_type() == LOG_UPDATE || log->get_type() == LOG_COMPENSATE) {
            if (log->get_type() == LOG_COMPENSATE) {
                fprintf(logmsg_file, "LSN %lu [CLR] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
            }
            int64_t table_id = log->get_table_id();
            if (opened_tables.find(table_id) == opened_tables.end()) {
                opened_tables.insert(table_id);
                std::string filename = "DATA";
                filename += std::to_string(table_id);
                open_table(const_cast<char*>(filename.c_str()));
            }

            control_block_t* ctrl_block = buf_read_page(table_id, log->get_pagenum());
            if (PageIO::BPT::get_page_lsn(ctrl_block->frame) < log->get_lsn()) {
                fprintf(logmsg_file, "LSN %lu [UPDATE] Transaction id %d redo apply\n", log->get_lsn(), log->get_trx_id());
                PageIO::BPT::set_page_lsn(ctrl_block->frame, log->get_lsn());
                char* buffer = new char[log->get_length()];
                log->get_new_image(buffer);
                ctrl_block->frame->set_data(const_cast<const char*>(buffer), log->get_offset(), log->get_length());
                buf_return_ctrl_block(&ctrl_block, 1);
                delete[] buffer;
            } else {
                if (log->get_type() == LOG_UPDATE) {
                    fprintf(logmsg_file, "LSN %lu [COONSIDER-REDO] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
                }
                buf_return_ctrl_block(&ctrl_block);
            }
        } else if (log->get_type() == LOG_BEGIN) {
            fprintf(logmsg_file, "LSN %lu [BEGIN] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        } else if (log->get_type() == LOG_COMMIT) {
            fprintf(logmsg_file, "LSN %lu [COMMIT] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        } else if (log->get_type() == LOG_ROLLBACK) {
            fprintf(logmsg_file, "LSN %lu [ROLLBACK] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        } else if (log->get_type() == LOG_COMPENSATE) {
            fprintf(logmsg_file, "LSN %lu [CLR] next undo lsn %lu\n", log->get_lsn(), log->get_next_undo_lsn());
        } else {
            fprintf(logmsg_file, "LSN %lu [UNKNOWN]\n", log->get_lsn());
        }

        delete log;
    }

    fprintf(logmsg_file, "[REDO] Redo pass end\n");
    // Redo Pass Done

    if(flag == 1){
        log_flush();
        return;
    }

    // Undo Pass
    fprintf(logmsg_file, "[UNDO] Undo pass start\n");

    int undo = 0;
    while (losers.size() > 0 && (flag != 2 || undo < log_num)) {
        undo++;
        int next_trx;
        uint64_t next_entry = 0;
        for (auto& x : losers) {
            if (next_entry < x.second) {
                next_trx = x.first;
                next_entry = x.second;
            }
            if (trx_table.find(x.first) == trx_table.end()) trx_resurrect(x.first, x.second);
        }
        fseek(log_file, next_entry, SEEK_SET);
        int sz;
        fread(&sz, sizeof(int), 1, log_file);
        log_entry_t* log = new log_entry_t(sz);
        fseek(log_file, -sizeof(int), SEEK_CUR);
        fread(log->data, 1, sz, log_file);
        
        if(log->get_type() == LOG_COMPENSATE){
            fprintf(logmsg_file, "LSN %lu [CLR] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
            losers[log->get_trx_id()] = log->get_next_undo_lsn();
        }

        if (log->get_type() == LOG_UPDATE) {
            control_block_t* ctrl_block = buf_read_page(log->get_table_id(), log->get_pagenum());
            if (PageIO::BPT::get_page_lsn(ctrl_block->frame) >= log->get_lsn()) {
                fprintf(logmsg_file, "LSN %lu [UPDATE] Transaction id %d undo apply\n", log->get_lsn(), log->get_trx_id());
                
                int length = log->get_length();
                char* old = new char[length];
                log->get_old_image(old);
                char* new_ = new char[length];
                log->get_new_image(new_);
                log_entry_t* new_log = create_compensate_log(log->get_trx_id(), log->get_table_id(), log->get_pagenum(), log->get_offset(), log->get_length(), new_, old, log->get_prev_lsn());
                uint64_t new_lsn = add_to_log_buffer(new_log);

                PageIO::BPT::set_page_lsn(ctrl_block->frame, new_lsn);
                ctrl_block->frame->set_data(const_cast<const char*>(old), log->get_offset(), length);
                buf_return_ctrl_block(&ctrl_block, 1);
                delete[] old, new_;
            } else {
                buf_return_ctrl_block(&ctrl_block);
            }
            losers[log->get_trx_id()] = log->get_prev_lsn();
        } else if (log->get_type() == LOG_BEGIN) {
            fprintf(logmsg_file, "LSN %lu [BEGIN] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
            log_entry_t* entry = create_rollback_log(log->get_trx_id());
            uint64_t lsn = add_to_log_buffer(entry);
            losers[log->get_trx_id()] = lsn;
            trx_remove(log->get_trx_id());
            losers.erase(log->get_trx_id());
        } else if (log->get_type() == LOG_COMMIT) {
            fprintf(logmsg_file, "LSN %lu [COMMIT] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        } else if (log->get_type() == LOG_ROLLBACK) {
            fprintf(logmsg_file, "LSN %lu [ROLLBACK] Transaction id %d\n", log->get_lsn(), log->get_trx_id());
        } else {
            fprintf(logmsg_file, "LSN %lu [UNKNOWN]\n", log->get_lsn());
        }

        delete log;


    }

    log_flush();

    fprintf(logmsg_file, "[UNDO] Undo pass end\n");
    // Undo Pass Done

    fflush(logmsg_file);

}