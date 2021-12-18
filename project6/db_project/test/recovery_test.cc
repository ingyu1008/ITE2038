#include <gtest/gtest.h>
#include "recovery.h"
#include "mybpt.h"
#include "trx.h"

#define BUF_SIZE 3

TEST(RecoveryTest, LogCreationTest){
    EXPECT_EQ(init_db(BUF_SIZE, 0, 0, "logfile.data", "logmsg.txt"), 0);

    std::cout << "[DEBUG] open table" << std::endl;
    int table_id = open_table("DATA1");
    std::cout << "[DEBUG] open table end" << std::endl;

    int n = 3;

    for (int i = 1; i <= n; i++) {
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    int err = 0;
    uint16_t val_size;
    for (int i = 1; i <= n; i++) {
        char ret_val[112];
        int res = db_find(table_id, i, ret_val, &val_size, trx_id);
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Successfully found all " << n << " records." << std::endl;

    trx_commit(trx_id);

    std::cout << "[INFO] Trx successfully committed." << std::endl;

    trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Updating key = " << i << std::endl;
        #endif
        std::string data = "12345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t old_val_size = 0;
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), &old_val_size, trx_id);
        EXPECT_EQ(res, 0);
    }
    trx_commit(trx_id);

    trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Updating key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        uint16_t old_val_size = 0;
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), &old_val_size, trx_id);
        EXPECT_EQ(res, 0);
    }

    trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    trx_commit(trx_id);

    //crash before commit
}