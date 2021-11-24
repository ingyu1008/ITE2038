#include "file.h"
#include "mybpt.h"
#include "trx.h"
#include <random>

#include <gtest/gtest.h>
#define BUF_SIZE 2048

TEST(ConcurrencyCtrl, OneTrxOneThread) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("singleThreaded.dat") == 0)
    {
        std::cout << "[INFO] File 'singleThreaded.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("singleThreaded.dat");

    int n = 100;
    int x = 0;

    for (int i = 1; i <= n; i++) {
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
        if (res) {
            std::cout << "[ERROR] Error inserting with key = " << i << std::endl;
        } else {
            x++;
        }
    }

    if (x == n) {
        std::cout << "[INFO] Successfully inserted " << n << " records." << std::endl;
    } else {
        FAIL();
    }

    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    int err = 0;
    uint16_t val_size;
    for (int i = -10; i <= n + 10; i++) {
        char ret_val[112];
        int res = db_find(table_id, i, ret_val, &val_size, trx_id);
        if (i <= 0 || i > n) {
            EXPECT_EQ(res, -1);
            if( res != -1 ){
                std::cout << "[ERROR] Error finding with key = " << i << std::endl;
                err++;
            }
        } else {
            EXPECT_EQ(res, 0);
            if(res != 0){
                std::cout << "[ERROR] Error finding with key = " << i << std::endl;
                err++;
            }
        }
        if (res) {
            std::cout << "[INFO] Could not find record with key = " << i << std::endl;
        }
    }

    if (err) {
        std::cout << "[FATAL] Something has gone wrong. err = " << err << std::endl;
        FAIL();
    } else {
        std::cout << "[INFO] Successfully found all " << n << " records." << std::endl;
    }

    trx_commit(trx_id);
    EXPECT_EQ(shutdown_db(), 0);
}