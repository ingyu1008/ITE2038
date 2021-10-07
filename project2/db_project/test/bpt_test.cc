#include "file.h"
#include "mybpt.h"

#include <gtest/gtest.h>

TEST(BPlus, FindOperation) {
    int table_id = open_table("db.txt");

    uint16_t val_size;
    for (int i = -10; i <= 10010; i++) {
        char ret_val[112];
        int res = db_find(table_id, i, ret_val, &val_size);
        if (i <= 0 || i > 10000) {
            // std::cout << "Cannot Find " << i << std::endl;
            EXPECT_EQ(res, 1);
        }
        else {
            EXPECT_EQ(res, 0);
            // std::cout << "Found " << i << ": ";
            // for (int j = 0; j < val_size; j++) {
            //     std::cout << ret_val[j];
            // }
            // std::cout << "\n";
        }
    }
}

TEST(BPlus, InsertOperationSmall) {
    if (std::remove("insertTest.dat") == 0)
    {
        std::cout << "File 'insertTest.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("insertTest.dat");
    std::cout << table_id << std::endl;

    for (int i = 1; i <= 100; i++) {
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
        if (res) {
            std::cout << "Error inserting with key = " << i << std::endl;
        }
    }

    std::cout << "Successfully inserted 100 records." << std::endl;

    int err = 0;
    uint16_t val_size;
    for (int i = -10; i <= 110; i++) {
        char ret_val[112];
        int res = db_find(table_id, i, ret_val, &val_size);
        if (i <= 0 || i > 100) {
            EXPECT_EQ(res, 1);
        }
        else {
            EXPECT_EQ(res, 0);
            // for (int j = 0; j < val_size; j++) {
            //     std::cout << ret_val[j];
            // }
            // std::cout << "\n";
        }
        err += res;
        if (res) {
            std::cout << "Could not find record with key = " << i << std::endl;
        }
    }
    if (err) {
        std::cout << "Something has gone wrong." << std::endl;
    }
    else {
        std::cout << "Successfully found all 100 records." << std::endl;
    }
}

TEST(BPlus, InsertOperationLarge) {
    if (std::remove("insertTest.dat") == 0)
    {
        std::cout << "File 'insertTest.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("insertTest.dat");

    int n = 100000;

    for (int i = 1; i <= n; i++) {
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
        if (res) {
            std::cout << "Error inserting with key = " << i << std::endl;
        }
    }

    std::cout << "Successfully inserted " << n << " records." << std::endl;

    int err = 0;
    uint16_t val_size;
    for (int i = -10; i <= n + 10; i++) {
        char ret_val[112];
        int res = db_find(table_id, i, ret_val, &val_size);
        if (i <= 0 || i > n) {
            EXPECT_EQ(res, 1);
        }
        else {
            EXPECT_EQ(res, 0);
            err += res;
            // for (int j = 0; j < val_size; j++) {
            //     std::cout << ret_val[j];
            // }
            // std::cout << "\n";
        }
        if (res) {
            std::cout << "Could not find record with key = " << i << std::endl;
        }
    }
    if (err) {
        std::cout << "Something has gone wrong." << std::endl;
    }
    else {
        std::cout << "Successfully found all " << n << " records." << std::endl;;
    }
}