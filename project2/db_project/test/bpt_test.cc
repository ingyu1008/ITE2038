#include "file.h"
#include "mybpt.h"
#include <random>

#include <gtest/gtest.h>

TEST(BPlus, InsertOperationSmall) {
    if (std::remove("insertTest.dat") == 0)
    {
        std::cout << "File 'insertTest.dat' already exists. Deleting it." << std::endl;
    }

    EXPECT_EQ(init_db(), 0);

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
        } else {
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
    } else {
        std::cout << "Successfully found all 100 records." << std::endl;
    }

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(BPlus, DeleteOperationSmall) {
    EXPECT_EQ(init_db(), 0);

    int table_id = open_table("insertTest.dat");
    char buffer[MAX_VAL_SIZE];
    uint16_t val_size;

    int mn = 10;
    int n = 100;

    std::set<int> st;
    for (int i = 1; i <= n; i++) {
        st.insert(i);
    }


    db_print_tree(table_id);

    for (int i = mn; i <= n; i++) {
        EXPECT_EQ(db_find(table_id, i, buffer, &val_size), 0);
        EXPECT_EQ(db_delete(table_id, i), 0);
        st.erase(i);
        for (int j = 1; j <= 100; j++) {
            EXPECT_EQ(db_find(table_id, j, buffer, &val_size), st.find(j) == st.end());
        }
        db_print_tree(table_id);
    }

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(BPlus, InsertOperationLarge) {
    if (std::remove("insertTest.dat") == 0)
    {
        std::cout << "File 'insertTest.dat' already exists. Deleting it." << std::endl;
    }

    EXPECT_EQ(init_db(), 0);

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
        } else {
            EXPECT_EQ(res, 0);
            err += res;
        }
        if (res) {
            std::cout << "Could not find record with key = " << i << std::endl;
        }
    }
    if (err) {
        std::cout << "Something has gone wrong." << std::endl;
    } else {
        std::cout << "Successfully found all " << n << " records." << std::endl;;
    }

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(BPlus, DeleteOperationLarge) {
    EXPECT_EQ(init_db(), 0);

    int table_id = open_table("insertTest.dat");
    char buffer[MAX_VAL_SIZE];
    uint16_t val_size;

    int mn = 1;
    int n = 100000;

    std::set<int> st;
    for (int i = 1; i <= n; i++) {
        st.insert(i);
    }

    std::mt19937 gen(2020011776);
    std::uniform_int_distribution<int> rand(1, n);

    int i;
    try {
        for (i = mn; i <= n; i++) {

            EXPECT_EQ(db_find(table_id, i, buffer, &val_size), 0);
            if(db_find(table_id, i, buffer, &val_size) == 1){
                throw std::invalid_argument("Tlqkf");
            }
            EXPECT_EQ(db_delete(table_id, i), 0);
            st.erase(i);
            for (int j = 1; j <= 10; j++) {
                int rnd = rand(gen);
                EXPECT_EQ(db_find(table_id, rnd, buffer, &val_size), st.find(rnd) == st.end());
            }

#ifdef DEBUG
            db_print_tree(table_id);
            printTotalCalls();
#endif
        }
    }
    catch (std::invalid_argument e) {
        std::cout << "[FATAL] Something went wrong while deleting " << i << std::endl;
        std::cout << "[FATAL] " << e.what() << std::endl;

        db_print_tree(table_id);
        printCallStack();
        shutdown_db();
        FAIL();
    }

    EXPECT_EQ(shutdown_db(), 0);
}