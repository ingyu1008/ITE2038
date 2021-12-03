#include "file.h"
#include "mybpt.h"
#include "trx.h"
#include <random>

#include <gtest/gtest.h>
#define BUF_SIZE 200
#define DEBUG_MODE 0
#define N 1000

TEST(ConcurrencyCtrl, SingleThread) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("singleThreaded.dat") == 0)
    {
        std::cout << "[INFO] File 'singleThreaded.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("singleThreaded.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    int err = 0;
    uint16_t val_size;
    for (int i = -10; i <= n + 10; i++) {
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
        char buffer[MAX_VAL_SIZE];
        res = db_find(table_id, i, buffer, &old_val_size, trx_id);
        EXPECT_EQ(res, 0);
        for (int j = 0; j < old_val_size; j++) {
            EXPECT_EQ(buffer[j], data[j]);
        }
    }
    trx_commit(trx_id);

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(ConcurrencyCtrl, SingleThreadRandom) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("singleThreaded.dat") == 0)
    {
        std::cout << "[INFO] File 'singleThreaded.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("singleThreaded.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    int err = 0;
    uint16_t val_size;
    for (int i = -10; i <= n + 10; i++) {
        char ret_val[112];
        int res = db_find(table_id, i, ret_val, &val_size, trx_id);
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Successfully found all " << n << " records." << std::endl;

    trx_commit(trx_id);

    std::cout << "[INFO] Trx successfully committed." << std::endl;


    std::vector<std::pair<int, int64_t>> v;
    for (int64_t i = 1; i <= n; i++) {
        v.emplace_back(0, i);
        v.emplace_back(0, i);
        v.emplace_back(0, i);
        v.emplace_back(0, i);
        v.emplace_back(1, i);
        v.emplace_back(1, i);
        v.emplace_back(1, i);
        v.emplace_back(1, i);
    }

    std::mt19937 gen(2020011776);
    std::shuffle(v.begin(), v.end(), gen);

    trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    char buffer[MAX_VAL_SIZE];
    int res;

    for (auto& p : v) {
        int i = p.second;
        uint16_t old_val_size = 0;

        if (p.first == 1) {
            #if DEBUG_MODE
            std::cout << "[DEBUG] Updating key = " << i << std::endl;
            #endif
            std::string data = "12345678901234567890123456789012345678901234567890" + std::to_string(i);

            res = db_find(table_id, i, buffer, &old_val_size, trx_id);
            EXPECT_EQ(res, 0);
            res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), &old_val_size, trx_id);
            EXPECT_EQ(res, 0);
            res = db_find(table_id, i, buffer, &old_val_size, trx_id);
            EXPECT_EQ(res, 0);

            for (int j = 0; j < old_val_size; j++) {
                EXPECT_EQ(buffer[j], data[j]);
            }
        } else {
            res = db_find(table_id, i, buffer, &old_val_size, trx_id);
            EXPECT_EQ(res, 0);
        }
    }

    trx_commit(trx_id);

    EXPECT_EQ(shutdown_db(), 0);
}

void* slock_only(void* arg) {
    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    int table_id = *((int*)arg);
    int n = N;
    int err = 0;
    uint16_t val_size;
    for (int i = 1; i <= n; i++) {
        char ret_val[112] = "\0";
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);

        int res = db_find(table_id, i, ret_val, &val_size, trx_id);
        EXPECT_EQ(res, 0);
        EXPECT_NE(val_size, 0);
    }

    trx_commit(trx_id);
    return NULL;
}

TEST(ConcurrencyCtrl, SLockOnlyTest) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("SLockOnly.dat") == 0)
    {
        std::cout << "[INFO] File 'SLockOnly.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("SLockOnly.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Population done, now testing SLockOnly Test" << std::endl;

    int m = 10;
    uint16_t val_size;
    pthread_t threads[m];
    // pthread_attr_t attr;
    // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
    for (int i = 0; i < m; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Create thread " << i << std::endl;
        #endif
        pthread_create(&threads[i], NULL, slock_only, &table_id);
    }

    for (int i = 0; i < m; i++) {
        pthread_join(threads[i], NULL);
    }

    EXPECT_EQ(shutdown_db(), 0);
}

void* xlock_only_disjoint(void* arg) {
    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    int table_id = ((int*)arg)[0];
    int n = N;
    int err = 0;
    uint16_t val_size;
    int st = ((int*)arg)[1];
    for (int i = st * (n / 10) + 1; i <= (st+1) * (n / 10); i++) {
        // char ret_val[112] = "\0";
        std::string data = "12345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t old_val_size = 0;
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), &old_val_size, trx_id);
        EXPECT_EQ(res, 0);
        // res = db_find(table_id, i, ret_val, &old_val_size);
        // EXPECT_EQ(res, 0);

        // for (int j = 0; j < val_size; j++) {
        //     EXPECT_EQ(ret_val[j], data[j]);
        // }
    }

    trx_commit(trx_id);
    return NULL;
}


TEST(ConcurrencyCtrl, XLockOnlyDisjointTest) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("XLockOnly.dat") == 0)
    {
        std::cout << "[INFO] File 'XLockOnly.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("XLockOnly.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Population done, now testing XLockOnly Test" << std::endl;

    int m = 10;
    uint16_t val_size;
    pthread_t threads[m];

    std::vector<int*> args;
    // pthread_attr_t attr;
    // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
    for (int i = 0; i < m; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Create thread " << i << std::endl;
        #endif
        int *arg = (int*)malloc(sizeof(int) * 2);
        arg[0] = table_id;
        arg[1] = i;
        args.push_back(arg);
        pthread_create(&threads[i], NULL, xlock_only_disjoint, arg);
    }

    for (int i = 0; i < m; i++) {
        pthread_join(threads[i], NULL);
    }

    for(auto i:args){
        free(i);
    }

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(ConcurrencyCtrl, XLockOnlyDisjointTestCheck) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    int table_id = open_table("XLockOnly.dat");

    int n = N;
    
    for (int i = 1; i <= n; i++) {
        std::string data = "12345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t old_val_size = 0;
        char buffer[MAX_VAL_SIZE] = "\0";
        int res = db_find(table_id, i, buffer, &old_val_size);
        EXPECT_EQ(res, 0);
        EXPECT_NE(old_val_size, 0);
        for (int j = 0; j < old_val_size; j++) {
            EXPECT_EQ(buffer[j], data[j]);
        }
    }

    EXPECT_EQ(shutdown_db(), 0);
}

void* xlock_only(void* arg) {
    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    int table_id = ((int*)arg)[0];
    int n = N;
    int err = 0;
    uint16_t val_size;
    std::string s[10] = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    for (int i = 1; i <= n; i++) {
        // char ret_val[112] = "\0";
        std::string data = s[trx_id%10] + "2345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t *old_val_size = (uint16_t*)malloc(sizeof(uint16_t));
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), old_val_size, trx_id);
        EXPECT_EQ(res, 0);
        EXPECT_NE(*old_val_size, 0);
        free(old_val_size);
        // res = db_find(table_id, i, ret_val, &old_val_size);
        // EXPECT_EQ(res, 0);

        // for (int j = 0; j < val_size; j++) {
        //     EXPECT_EQ(ret_val[j], data[j]);
        // }
    }

    trx_commit(trx_id);
    return NULL;
}


TEST(ConcurrencyCtrl, XLockOnlyTest) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("XLockOnly.dat") == 0)
    {
        std::cout << "[INFO] File 'XLockOnly.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("XLockOnly.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Population done, now testing XLockOnly Test" << std::endl;

    int m = 10;
    uint16_t val_size;
    pthread_t threads[m];
    std::vector<int*> args;
    // pthread_attr_t attr;
    // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
    for (int i = 0; i < m; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Create thread " << i << std::endl;
        #endif
        int *arg = (int*)malloc(sizeof(int));
        *arg = table_id;
        args.push_back(arg);
        pthread_create(&threads[i], NULL, xlock_only, arg);
    }

    for (int i = 0; i < m; i++) {
        pthread_join(threads[i], NULL);
    }

    for(auto i:args){
        free(i);
    }
    

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(ConcurrencyCtrl, XLockOnlyTestCheck) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);
    int table_id = open_table("XLockOnly.dat");

    int n = N;
    int last_trx = 0;
    uint16_t old_val_size = 0;
    char buffer[MAX_VAL_SIZE];
    int res = db_find(table_id, 1, buffer, &old_val_size);
    char x = buffer[0];
    for (int i = 1; i <= n; i++) {
        int res = db_find(table_id, i, buffer, &old_val_size);
        EXPECT_EQ(res, 0);
        EXPECT_EQ(buffer[0], x);
    }
    
    EXPECT_EQ(shutdown_db(), 0);
}

TEST(ConcurrencyCtrl, MixedLockTest) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("MixedLock.dat") == 0)
    {
        std::cout << "[INFO] File 'MixedLock.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("MixedLock.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Population done, now testing MixedLock Test" << std::endl;

    int m = 10;
    uint16_t val_size;
    pthread_t threads[m];
    // pthread_attr_t attr;
    // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
    for (int i = 0; i < m; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Create thread " << i << std::endl;
        #endif
        if(i &1){
            pthread_create(&threads[i], NULL, xlock_only, &table_id);
        }
        else{
            pthread_create(&threads[i], NULL, slock_only, &table_id);
        }
    }

    for (int i = 0; i < m; i++) {
        pthread_join(threads[i], NULL);
    }

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(ConcurrencyCtrl, MixedLockTestCheck) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);
    int table_id = open_table("MixedLock.dat");

    int n = N;
    int last_trx = 0;
    uint16_t old_val_size = 0;
    char buffer[MAX_VAL_SIZE];
    int res = db_find(table_id, 1, buffer, &old_val_size);
    char x = buffer[0];
    for (int i = 1; i <= n; i++) {
        int res = db_find(table_id, i, buffer, &old_val_size);
        EXPECT_EQ(res, 0);
        EXPECT_EQ(buffer[0], x);
    }
    
    EXPECT_EQ(shutdown_db(), 0);
}


void* deadlock_test(void* arg) {
    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    int table_id = ((int*)arg)[0];
    int n = N;
    int err = 0;
    uint16_t val_size;
    std::string s[10] = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    std::vector<int> v;
    for(int i = 1; i <= n; i++){
        v.push_back(i);
    }

    std::mt19937 gen(2020011776 + trx_id);
    std::shuffle(v.begin(), v.end(), gen);

    bool aborted = false;

    for (int i : v) {
        // char ret_val[112] = "\0";
        std::string data = s[trx_id%10] + "2345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t *old_val_size = (uint16_t*)malloc(sizeof(uint16_t));
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), old_val_size, trx_id);
        free(old_val_size);
        if(res){
            std::cout << "[DEBUG] ABORTED" << std::endl;
            aborted = true;
            break;
        }
    }
    if(!aborted) trx_commit(trx_id);
    std::cout << "[DEBUG] Thread " << trx_id << " done" << std::endl;
    return NULL;
}


TEST(ConcurrencyCtrl, DeadlockTest) {
    EXPECT_EQ(init_db(BUF_SIZE), 0);

    if (std::remove("deadlockTest.dat") == 0)
    {
        std::cout << "[INFO] File 'deadlockTest.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("deadlockTest.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Population done, now testing Deadlock Test" << std::endl;
    std::cout << "[WARN] This test does not guarentee correctness." << std::endl;

    int m = 10;
    uint16_t val_size;
    pthread_t threads[m];
    std::vector<int*> args;
    // pthread_attr_t attr;
    // pthread_attr_setstacksize(&attr, 1024 * 1024 * 1024);
    for (int i = 0; i < m; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Create thread " << i << std::endl;
        #endif
        int *arg = (int*)malloc(sizeof(int));
        *arg = table_id;
        args.push_back(arg);
        pthread_create(&threads[i], NULL, deadlock_test, arg);
    }

    for (int i = 0; i < m; i++) {
        pthread_join(threads[i], NULL);
    }

    for(auto i:args){
        free(i);
    }
    

    EXPECT_EQ(shutdown_db(), 0);
}