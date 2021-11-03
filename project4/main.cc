#include <lock_table.h>

#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

#define TRANSFER_THREAD_NUMBER	(8)
#define SCAN_THREAD_NUMBER		(1)

#define TRANSFER_COUNT			(1000000)
#define SCAN_COUNT				(1000000)

#define TABLE_NUMBER			(3)
#define RECORD_NUMBER			(5)
#define INITIAL_MONEY			(100000)
#define MAX_MONEY_TRANSFERRED	(100)
#define SUM_MONEY				(TABLE_NUMBER * RECORD_NUMBER * INITIAL_MONEY)

/* This is shared data pretected by your lock table. */
int accounts[TABLE_NUMBER][RECORD_NUMBER];

/*
 * This thread repeatedly transfers some money between accounts randomly.
 */
void*
transfer_thread_func(void* arg)
{
	lock_t*			source_lock;
	lock_t*			destination_lock;
	int				source_table_id;
	int				source_record_id;
	int				destination_table_id;
	int				destination_record_id;
	int				money_transferred;

	for (int i = 0; i < TRANSFER_COUNT; i++) {
		/* Decide the source account and destination account for transferring. */
		source_table_id = rand() % TABLE_NUMBER;
		source_record_id = rand() % RECORD_NUMBER;
		destination_table_id = rand() % TABLE_NUMBER;
		destination_record_id = rand() % RECORD_NUMBER;

		if ((source_table_id > destination_table_id) ||
				(source_table_id == destination_table_id &&
				 source_record_id >= destination_record_id)) {
			/* Descending order may invoke deadlock conditions, so avoid it. */
			continue;
		}
		
		/* Decide the amount of money transferred. */
		money_transferred = rand() % MAX_MONEY_TRANSFERRED;
		money_transferred = rand() % 2 == 0 ?
			(-1) * money_transferred : money_transferred;
		
		/* Acquire lock!! */
		source_lock = lock_acquire(source_table_id, source_record_id);

		/* withdraw */
		accounts[source_table_id][source_record_id] -= money_transferred;

		/* Acquire lock!! */
		destination_lock =
			lock_acquire(destination_table_id, destination_record_id);

		/* deposit */
		accounts[destination_table_id][destination_record_id]
			+= money_transferred;

		/* Release lock!! */
		lock_release(destination_lock);
		lock_release(source_lock);
	}

	printf("Transfer thread is done.\n");

	return NULL;
}

/*
 * This thread repeatedly check the summation of all accounts.
 * Because the locking strategy is 2PL (2 Phase Locking), the summation must
 * always be consistent.
 */
void*
scan_thread_func(void* arg)
{
	int				sum_money;
	lock_t*			lock_array[TABLE_NUMBER][RECORD_NUMBER];

	for (int i = 0; i < SCAN_COUNT; i++) {
		sum_money = 0;

		/* Iterate all accounts and summate the amount of money. */
		for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
			for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
				/* Acquire lock!! */
				lock_array[table_id][record_id] =
					lock_acquire(table_id, record_id);

				/* Summation. */
				sum_money += accounts[table_id][record_id];
			}
		}

		for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
			for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
				/* Release lock!! */
				lock_release(lock_array[table_id][record_id]);
			}
		}

		/* Check consistency. */
		if (sum_money != SUM_MONEY) {
			printf("Inconsistent state is detected!!!!!\n");
			printf("sum_money : %d\n", sum_money);
			printf("SUM_MONEY : %d\n", SUM_MONEY);
			return NULL;
		}
	}

	printf("Scan thread is done.\n");

	return NULL;
}

int main()
{
	pthread_t	transfer_threads[TRANSFER_THREAD_NUMBER];
	pthread_t	scan_threads[SCAN_THREAD_NUMBER];

	srand(time(NULL));

	/* Initialize accounts. */
	for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
		for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
			accounts[table_id][record_id] = INITIAL_MONEY;
		}
	}

	/* Initialize your lock table. */
	init_lock_table();

	/* thread create */
	for (int i = 0; i < TRANSFER_THREAD_NUMBER; i++) {
		pthread_create(&transfer_threads[i], 0, transfer_thread_func, NULL);
	}
	for (int i = 0; i < SCAN_THREAD_NUMBER; i++) {
		pthread_create(&scan_threads[i], 0, scan_thread_func, NULL);
	}

	/* thread join */
	for (int i = 0; i < TRANSFER_THREAD_NUMBER; i++) {
		pthread_join(transfer_threads[i], NULL);
	}
	for (int i = 0; i < SCAN_THREAD_NUMBER; i++) {
		pthread_join(scan_threads[i], NULL);
	}

	return 0;
}

