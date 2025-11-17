#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include "acquisitionManager.h"
#include "msg.h"
#include "iSensor.h"
#include "multitaskingAccumulator.h"
#include "iAcquisitionManager.h"
#include "debug.h"


//producer count storage
volatile unsigned int produceCount = 0;


pthread_t producers[4];

static void *produce(void *params);

/**
* Semaphores and Mutex
*/
//TODO
sem_t sem_read, sem_write;
pthread_mutex_t mut_w_writable_idx, mut_w_readable_idx, mut_produceCount;


#define BUFFER_SIZE 10

MSG_BLOCK data[BUFFER_SIZE];
unsigned int readables[BUFFER_SIZE];
unsigned int writables[BUFFER_SIZE];
unsigned int next_w_readable_idx, next_w_writable_idx, next_r_readable_idx, next_r_writable_idx;


/*
* Creates the synchronization elements.
* @return ERROR_SUCCESS if the init is ok, ERROR_INIT otherwise
*/
static unsigned int createSynchronizationObjects(void);

/*
* Increments the produce count.
*/
static void incrementProducedCount(void);

static unsigned int createSynchronizationObjects(void)
{

	//TODO
	if(pthread_mutex_init(&mut_w_readable_idx, NULL)) 
	{
		return ERROR_INIT;
	}
	if(pthread_mutex_init(&mut_w_writable_idx, NULL)) 
	{
		return ERROR_INIT;
	}
	if(pthread_mutex_init(&mut_produceCount, NULL)) 
	{
		return ERROR_INIT;
	}
	if(sem_init(&sem_read, NULL, 0)) 
	{
		return ERROR_INIT;
	}
	if(sem_init(&sem_write, NULL, BUFFER_SIZE)) 
	{
		return ERROR_INIT;
	}
	unsigned int i=0;
	for (unsigned int i = 0; i < BUFFER_SIZE; i++)
	{
		writables[i] = i;
		readables[i] = i;
	}
	
	next_w_readable_idx=next_w_writable_idx=0;
	next_r_readable_idx=next_r_writable_idx=0;
	
	D(int r_count);
	D(sem_getvalue(&sem_read, &r_count));
	D(printf("[acquisitionManager]Read semaphore initialized to : %d\n", r_count));
	D(int w_count);
	D(sem_getvalue(&sem_write, &w_count));
	D(printf("[acquisitionManager]Write semaphore initialized to : %d\n", w_count));

	printf("[acquisitionManager]Semaphore created\n");
	return ERROR_SUCCESS;
}

static void incrementProducedCount(void)
{
	//TODO
	pthread_mutex_lock(&mut_produceCount);
	++produceCount;
	pthread_mutex_unlock(&mut_produceCount);
}

unsigned int getProducedCount(void)
{
	unsigned int p = 0;
	//TODO - DONE
	pthread_mutex_lock(&mut_produceCount);
	p = produceCount;
	pthread_mutex_unlock(&mut_produceCount);
	return produceCount;
}
static void write_data(MSG_BLOCK * msg)
{
	sem_wait(&sem_write);

	pthread_mutex_lock(&mut_w_writable_idx);
	unsigned int i = next_w_readable_idx;
	next_w_writable_idx = (next_w_writable_idx + 1) % BUFFER_SIZE; 
	pthread_mutex_unlock(&mut_w_writable_idx);
	data[writables[i]] = *msg;
	pthread_mutex_lock(&mut_w_readable_idx);
	unsigned int j = next_w_readable_idx;
	next_w_readable_idx = (next_w_readable_idx + 1) % BUFFER_SIZE; 
	pthread_mutex_unlock(&mut_w_readable_idx);
	readables[j] = writables[i];

	sem_post(&sem_read);
}

MSG_BLOCK getMessage(void){
	//TODO
	sem_wait(&sem_read);
	unsigned int i = next_r_readable_idx;
	next_r_readable_idx = (next_r_readable_idx + 1) % BUFFER_SIZE;
	MSG_BLOCK msg = data[readables[i]];
	writables[next_r_writable_idx] = readables[i];
	next_r_writable_idx = (next_r_writable_idx + 1) % BUFFER_SIZE;
	sem_post(&sem_write);
	return msg;
}

//TODO create accessors to limit semaphore and mutex usage outside of this C module.

unsigned int acquisitionManagerInit(void)
{
	unsigned int i;
	printf("[acquisitionManager]Synchronization initialization in progress...\n");
	fflush( stdout );
	if (createSynchronizationObjects() == ERROR_INIT)
		return ERROR_INIT;
	
	printf("[acquisitionManager]Synchronization initialization done.\n");

	for (i = 0; i < PRODUCER_COUNT; i++)
	{
		//TODO
		pthread_create(producers + i, NULL, produce, NULL);
	}

	return ERROR_SUCCESS;
}

void acquisitionManagerJoin(void)
{
	unsigned int i;
	for (i = 0; i < PRODUCER_COUNT; i++)
	{
		//TODO - DONE
		pthread_join(producers[i], NULL);
	}

	//TODO
	pthread_mutex_destroy(&mut_produceCount);
	pthread_mutex_destroy(&mut_w_readable_idx);
	pthread_mutex_destroy(&mut_w_writable_idx);

	sem_destroy(&sem_read);
	sem_destroy(&sem_write);

	printf("[acquisitionManager]Semaphore cleaned\n");
}

void *produce(void* params)
{
	D(printf("[acquisitionManager]Producer created with id %d\n", gettid()));
	unsigned int i = 0;
	while (i < PRODUCER_LOOP_LIMIT)
	{
		i++;
		sleep(PRODUCER_SLEEP_TIME+(rand() % 5));
		//TODO
		MSG_BLOCK msg;
		getInput((unsigned int)params, &msg);
		if(messageCheck(&msg))
		{
			write_data(&msg);
			incrementProducedCount();
		}
	}
	printf("[acquisitionManager] %d termination\n", gettid());
	//TODO
	return NULL;
}