#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h> 
#include <unistd.h>
#include <pthread.h>
#include "messageAdder.h"
#include "msg.h"
#include "iMessageAdder.h"
#include "multitaskingAccumulator.h"
#include "iAcquisitionManager.h"
#include "debug.h"

//consumer thread
pthread_t consumer;
//Message computed
volatile MSG_BLOCK out;
//Consumer count storage
volatile unsigned int consumeCount = 0;

pthread_mutex_t mut_out;

/**
 * Increments the consume count.
 */
static void incrementConsumeCount(void)
{
	consumeCount++;
}

/**
 * Consumer entry point.
 */
static void *sum( void *parameters );


MSG_BLOCK getCurrentSum(){
	//TODO
	pthread_mutex_lock(&mut_out);
	MSG_BLOCK msg = out;
	pthread_mutex_unlock(&mut_out);
	return msg;
}

unsigned int getConsumedCount(){
	//TODO
	return consumeCount;
}


void messageAdderInit(void){
	out.checksum = 0;
	for (size_t i = 0; i < DATA_SIZE; i++)
	{
		out.mData[i] = 0;
	}
	//TODO
	pthread_create(&consumer, NULL, sum, NULL);
	pthread_mutex_init(&mut_out, NULL);
}

void messageAdderJoin(void){
	//TODO
	pthread_join(consumer, NULL);
}

static void *sum( void *parameters )
{
	D(printf("[messageAdder]Thread created for sum with id %d\n", gettid()));
	unsigned int i = 0;
	while(i<ADDER_LOOP_LIMIT){
		i++;
		sleep(ADDER_SLEEP_TIME);
		//TODO
		MSG_BLOCK new_msg;
		pthread_mutex_lock(&mut_out);
		out = getMessage();
		pthread_mutex_unlock(&mut_out);
		incrementConsumeCount();

		unsigned int j = 4;
		while (--j)
		{
			new_msg = getMessage();
			pthread_mutex_lock(&mut_out);
			messageAdd(&out, &new_msg);
			pthread_mutex_unlock(&mut_out);
			incrementConsumeCount();
			
		}
		
	}
	printf("[messageAdder] %d termination\n", gettid());
	//TODO
	pthread_exit(NULL);
}


