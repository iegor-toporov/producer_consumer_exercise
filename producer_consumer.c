/*  

EXERCISE 4 - IEGOR TOPOROV

Exercise text : 
   
Producer-(single) consumer program with dynamic message rate adjustment. The consumer
shall consume messages at a given rate, that is, with a given delay simulating the consumed
message usage. An actor (task or process) separate from producer and consumer shall
periodically check the message queue length and if the length is below a given threshold, it
will increase the production rate. Otherwise (i.e. the message length is above the given
threshold), it will decrease the production rate.

*/

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

/* Constants */
#define MAX_QUEUE_SIZE 20
#define THRESHOLD 10
#define RUN_SECONDS 30

/* Shared queue */

int queue[MAX_QUEUE_SIZE];
int head = 0, tail = 0, count = 0;

pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER; // Binary lock/unlock. Thread is owner.
pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;      // Smart way to not waste CPU. Condition variable.
pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;

void queue_put(int msg) {
    pthread_mutex_lock(&queue_mutex);
    while (count == MAX_QUEUE_SIZE)          // if queue is full (count holds the length of the queue) -> wait for it to be not full
        pthread_cond_wait(&not_full, &queue_mutex);
    queue[tail] = msg;                       // append message to tail
    tail  = (tail + 1) % MAX_QUEUE_SIZE;
    count++;
    pthread_cond_signal(&not_empty);         // wake up consumer
    pthread_mutex_unlock(&queue_mutex);
}

int queue_get(void) {
    pthread_mutex_lock(&queue_mutex);
    while (count == 0)                       // if queue is empty: wait
        pthread_cond_wait(&not_empty, &queue_mutex);
    int msg = queue[head];
    head  = (head + 1) % MAX_QUEUE_SIZE;
    count--;
    pthread_cond_signal(&not_full);          // wake up producer
    pthread_mutex_unlock(&queue_mutex);
    return msg;
}

int queue_size(void) {
    pthread_mutex_lock(&queue_mutex);
    int sz = count;
    pthread_mutex_unlock(&queue_mutex);
    return sz;
}

/*  Shared production rate  */

int prod_delay_us = 500000;                  // 500 ms initial delay
pthread_mutex_t rate_mutex = PTHREAD_MUTEX_INITIALIZER;

/*  Thread functions  */

void *producer(void *arg) {
    int msg_id = 1;
    while (1) { 
        pthread_mutex_lock(&rate_mutex);     // access rate_mutex to read delay
        int delay = prod_delay_us;
        pthread_mutex_unlock(&rate_mutex);

        queue_put(msg_id);
        printf("[Producer]  P msg %-4d  (delay: %d ms)\n", msg_id, delay / 1000);
        msg_id++;
        usleep(delay);
    }
    return NULL;
}

void *consumer(void *arg) {
    const int consume_delay_us = 800000;      // fixed 800 ms for consumer delay
    while (1) {
        int msg = queue_get();                // consume
        printf("[Consumer]  C msg %d\n", msg);
        usleep(consume_delay_us);             // sleep for delay
    }
    return NULL;
}

void *rate_adjuster(void *arg) {
    const int check_period_us = 2000000;     // check every 2 s
    while (1) {
        usleep(check_period_us);
        int sz = queue_size();
        printf("[Adjuster]  Actual queue size = %d  (threshold = %d)\n", sz, THRESHOLD);

        pthread_mutex_lock(&rate_mutex);
        if (sz < THRESHOLD) {
            /* queue is emptying fast: produce faster (smaller delay) */
            prod_delay_us -= 300000;
            if (prod_delay_us < 100000)
                prod_delay_us = 50000;             // clamp to minimum
            printf("[Adjuster]  queue LOW  -> decrease delay  (new delay: %d ms)\n",
                   prod_delay_us / 1000);
        } else {
            // queue is filling up: produce slower (larger delay)
            prod_delay_us += 300000;
            if (prod_delay_us > 2000000)
                prod_delay_us = 2000000;            // clamp to maximum
            printf("[Adjuster]  queue HIGH -> increase delay  (new delay: %d ms)\n",
                   prod_delay_us / 1000);
        }
        pthread_mutex_unlock(&rate_mutex);
    }
    return NULL;
}

/* Main  */

int main(void) {
    pthread_t t_producer, t_consumer, t_adjuster;

    pthread_create(&t_producer, NULL, producer, NULL);
                //  ^ ID        ^ attr ^ func   ^argument to func
    pthread_create(&t_consumer, NULL, consumer, NULL);
    pthread_create(&t_adjuster, NULL, rate_adjuster, NULL);

    sleep(RUN_SECONDS);
    printf("\n[Main] %d seconds elapsed, job done.\n", RUN_SECONDS);
    return 0;
}