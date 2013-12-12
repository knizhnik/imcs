#include <stdlib.h>
#include "smp.h"

typedef struct imcs_thread_pool_impl_t 
{
    imcs_thread_pool_t vtab;
    int n_workers;
    imcs_thread_t** workers;
    void* arg;
    imcs_job_t job;
    int work_id;
    imcs_mutex_t* mutex;
    imcs_mutex_t* sync;
    imcs_semaphore_t* start;
    imcs_semaphore_t* finish;
    int stop;
} imcs_thread_pool_impl_t;


static void imcs_thread_pool_worker(imcs_thread_pool_impl_t* pool)
{
    int work_id;
    pool->sync->lock(pool->sync);
    while (1) { 
        pool->start->wait(pool->start, pool->sync, 1, IMCS_TM_INFINITE);
        if (pool->stop) {
            pool->finish->signal(pool->finish, 1); 
            pool->sync->unlock(pool->sync);
            break;
        }
        work_id = pool->work_id++;
        pool->sync->unlock(pool->sync);
        pool->job(work_id, pool->n_workers, pool->arg);    
        pool->sync->lock(pool->sync);
        pool->finish->signal(pool->finish, 1);
    }
} 
    
static void imcs_thread_pool_wait(imcs_thread_pool_impl_t* pool)
{
    pool->sync->lock(pool->sync);
    pool->work_id = 0;
    pool->start->signal(pool->start, pool->n_workers);
    pool->finish->wait(pool->finish, pool->sync, pool->n_workers, IMCS_TM_INFINITE);
    pool->sync->unlock(pool->sync);

}
int counters[4] = {0,0,0,0};
static void imcs_thread_pool_execute(struct imcs_thread_pool_t* self, imcs_job_t job, void* arg)
{
    imcs_thread_pool_impl_t* pool = (imcs_thread_pool_impl_t*)self;
    pool->mutex->lock(pool->mutex);
    pool->job = job;
    pool->arg = arg;
    imcs_thread_pool_wait(pool);
    pool->mutex->unlock(pool->mutex);
}

static void imcs_thread_pool_destroy(struct imcs_thread_pool_t* self)
{
    int i;
    imcs_thread_pool_impl_t* pool = (imcs_thread_pool_impl_t*)self;
    pool->stop = 1;
    imcs_thread_pool_wait(pool);
    for (i = 0; i < pool->n_workers; i++) { 
        pool->workers[i]->join(pool->workers[i]);
    }    
    pool->sync->destroy(pool->sync);
    pool->mutex->destroy(pool->sync);
    pool->start->destroy(pool->start);
    pool->finish->destroy(pool->finish);
    free(pool->workers);
    free(pool);
}

static int imcs_thread_pool_get_number_of_threads(struct imcs_thread_pool_t* self)
{
    imcs_thread_pool_impl_t* pool = (imcs_thread_pool_impl_t*)self;
    return pool->n_workers;
}

static void imcs_thread_pool_merge(struct imcs_thread_pool_t* self, imcs_job_callback_t callback, void* result)
{
    imcs_thread_pool_impl_t* pool = (imcs_thread_pool_impl_t*)self;
    pool->sync->lock(pool->sync);
    callback(pool->arg, result);
    pool->sync->unlock(pool->sync);
}

struct imcs_thread_pool_t* imcs_create_thread_pool(int n_threads)
{
    int i;
    imcs_thread_pool_impl_t* pool = (imcs_thread_pool_impl_t*)malloc(sizeof(imcs_thread_pool_impl_t));
    if (n_threads == 0) { 
        n_threads = imcs_get_number_of_cpus();
    }
    pool->workers = (imcs_thread_t**)malloc(sizeof(imcs_thread_t*)*n_threads);
    pool->sync = imcs_create_mutex();
    pool->mutex = imcs_create_mutex();
    pool->start = imcs_create_semaphore(0);
    pool->finish = imcs_create_semaphore(0);
    pool->n_workers = n_threads;
    pool->vtab.execute = imcs_thread_pool_execute;
    pool->vtab.merge = imcs_thread_pool_merge;
    pool->vtab.destroy = imcs_thread_pool_destroy;
    pool->vtab.get_number_of_threads = imcs_thread_pool_get_number_of_threads;

    pool->stop = 0;
    for (i = 0; i < n_threads; i++) { 
        pool->workers[i] = imcs_create_thread((imcs_thread_proc_t)imcs_thread_pool_worker, pool);
    }
    return (struct imcs_thread_pool_t*)pool;
}
