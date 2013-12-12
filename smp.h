#ifndef __SMP_H__
#define __SMP_H__

typedef void* imcs_process_t;
typedef int imcs_bool;

#define IMCS_TM_INFINITE ((unsigned)-1)

typedef void (*imcs_thread_proc_t)(void* arg);

typedef struct imcs_tls_t { 
    void* (*get)(struct imcs_tls_t* tls);
    void (*set)(struct imcs_tls_t* tls, void* value);
    void (*destroy)(struct imcs_tls_t* tls);
} imcs_tls_t;

imcs_tls_t* imcs_create_tls(void);


typedef struct imcs_thread_t { 
    void (*join)(struct imcs_thread_t* id);
} imcs_thread_t;

extern imcs_thread_t* imcs_create_thread(imcs_thread_proc_t proc, void* arg);

typedef struct imcs_mutex_t { 
    void (*lock)(struct imcs_mutex_t* m);
    void (*unlock)(struct imcs_mutex_t* m);
    void (*destroy)(struct imcs_mutex_t* sem);
} imcs_mutex_t;

extern imcs_mutex_t* imcs_create_mutex(void);    

typedef struct imcs_semaphore_t { 
    imcs_bool (*wait)(struct imcs_semaphore_t* sem, imcs_mutex_t* mutex, int n, unsigned timeout);
    void (*signal)(struct imcs_semaphore_t* sem, int n);
    void (*destroy)(struct imcs_semaphore_t* sem);
} imcs_semaphore_t;

extern imcs_semaphore_t* imcs_create_semaphore(int value);

extern int imcs_get_number_of_cpus(void);
extern imcs_process_t imcs_get_pid(void);
extern imcs_bool imcs_is_process_alive(imcs_process_t proc);


typedef void (*imcs_job_t)(int thread_id, int n_threads, void* arg);
typedef void (*imcs_job_callback_t)(void* arg, void* result);

typedef struct imcs_thread_pool_t { 
    int  (*get_number_of_threads)(struct imcs_thread_pool_t* pool);
    void (*execute)(struct imcs_thread_pool_t* pool, imcs_job_t job, void* arg);
    void (*merge)(struct imcs_thread_pool_t* pool, imcs_job_callback_t callback, void* result);
    void (*destroy)(struct imcs_thread_pool_t* pool);
} imcs_thread_pool_t;

extern imcs_thread_pool_t* imcs_create_thread_pool(int n_threads); /* 0 - choose number of threads automaticallym based on number of cores */
 
#endif
