#include <stdio.h>
#include <stdlib.h>
#include <postgres.h>
#include <utils/elog.h>
#include "smp.h"

#define IMCS_SMP_CHECK(call) if ((call) != 0) imcs_error_handler(__FILE__, __LINE__, #call)

#ifdef _WIN32

#include <windows.h>

#define IMCS_MAX_SEM_VALUE 1000000

#ifndef TLS_OUT_OF_INDEXES
#define TLS_OUT_OF_INDEXES 0xffffffff
#endif

static void imcs_error_handler(char const* file, int line, char const* msg)
{
    elog(ERROR, "%s:%d %s error=%d", file, line, msg, GetLastError());
}


typedef struct imcs_win_tls_t 
{
    imcs_tls_t vtab;
    unsigned int key;
} imcs_win_tls_t;

static void* imcs_get_tls(imcs_tls_t* t)
{
    imcs_win_tls_t* tls = (imcs_win_tls_t*)t;
    return TlsGetValue(tls->key);
}

static void imcs_set_tls(imcs_tls_t* t, void* value)
{
    imcs_win_tls_t* tls = (imcs_win_tls_t*)t;
    TlsSetValue(tls->key, value);
}

static void imcs_destroy_tls(imcs_tls_t* t)
{
    imcs_win_tls_t* tls = (imcs_win_tls_t*)t;
    TlsFree(tls->key);
    free(tls);
}
                            

imcs_tls_t* imcs_create_tls(void)
{
    imcs_win_tls_t* tls = (imcs_win_tls_t*)malloc(sizeof(imcs_win_tls_t));
    tls->vtab.get = imcs_get_tls;
    tls->vtab.set = imcs_set_tls;
    tls->vtab.destroy = imcs_destroy_tls;
    tls->key = TlsAlloc();
    if (tls->key == TLS_OUT_OF_INDEXES) { 
        free(tls);
        return NULL;
    }
    return &tls->vtab;
}

typedef struct imcs_win_thread_t { 
    imcs_thread_t vtab;
    HANDLE handle;
} imcs_win_thread_t;

static void imcs_join_thread(imcs_thread_t* t)
{
    imcs_win_thread_t* thread = (imcs_win_thread_t*)t;
    IMCS_SMP_CHECK(WaitForSingleObject(thread->handle, INFINITE) == WAIT_OBJECT_0);
    IMCS_SMP_CHECK(CloseHandle(thread->handle));
    free(thread);
}

imcs_thread_t* imcs_create_thread(imcs_thread_proc_t proc, void* arg)
{
    DWORD threadid;
    HANDLE h = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE) proc, arg, 0, &threadid);
    if (h == NULL) { 
        return NULL;
    } else { 
        imcs_win_thread_t* thread = (imcs_win_thread_t*)malloc(sizeof(imcs_win_thread_t));
        thread->vtab.join = imcs_join_thread;
        thread->handle = h;
        return &thread->vtab;
    }
}

int imcs_get_number_of_cpus(void)
{
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return sysinfo.dwNumberOfProcessors;
}

typedef struct 
{ 
    imcs_mutex_t vtab;
    CRITICAL_SECTION cs;
} imcs_win_mutex_t;

static void imcs_lock_mutex(imcs_mutex_t* m)
{
    imcs_win_mutex_t* mutex = (imcs_win_mutex_t*)m;
    EnterCriticalSection(&mutex->cs);
}

static void imcs_unlock_mutex(imcs_mutex_t* m)
{
    imcs_win_mutex_t* mutex = (imcs_win_mutex_t*)m;
    LeaveCriticalSection(&mutex->cs);
}

static void imcs_destroy_mutex(imcs_mutex_t* m)
{
    imcs_win_mutex_t* mutex = (imcs_win_mutex_t*)m;
    DeleteCriticalSection(&mutex->cs);
    free(mutex);
}


imcs_mutex_t* imcs_create_mutex()
{
    imcs_win_mutex_t* mutex = (imcs_win_mutex_t*)malloc(sizeof(imcs_win_mutex_t));
    InitializeCriticalSection(&mutex->cs);
    mutex->vtab.lock = imcs_lock_mutex;
    mutex->vtab.unlock = imcs_unlock_mutex;
    mutex->vtab.destroy = imcs_destroy_mutex;
    return &mutex->vtab;
}


typedef struct 
{ 
    imcs_semaphore_t vtab;
    HANDLE handle;
} imcs_win_semaphore_t;

static imcs_bool imcs_semaphore_wait(imcs_semaphore_t* s, imcs_mutex_t* mutex, int n, unsigned timeout)
{
    imcs_win_semaphore_t* sem = (imcs_win_semaphore_t*)s;
    int rc;
    mutex->unlock(mutex);
    while (--n >= 0) { 
        rc = WaitForSingleObject(sem->handle, timeout);
        if (rc == WAIT_TIMEOUT) { 
            mutex->lock(mutex);
            return 0;
        }
        IMCS_SMP_CHECK(rc == WAIT_OBJECT_0);
    }
    mutex->lock(mutex);
    return 1;
}
    
static void imcs_semaphore_signal(imcs_semaphore_t* s, int inc)
{
    imcs_win_semaphore_t* sem = (imcs_win_semaphore_t*)s;    
    IMCS_SMP_CHECK(ReleaseSemaphore(sem->handle, inc, NULL));
}

static void imcs_semaphore_destroy(imcs_semaphore_t* s)
{
    imcs_win_semaphore_t* sem = (imcs_win_semaphore_t*)s;    
    CloseHandle(sem->handle);
    free(sem);
}

imcs_semaphore_t* imcs_create_semaphore(int value)
{
    imcs_win_semaphore_t* sem;
    HANDLE s = CreateSemaphore(NULL, value, IMCS_MAX_SEM_VALUE, NULL);
    IMCS_SMP_CHECK(s != NULL);
    sem = (imcs_win_semaphore_t*)malloc(sizeof (imcs_win_semaphore_t));
    sem->vtab.wait = imcs_semaphore_wait;
    sem->vtab.signal = imcs_semaphore_signal;
    sem->vtab.destroy = imcs_semaphore_destroy;
    sem->handle = s;
    return &sem->vtab;        
}

imcs_process_t imcs_get_pid(void) 
{ 
    return (imcs_process_t)(imcs_size_t)GetCurrentProcessId();
}

imcs_bool imcs_is_process_alive(imcs_process_t proc)
{
    HANDLE h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, (DWORD)(imcs_size_t)proc);
    if (h == NULL) { 
        return 0;
    }
    CloseHandle(h);
    return 1;
}

#else

#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/time.h>
#include <pthread.h>

static void imcs_error_handler(char const* file, int line, char const* msg)
{
    elog(ERROR, "%s:%d %s errno=%d", file, line, msg, errno);
}

/* Posix threads */
typedef struct imcs_posix_tls_t 
{
    imcs_tls_t vtab;
    pthread_key_t key;
} imcs_posix_tls_t;

static void* imcs_get_tls(imcs_tls_t* t)
{
    imcs_posix_tls_t* tls = (imcs_posix_tls_t*)t;
    return pthread_getspecific(tls->key);
}

static void imcs_set_tls(imcs_tls_t* t, void* value)
{
    imcs_posix_tls_t* tls = (imcs_posix_tls_t*)t;
    IMCS_SMP_CHECK(pthread_setspecific(tls->key, value));
}

static void imcs_destroy_tls(imcs_tls_t* t)
{
    imcs_posix_tls_t* tls = (imcs_posix_tls_t*)t;
    pthread_key_delete(tls->key);
    free(tls);
}
                            

imcs_tls_t* imcs_create_tls(void)
{
    imcs_posix_tls_t* tls = (imcs_posix_tls_t*)malloc(sizeof(imcs_posix_tls_t));
    tls->vtab.get = imcs_get_tls;
    tls->vtab.set = imcs_set_tls;
    tls->vtab.destroy = imcs_destroy_tls;
    IMCS_SMP_CHECK(pthread_key_create(&tls->key, NULL)); 
    return &tls->vtab;
}

typedef struct imcs_posix_thread_t { 
    imcs_thread_t vtab;
    pthread_t thread;
} imcs_posix_thread_t;

static void imcs_join_thread(imcs_thread_t* t)
{
    IMCS_SMP_CHECK(pthread_join(((imcs_posix_thread_t*)t)->thread, NULL));
    free(t);
}

imcs_thread_t* imcs_create_thread(imcs_thread_proc_t proc, void* arg)
{
    imcs_posix_thread_t* t = (imcs_posix_thread_t*)malloc(sizeof(imcs_posix_thread_t));
    IMCS_SMP_CHECK(pthread_create(&t->thread, NULL, (void*(*)(void*))proc, arg));
    t->vtab.join = imcs_join_thread;
    return &t->vtab;
}

#if defined(_SC_NPROCESSORS_ONLN) 
int imcs_get_number_of_cpus(void)
{
    return sysconf(_SC_NPROCESSORS_ONLN); 
}
#elif defined(__linux__)
#include <linux/smp.h>
int imcs_get_number_of_cpus(void)
{
    return smp_num_cpus; 
}
#elif defined(__FreeBSD__) || defined(__bsdi__) || defined(__OpenBSD__) || defined(__NetBSD__)
#if defined(__bsdi__) || defined(__OpenBSD__)
#include <sys/param.h>
#endif
#include <sys/sysctl.h>
int imcs_get_number_of_cpus(void)
{
    int mib[2],ncpus=0;
    size_t len=sizeof(ncpus);
    mib[0]= CTL_HW;
    mib[1]= HW_NCPU;
    sysctl(mib,2,&ncpus,&len,NULL,0);
    return ncpus; 
}
#else
int imcs_get_number_of_cpus(void)
{
    return 1;
}
#endif

typedef struct 
{ 
    imcs_mutex_t vtab;
    pthread_mutex_t cs;
} imcs_posix_mutex_t;

static void imcs_lock_mutex(imcs_mutex_t* m)
{
    imcs_posix_mutex_t* mutex = (imcs_posix_mutex_t*)m;
    IMCS_SMP_CHECK(pthread_mutex_lock(&mutex->cs));
}

static void imcs_unlock_mutex(imcs_mutex_t* m)
{
    imcs_posix_mutex_t* mutex = (imcs_posix_mutex_t*)m;
    IMCS_SMP_CHECK(pthread_mutex_unlock(&mutex->cs));
}

static void imcs_destroy_mutex(imcs_mutex_t* m)
{
    imcs_posix_mutex_t* mutex = (imcs_posix_mutex_t*)m;
    pthread_mutex_destroy(&mutex->cs);
    free(mutex);
}


imcs_mutex_t* imcs_create_mutex(void)
{
    imcs_posix_mutex_t* mutex = (imcs_posix_mutex_t*)malloc(sizeof(imcs_posix_mutex_t));
    IMCS_SMP_CHECK(pthread_mutex_init(&mutex->cs, NULL)); 
    mutex->vtab.lock = imcs_lock_mutex;
    mutex->vtab.unlock = imcs_unlock_mutex;
    mutex->vtab.destroy = imcs_destroy_mutex;
    return &mutex->vtab;
}


typedef struct 
{ 
    imcs_semaphore_t vtab;
    pthread_cond_t cond;
    int count;
} imcs_posix_semaphore_t;

static imcs_bool imcs_semaphore_wait(imcs_semaphore_t* s, imcs_mutex_t* mutex, int n, unsigned timeout)
{
    imcs_posix_semaphore_t* sem = (imcs_posix_semaphore_t*)s;
    struct timespec abs_ts; 
    int rc;

    if (timeout != IMCS_TM_INFINITE) {            
#ifdef PTHREAD_GET_EXPIRATION_NP
        struct timespec rel_ts; 
        rel_ts.tv_sec = timeout/1000; 
        rel_ts.tv_nsec = timeout%1000*1000000;
        pthread_get_expiration_np(&rel_ts, &abs_ts);
#else
        struct timeval cur_tv;
        gettimeofday(&cur_tv, NULL);
        abs_ts.tv_sec = cur_tv.tv_sec + timeout/1000; 
        abs_ts.tv_nsec = cur_tv.tv_usec*1000 + timeout%1000*1000000;
        if (abs_ts.tv_nsec > 1000000000) { 
            abs_ts.tv_nsec -= 1000000000;
            abs_ts.tv_sec += 1;
        }
#endif
    }
    while (sem->count < n)
    {
        rc = timeout != IMCS_TM_INFINITE
            ? pthread_cond_timedwait(&sem->cond, &((imcs_posix_mutex_t*)mutex)->cs, &abs_ts)
            : pthread_cond_wait(&sem->cond, &((imcs_posix_mutex_t*)mutex)->cs);
        if (rc == ETIMEDOUT) { 
            return 0;
        }
        IMCS_SMP_CHECK(rc);
    }
    sem->count -= n;
    return 1;
}
    
static void imcs_semaphore_signal(imcs_semaphore_t* s, int inc)
{
    imcs_posix_semaphore_t* sem = (imcs_posix_semaphore_t*)s;
    sem->count += inc;
    if (inc > 1)
    {
        IMCS_SMP_CHECK(pthread_cond_broadcast(&sem->cond));
    }
    else if (inc == 1)
    {
        IMCS_SMP_CHECK(pthread_cond_signal(&sem->cond));
    }
}

static void imcs_semaphore_destroy(imcs_semaphore_t* s)
{
    imcs_posix_semaphore_t* sem = (imcs_posix_semaphore_t*)s;
    pthread_cond_destroy(&sem->cond);
    free(sem);
}

imcs_semaphore_t* imcs_create_semaphore(int value)
{
    imcs_posix_semaphore_t* sem = (imcs_posix_semaphore_t*)malloc(sizeof(imcs_posix_semaphore_t));
    IMCS_SMP_CHECK(pthread_cond_init(&sem->cond, NULL)); 
    sem->vtab.wait = imcs_semaphore_wait;
    sem->vtab.signal = imcs_semaphore_signal;
    sem->vtab.destroy = imcs_semaphore_destroy;
    sem->count = value;
    return &sem->vtab;        
}

imcs_process_t imcs_get_pid(void) 
{ 
    return (imcs_process_t)(size_t)getpid();
}

imcs_bool imcs_is_process_alive(imcs_process_t proc)
{
    return kill((int)(size_t)proc, 0) == 0;
}

#endif
