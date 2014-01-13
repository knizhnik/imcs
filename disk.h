/*
 * Implementation of disk layer for IMCS
 */
#ifndef __DISK_H__
#define __DISK_H__

#include "imcs.h"
#include <storage/spin.h>

/* 
 * IMCS implements two-level LRU using L2-list.
 * Head of the list corresponds to most recently used internal (not leaf) pages, tail - least recently leaf used.
 */
typedef struct imcs_cache_item_t_
{
    uint64 offs;
    int    collision; /* hash table collision chain */
    int    next; /* L2-list to implement LRU */
    int    prev;
    int    dirty_index; /* for dirty page index of page in dirty_pages + 1, 0 otherwise  */
    int    access_count; /* page access counter */
    volatile bool is_busy; /* page is currently loaded */
} imcs_cache_item_t;

typedef struct 
{
    imcs_cache_item_t* items; /* imcs_cache_item_t[imcs_cache_size+1], first item is used as head of LRU list */
    char*   data; /* char[imcs_cache_size*imcs_page_size] */
    int*    dirty_pages;  /* int[imcs_cache_size] */
    int*    hash_table; /* int[imcs_cache_size] */
    int     n_dirty_pages; /* number of used items in array dirty_page */ 
    int     n_used_items; /* number of used items in cache (<= imcs_cache_size), initially 0 */
    int     free_items_chain; /* L1 list of free pages (linked by "next" field) */
    int     lru_internal; /* index of least recently used internal page: it is used to separate in LRU list leaf pages from internal pages */
    uint64  n_used_pages;
    uint64  file_size;  /* size of data file */
    uint64  free_pages_chain_head; /* head of L1-list of free pages */
    uint64  free_pages_chain_tail; /* tail of L1-list of free pages */
    slock_t mutex; /* spinlock synchronizing access to the cache */
} imcs_disk_cache_t;

#ifdef IMCS_DISK_SUPPORT

#define IMCS_PAGE_DATA(cache, pid) (imcs_page_t*)(cache->data + ((pid)-1)*imcs_page_size)

typedef enum { 
    PM_READ_ONLY,
    PM_READ_WRITE,
    PM_NEW
} imcs_page_access_mode_t;

#define IMCS_LOAD_PAGE(pg) pg = imcs_load_page(pg, PM_READ_ONLY)
#define IMCS_LOAD_NEW_PAGE(pg) pg = imcs_load_page(pg, PM_NEW)
#define IMCS_LOAD_PAGE_FOR_UPDATE(pg) pg = imcs_load_page(pg, PM_READ_WRITE)
#define IMCS_UNLOAD_PAGE(pg) imcs_unload_page(pg), pg = 0

imcs_page_t* imcs_load_page(imcs_page_t* pg, imcs_page_access_mode_t mode);
void imcs_unload_page(imcs_page_t* pg);
void imcs_disk_initialize(imcs_disk_cache_t* cache);
void imcs_disk_open(void);
void imcs_disk_close(void);
void imcs_disk_flush(void);

#else

#define IMCS_LOAD_PAGE(pg) 
#define IMCS_LOAD_NEW_PAGE(pg) 
#define IMCS_LOAD_PAGE_FOR_UPDATE(pg) 
#define IMCS_UNLOAD_PAGE(pg) 

#define imcs_disk_initialize(cache)
#define imcs_disk_open()
#define imcs_disk_close()
#define imcs_disk_flush()

#endif


#endif
