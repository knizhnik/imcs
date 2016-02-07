#include "disk.h"
#include "btree.h"
#include "fileio.h"

static imcs_file_h imcs_file;
static imcs_disk_cache_t* imcs_disk_cache;

inline static void imcs_unlink(int pid)
{
    imcs_disk_cache_t* cache = imcs_disk_cache;
    imcs_cache_item_t* item = &cache->items[pid];

    cache->items[item->prev].next = item->next;
    cache->items[item->next].prev = item->prev;

    /* adjust if needed pointer of last internal page in LRU list */
    if (cache->lru_internal == pid) { 
        cache->lru_internal = item->prev;
    }
}

inline static void imcs_link_after(int after, int pid)
{
    imcs_disk_cache_t* cache = imcs_disk_cache;
    imcs_cache_item_t* item = &cache->items[pid];

    cache->items[item->next = cache->items[after].next].prev = pid;
    cache->items[item->prev = after].next = pid;
}

imcs_page_t* imcs_load_page(imcs_page_t* pg, imcs_page_access_mode_t mode)
{
    size_t offs = (size_t)pg;
    size_t h = (offs / imcs_page_size) % imcs_cache_size;    
    size_t pid;
    imcs_cache_item_t* item;
    imcs_disk_cache_t* cache = imcs_disk_cache;
    
  Retry:
    SpinLockAcquire(&cache->mutex);
    for (pid = cache->hash_table[h]; pid != 0; pid = item->collision) { 
        item = &cache->items[pid];
        if (item->offs == offs) { 
            while (item->is_busy) { 
                SpinLockRelease(&cache->mutex);
                SPIN_DELAY();
                goto Retry;
            }
            if (item->access_count++ == 0) { /* pin page in memory: exclude from LRU list */
                imcs_unlink(pid);
            }
            if (mode != PM_READ_ONLY) { /* page will be updated */
                if (item->dirty_index == 0) { /* page was not yet modified */
                    cache->dirty_pages[cache->n_dirty_pages] = pid; /* include in dirty pages list */
                    item->dirty_index = ++cache->n_dirty_pages;
                }
            }
            SpinLockRelease(&cache->mutex);
            return IMCS_PAGE_DATA(cache, pid);
        }
    }            
    if (cache->free_items_chain != 0) { 
        pid = cache->free_items_chain;
        cache->free_items_chain = cache->items[pid].next;
    } else if (cache->n_used_items < imcs_cache_size) { 
        pid = ++cache->n_used_items;
    } else { /* no free items, replace LRU item */
        size_t vh;
        int* pp;
        pid = cache->items->prev; /* LRU victim */
        if (pid == 0) { /* no free pages */
            imcs_ereport(ERRCODE_OUT_OF_MEMORY, "no available page in cache");
        }
        item = &cache->items[pid];

        /* exclude item from hash table */
        vh = (size_t)(item->offs / imcs_page_size) % imcs_cache_size;    
        for (pp = &cache->hash_table[vh]; *pp != pid; pp = &cache->items[*pp].collision) { 
            Assert(*pp != 0); /* item should be present in collision chain */
        }
        *pp = item->collision;

        /* exclude item from LRU list */
        imcs_unlink(pid);

        /* save dirty page */
        if (item->dirty_index) { 
            pg = IMCS_PAGE_DATA(cache, pid);
            imcs_file_write(imcs_file, pg, imcs_page_size, item->offs);
            cache->dirty_pages[item->dirty_index-1] = cache->dirty_pages[--cache->n_dirty_pages]; /* exclude from dirty list */
        }
    }
    item = &cache->items[pid];
    if (mode != PM_NEW) { 
        /* prepare to load page from the disk: mark it as busy to avoid redundant reads */
        pg = IMCS_PAGE_DATA(cache, pid);
        item->is_busy = true;    
        SpinLockRelease(&cache->mutex); /* release mutex during IO */
        imcs_file_read(imcs_file, pg, imcs_page_size, offs); /* read page */
        SpinLockAcquire(&cache->mutex); 
    }
    if (mode != PM_READ_ONLY) { /* include page in dirty list */
        cache->dirty_pages[cache->n_dirty_pages] = pid;
        item->dirty_index = ++cache->n_dirty_pages;
    } else { 
        item->dirty_index = 0;
    }
    item->offs = offs;
    item->collision = cache->hash_table[h];
    cache->hash_table[h] = pid;
    item->access_count = 1;
    item->is_busy = false;  
    SpinLockRelease(&cache->mutex);
    return IMCS_PAGE_DATA(cache, pid);
}

void imcs_unload_page(imcs_page_t* pg)
{
    imcs_disk_cache_t* cache = imcs_disk_cache;
    size_t pid = ((char*)pg - cache->data)/imcs_page_size + 1;
    imcs_cache_item_t* item = &cache->items[pid];
    Assert(pid-1 < (size_t)imcs_cache_size);
    SpinLockAcquire(&cache->mutex); 
    if (--item->access_count == 0) { /* unpin page */
        imcs_link_after(pg->is_leaf ? cache->lru_internal : 0, pid);
        if (!pg->is_leaf && cache->lru_internal == 0) { 
            cache->lru_internal = pid;
        }
    }
    SpinLockRelease(&cache->mutex);
}

void imcs_disk_initialize(imcs_disk_cache_t* cache)
{
    memset(cache, 0, sizeof(*cache));
    cache->items = (imcs_cache_item_t*)ShmemAlloc((imcs_cache_size+1)*sizeof(imcs_cache_item_t));
    if (cache->items == NULL) { 
        imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory for disk cache");
    }
    cache->data = (char*)ShmemAlloc((size_t)imcs_cache_size*imcs_page_size);
    if (cache->data == NULL) { 
        imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory for disk cache");
    }
    cache->hash_table = (int*)ShmemAlloc(imcs_cache_size*sizeof(int));
    if (cache->hash_table == NULL) { 
        imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory for disk cache");
    }
    memset(cache->hash_table, 0, imcs_cache_size*sizeof(int));

    cache->dirty_pages = (int*)ShmemAlloc(imcs_cache_size*sizeof(int));
    if (cache->dirty_pages == NULL) { 
        imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory for disk cache");
    }
    cache->file_size = imcs_page_size; /* reserve first page to make address not NULL */
    SpinLockInit(&cache->mutex);
    imcs_disk_cache = cache;
}

    
void imcs_disk_open(void)
{
    imcs_file = imcs_file_open(imcs_file_path);
}

void imcs_disk_close(void)
{
    imcs_file_close(imcs_file);
}

static int compare_page_offset(void const* p, void const* q) 
{ 
    imcs_cache_item_t* p1 = &imcs_disk_cache->items[*(int*)p];
    imcs_cache_item_t* p2 = &imcs_disk_cache->items[*(int*)q];
    return p1->offs < p2->offs ? -1 : p1->offs == p2->offs ? 0 : 1;
}

void imcs_disk_flush(void)
{
    imcs_disk_cache_t* cache = imcs_disk_cache;
    int i, n;
    SpinLockAcquire(&cache->mutex);
    n = cache->n_dirty_pages;
    if (n != 0) { 
        /* sort dirty pages by offset so that them will be written in more or less sequential order */
        qsort(cache->dirty_pages, n, sizeof(int), compare_page_offset);
        for (i = 0; i < n; i++) {
            int pid = cache->dirty_pages[i];
            imcs_cache_item_t* item = &cache->items[pid];
            imcs_page_t* pg = IMCS_PAGE_DATA(cache, pid);
            imcs_file_write(imcs_file, pg, imcs_page_size, item->offs);
            item->dirty_index = 0;
        }
        cache->n_dirty_pages = 0;
    }
    SpinLockRelease(&cache->mutex);

	if (imcs_flush_file) {
		imcs_file_flush(imcs_file);
	}
}

/* This function is called in context protected by imcs->lock */
imcs_page_t* imcs_new_page(void)
{
    imcs_disk_cache_t* cache = imcs_disk_cache;
    uint64 addr = cache->free_pages_chain_head;
    if (addr != 0) { /* free page list is not empty */
        if (cache->free_pages_chain_head == cache->free_pages_chain_tail) { 
            /* free page list is now empty */
            cache->free_pages_chain_head = cache->free_pages_chain_tail = 0;
        } else { 
            if (!imcs_file_read(imcs_file, &cache->free_pages_chain_head, sizeof cache->free_pages_chain_head, addr)) { 
                imcs_ereport(ERRCODE_IO_ERROR, "Failed to read free page");
            }
            Assert(cache->free_pages_chain_tail != 0);
        }
    } else { 
        addr = cache->file_size;
        cache->file_size += imcs_page_size;
    }
    cache->n_used_pages += 1;
    return (imcs_page_t*)(size_t)addr;
}

/* This function is called in context protected by imcs->lock.
 * "page" is address of page in RAM.
 * This function deallocates page, include it in free pages list and exclusde correspondent item from cache 
 */
void imcs_free_page(imcs_page_t* pg) 
{ 
    imcs_disk_cache_t* cache = imcs_disk_cache;
    size_t pid = ((char*)pg - cache->data)/imcs_page_size + 1;
    imcs_cache_item_t* item = &cache->items[pid];
    int* pp;
    size_t h = (size_t)(item->offs / imcs_page_size) % imcs_cache_size;    

    Assert(pid-1 < (size_t)imcs_cache_size);
    Assert(item->access_count == 1); /* removed page is pinned */
    
    /* remove item from hash table */
    for (pp = &cache->hash_table[h]; *pp != pid; pp = &cache->items[*pp].collision) { 
        Assert(*pp != 0); /* item should be present in collision chain */
    }
    *pp = item->collision;

    /* exclude page from dirty list */
    if (item->dirty_index) { 
        cache->dirty_pages[item->dirty_index-1] = cache->dirty_pages[--cache->n_dirty_pages];
    }
     
    /* include item in free items list */
    item->next = cache->free_items_chain;
    cache->free_items_chain = pid;

    /* append page to free pages list */
    if (cache->free_pages_chain_tail != 0) { 
        imcs_file_write(imcs_file, &item->offs, sizeof item->offs, cache->free_pages_chain_tail);
    } else { 
        cache->free_pages_chain_head = item->offs;
    }
    cache->free_pages_chain_tail = item->offs;
    cache->n_used_pages -= 1;
}

uint64 imcs_used_memory(void)
{
    return imcs_disk_cache == NULL ? 0 : imcs_disk_cache->n_used_pages*imcs_page_size;
}
