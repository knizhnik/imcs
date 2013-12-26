#include <btree.h>
#include <string.h>

static bool imcs_next_tile(imcs_iterator_h iterator)    
{                  
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)iterator->context; 
    int i = ctx->stack_size-1;                                          
    if (i >= 0 && iterator->next_pos <= iterator->last_pos) {      
        imcs_page_t* pg;                                             
        size_t tile_size = 0, n_vals;                               
        while (true) {                                                     
            size_t inc = 0;
            while (true) {                                                 
                pg = ctx->stack[i].page; 
                ctx->stack[i].pos += inc;
                if (ctx->stack[i].pos == pg->n_items) {                 
                    inc = 1;
                    if (--i < 0) {                                      
                        ctx->stack_size = 0;                            
                        if (tile_size == 0) {                           
                            return false;                    
                        } else {                                        
                            iterator->tile_size = tile_size;            
                            return true;                            
                        }                                               
                    }                                                   
                } else {                                                
                    Assert(pg->n_items > ctx->stack[i].pos);                
                    break;                                              
                }                                                       
            }                                                           
            while (!pg->is_leaf) {      
                ctx->stack[i+1].page = CHILD(pg, ctx->stack[i].pos).page; 
                ctx->stack[++i].pos = 0;                                
                pg = ctx->stack[i].page; 
            }                          
            Assert(pg->n_items > ctx->stack[i].pos);                
            n_vals = pg->n_items - ctx->stack[i].pos;                   
            if (n_vals - 1 > iterator->last_pos - iterator->next_pos) {
                n_vals = (size_t)(iterator->last_pos - iterator->next_pos + 1); 
            }                                                           
            if (n_vals > imcs_tile_size - tile_size) {               
                n_vals = imcs_tile_size - tile_size;                 
            }                                                           
            memcpy(&iterator->tile.arr_char[tile_size*iterator->elem_size], &pg->u.val_char[ctx->stack[i].pos*iterator->elem_size], n_vals*iterator->elem_size); 
            iterator->next_pos += n_vals;                            
            ctx->stack[i].pos += n_vals; 
            ctx->stack_size = i + 1;
            tile_size += n_vals;                                        
            if (tile_size == imcs_tile_size || iterator->next_pos > iterator->last_pos) {                       
                iterator->tile_size = tile_size;                        
                return true;                                        
            }                                                           
        }                                                               
    }                                                                   
    return false;                                            
}                                                                       


static bool imcs_subseq_page(imcs_iterator_h iterator, imcs_page_t* pg,  imcs_pos_t from, int level) 
{                                                                       
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)iterator->context; 
    int n_items;                                                        
    Assert(level < IMCS_STACK_SIZE);                
    ctx->stack[level].page = pg;                                      
    n_items = pg->n_items;                                              
    Assert(n_items > 0);                               
    if (!pg->is_leaf) {                 
        int i;                                                          
        for (i = 0; i < n_items; i++) {                                 
            imcs_count_t count = CHILD(pg, i).count;                 
            if (from < count) {                                         
                imcs_page_t* child = CHILD(pg, i).page;      
                ctx->stack[level].pos = i;                              
                return imcs_subseq_page(iterator, child, from, level+1); 
            }                                                           
            from -= count;                                              
        }                                                               
    } else {                                                            
        if (from < (imcs_pos_t)n_items) {                             
            ctx->stack[level].pos = (int)from;                          
            ctx->stack_size = level+1;                                  
            return true;                                            
        }                                                               
    }                                                                   
    return false;                                              
}                                                                       
                                                                        
static void imcs_reset_tree_iterator(imcs_iterator_h iterator) 
{ 
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)iterator->context; 
    imcs_subseq_page(iterator, ctx->stack[0].page, iterator->first_pos, 0);
    imcs_reset_iterator(iterator);
}

void imcs_subseq_random_access_iterator(imcs_iterator_h iterator, imcs_pos_t from, imcs_pos_t till)
{
    imcs_timeseries_t* ts = iterator->cs_hdr;
    Assert(iterator->last_pos != IMCS_INFINITY);    
    from = ((int64)from < 0) ? iterator->last_pos + from + 1 : iterator->first_pos + from;
    till = ((int64)till < 0) ? iterator->last_pos + till + 1 : iterator->first_pos + till;
    if (from < iterator->first_pos) { 
        from = iterator->first_pos;
    }
    if (till > iterator->last_pos) { 
        till = iterator->last_pos;
    }
    if (till >= from && (ts == NULL || (ts->root_page != NULL && imcs_subseq_page(iterator, ts->root_page, from, 0)))) { 
        iterator->first_pos = iterator->next_pos = from;                               
        iterator->last_pos = till;
    } else {                                                                    
        iterator->first_pos = iterator->next_pos = 1;                                          
        iterator->last_pos = 0;                                          
    }
}
 
imcs_iterator_h imcs_subseq(imcs_timeseries_t* ts, imcs_pos_t from, imcs_pos_t till) 
{                                                                       
    imcs_iterator_t* iterator = (imcs_iterator_t*)imcs_new_iterator(ts->elem_size, sizeof(imcs_iterator_context_t));
    iterator->cs_hdr = ts;                                              
    iterator->elem_type = ts->elem_type;
    iterator->flags = FLAG_CONTEXT_FREE|FLAG_RANDOM_ACCESS;             
    iterator->reset = imcs_reset_tree_iterator;                                 
    iterator->next = imcs_next_tile;                                 
    if (till >= from && ts->root_page != NULL && imcs_subseq_page(iterator, ts->root_page, from, 0)) { 
        iterator->first_pos = iterator->next_pos = from;                               
        iterator->last_pos = till >= ts->count ? ts->count-1 : till;
    } else {                                                                    
        iterator->first_pos = iterator->next_pos = 1;                                          
        iterator->last_pos = 0;                                          
    }
    return iterator;                                                          
}                                                                       
                           
static bool imcs_map_next(imcs_iterator_h iterator)    
{
    size_t i, tile_size;  
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)iterator->context; 
    size_t elem_size = iterator->elem_size;
    size_t prev_page_size = 0;
    imcs_pos_t prev_page_pos = 0;
    imcs_iterator_h map = iterator->opd[0];

    if (!map->next(map)) { 
        return false;                                                      
    }
    tile_size = map->tile_size;
    for (i = 0; i < tile_size; i++) {          
        imcs_pos_t next_pos = iterator->first_pos + map->tile.arr_int64[i]; 
        imcs_page_t* pg;
        if (next_pos - prev_page_pos >= prev_page_size) { 
            if (!imcs_subseq_page(iterator, ctx->stack[0].page, next_pos, 0)) { 
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid position in timeseries"))); 
            }
            prev_page_pos = next_pos - ctx->stack[ctx->stack_size-1].pos;
        }
        pg = ctx->stack[ctx->stack_size-1].page; 
        prev_page_size = pg->n_items;
        Assert(next_pos - prev_page_pos < prev_page_size);
        memcpy(&iterator->tile.arr_char[i*elem_size], &pg->u.val_char[((size_t)(next_pos - prev_page_pos))*elem_size], elem_size);
    }
    iterator->next_pos += tile_size;
    iterator->tile_size = tile_size;                        
    return true;                                        
}                                             
                         
imcs_iterator_h imcs_map(imcs_iterator_h input, imcs_iterator_h map_iterator) 
{                                                                       
    imcs_timeseries_t* ts = input->cs_hdr;
    imcs_iterator_h iterator = imcs_new_iterator(ts->elem_size, sizeof(imcs_iterator_context_t));
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)iterator->context; 
    iterator->elem_type = ts->elem_type;
    iterator->flags = FLAG_CONTEXT_FREE;
    iterator->next = imcs_map_next;                                 
    iterator->opd[0] = map_iterator;
    iterator->first_pos = iterator->next_pos = input->first_pos;
    ctx->stack[0].page = ts->root_page;
    return iterator;                                                          
}                                                                       

    
#define IMCS_IMPLEMENTATIONS(TYPE) \
bool imcs_first_##TYPE(imcs_timeseries_t* ts, TYPE* val)                \
{                                                                       \
    imcs_page_t* pg = ts->root_page;                                    \
    if (pg == NULL) {                                                   \
        return false;                                                   \
    }                                                                   \
    while (!pg->is_leaf) {                                              \
        pg = CHILD(pg, 0).page;                                         \
    }                                                                   \
    *val = pg->u.val_##TYPE[0];                                         \
    return true;                                                        \
}                                                                       \
bool imcs_last_##TYPE(imcs_timeseries_t* ts, TYPE* val)                 \
{                                                                       \
    imcs_page_t* pg = ts->root_page;                                    \
    if (pg == NULL) {                                                   \
        return false;                                                   \
    }                                                                   \
    while (!pg->is_leaf) {                                              \
        pg = CHILD(pg, pg->n_items-1).page;                             \
    }                                                                   \
    *val = pg->u.val_##TYPE[pg->n_items-1];                             \
    return true;                                                        \
}                                                                       \
/* return false on overflow */                                          \
static bool imcs_append_page_##TYPE(imcs_page_t** root_page, TYPE val, bool is_timestamp) \
{                                                                       \
    imcs_page_t* pg = *root_page;                                       \
    int n_items = pg->n_items;                                          \
    Assert(n_items > 0);                                                \
    if (is_timestamp && pg->u.val_##TYPE[n_items-1] > val) {            \
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("value out of timeseries order"))); \
    }                                                                   \
    if (!pg->is_leaf) {                                                 \
        imcs_page_t* child = CHILD(pg, n_items-1).page;                 \
        if (!imcs_append_page_##TYPE(&child, val, is_timestamp)) {      \
            if (n_items == MAX_NODE_ITEMS(TYPE)) {                      \
                imcs_page_t* new_page = imcs_new_page();                \
                *root_page = new_page;                                  \
                new_page->is_leaf = false;                              \
                new_page->n_items = 1;                                  \
                CHILD(new_page, 0).page = child;                        \
                CHILD(new_page, 0).count = 1;                           \
                if (is_timestamp) {                                     \
                    new_page->u.val_##TYPE[0] = val;                    \
                }                                                       \
                return false;                                           \
            } else {                                                    \
                CHILD(pg, n_items).page = child;                        \
                CHILD(pg, n_items).count = 1;                           \
                if (is_timestamp) {                                     \
                    pg->u.val_##TYPE[n_items] = val;                    \
                }                                                       \
                pg->n_items += 1;                                       \
            }                                                           \
        } else {                                                        \
            CHILD(pg, n_items-1).count += 1;                            \
        }                                                               \
    } else {                                                            \
        int max_items = MAX_LEAF_ITEMS(TYPE);                           \
        Assert(n_items <= max_items);                                   \
        if (n_items == max_items) {                                     \
            imcs_page_t* new_page = imcs_new_page();                    \
            *root_page = new_page;                                      \
            new_page->is_leaf = true;                                   \
            new_page->u.val_##TYPE[0] = val;                            \
            new_page->n_items = 1;                                      \
            return false;                                               \
        } else {                                                        \
            pg->u.val_##TYPE[n_items] = val;                            \
            pg->n_items += 1;                                           \
        }                                                               \
    }                                                                   \
    return true;                                                        \
}                                                                       \
                                                                        \
void imcs_append_##TYPE(imcs_timeseries_t* ts, TYPE val)                \
{                                                                       \
    if (ts->root_page == 0) {                                           \
        ts->root_page = imcs_new_page();                                \
        ts->root_page->is_leaf = true;                                  \
        ts->root_page->u.val_##TYPE[0] = val;                           \
        ts->count = ts->root_page->n_items = 1;                         \
    } else {                                                            \
        imcs_page_t* root_page = ts->root_page;                         \
        if (!imcs_append_page_##TYPE(&root_page, val, ts->is_timestamp)) { \
            imcs_page_t* new_root = imcs_new_page();                    \
            new_root->is_leaf = false;                                  \
            new_root->n_items = 2;                                      \
            CHILD(new_root, 0).page = ts->root_page;                    \
            CHILD(new_root, 0).count = ts->count;                       \
            CHILD(new_root, 1).page = root_page;                        \
            CHILD(new_root, 1).count = 1;                               \
            if (ts->is_timestamp) {                                     \
                new_root->u.val_##TYPE[0] = ts->root_page->u.val_##TYPE[0]; \
                new_root->u.val_##TYPE[1] = val;                        \
            }                                                           \
            ts->root_page = new_root;                                   \
        }                                                               \
        ts->count += 1;                                                 \
    }                                                                   \
}                                                                       \
bool imcs_search_page_##TYPE(imcs_page_t* pg, imcs_iterator_h iterator, TYPE val, imcs_boundary_kind_t boundary, int level) \
{                                                                       \
    int i, l, r, n_items;                                               \
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)iterator->context; \
    bool found = false;                                                 \
    Assert(level < IMCS_STACK_SIZE);                                    \
    ctx->stack[level].page = pg;                                        \
    n_items = pg->n_items;                                              \
    Assert(n_items > 0);                                                \
    l = 0;                                                              \
    r = n_items;                                                        \
    if (boundary == BOUNDARY_INCLUSIVE || boundary == BOUNDARY_EXACT)  { \
        while (l < r) {                                                 \
            int m = (l + r) >> 1;                                       \
            if (pg->u.val_##TYPE[m] < val) {                            \
                l = m + 1;                                              \
            } else {                                                    \
                r = m;                                                  \
            }                                                           \
        }                                                               \
    } else if (boundary == BOUNDARY_EXCLUSIVE)  {                       \
        while (l < r) {                                                 \
            int m = (l + r) >> 1;                                       \
            if (pg->u.val_##TYPE[m] <= val) {                           \
                l = m + 1;                                              \
            } else {                                                    \
                r = m;                                                  \
            }                                                           \
        }                                                               \
    }                                                                   \
    if (!pg->is_leaf) {                                                 \
        if (l > 0) {                                                    \
            found = imcs_search_page_##TYPE(CHILD(pg, l-1).page, iterator, val, boundary, level+1); \
            if (found) {                                                \
                l -= 1;                                                 \
            }                                                           \
        }                                                               \
        if (!found && l < n_items) {                                    \
             found = imcs_search_page_##TYPE(CHILD(pg, l).page, iterator, val, boundary, level+1); \
        }                                                               \
        if (found) {                                                    \
            for (i = 0; i < l; i++) {                                   \
                iterator->next_pos += CHILD(pg, i).count;               \
            }                                                           \
            ctx->stack[level].pos = l;                                  \
        }                                                               \
    } else {                                                            \
        if (l < n_items && (boundary != BOUNDARY_EXACT || pg->u.val_##TYPE[l] == val)) { \
            iterator->next_pos += l;                                    \
            ctx->stack[level].pos = l;                                  \
            ctx->stack_size = level+1;                                  \
            found = true;                                               \
        }                                                               \
    }                                                                   \
    return found;                                                       \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_search_##TYPE(imcs_timeseries_t* ts, TYPE low, imcs_boundary_kind_t low_boundary, TYPE high, imcs_boundary_kind_t high_boundary) \
{                                                                       \
    imcs_iterator_h iterator = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_iterator_context_t)); \
    iterator->reset = imcs_reset_tree_iterator;                         \
    iterator->next = imcs_next_tile;                                    \
    iterator->elem_type = ts->elem_type;                                \
    iterator->cs_hdr = ts;                                              \
    iterator->flags = FLAG_CONTEXT_FREE|FLAG_RANDOM_ACCESS;             \
    if (ts->root_page != NULL) {                                        \
        if (high_boundary != BOUNDARY_OPEN) {                           \
            iterator->next_pos = 0;                                     \
            if (!imcs_search_page_##TYPE(ts->root_page, iterator, high, BOUNDARY_INCLUSIVE + BOUNDARY_EXCLUSIVE - high_boundary, 0)) { \
                iterator->last_pos = ts->count-1;                       \
            } else {                                                    \
                if (iterator->next_pos == 0) {                          \
                    iterator->first_pos = iterator->next_pos = 1;       \
                    iterator->last_pos = 0;                             \
                } else {                                                \
                    iterator->last_pos = iterator->next_pos - 1;        \
                }                                                       \
            }                                                           \
        } else {                                                        \
            iterator->last_pos = ts->count-1;                           \
        }                                                               \
        iterator->next_pos = 0;                                         \
        if (imcs_search_page_##TYPE(ts->root_page, iterator, low, low_boundary, 0)) { \
            if (iterator->next_pos <= iterator->last_pos) {             \
                iterator->first_pos = iterator->next_pos;               \
                return iterator;                                        \
            }                                                           \
        }                                                               \
    }                                                                   \
    iterator->first_pos = iterator->next_pos = 1;                       \
    iterator->last_pos = 0;                                             \
    return iterator;                                                    \
}                                                                       \

IMCS_IMPLEMENTATIONS(int8)
IMCS_IMPLEMENTATIONS(int16)
IMCS_IMPLEMENTATIONS(int32)
IMCS_IMPLEMENTATIONS(int64)
IMCS_IMPLEMENTATIONS(float)
IMCS_IMPLEMENTATIONS(double)

static bool imcs_append_page_char(imcs_page_t** root_page, char const* val, size_t val_len, size_t elem_size) 
{                                                                       
    imcs_page_t* pg = *root_page;
    int n_items = pg->n_items;                                          
    Assert(n_items > 0);                                                
    if (!pg->is_leaf) {                                                 
        imcs_page_t* child = CHILD(pg, n_items-1).page;                 
        if (!imcs_append_page_char(&child, val, val_len, elem_size)) {             
            if (n_items == MAX_NODE_ITEMS_CHAR(elem_size)) {                      
                imcs_page_t* new_page = imcs_new_page();                
                *root_page = new_page;
                new_page->is_leaf = false;                              
                new_page->n_items = 1;              
                CHILD(new_page, 0).page = child;                        
                CHILD(new_page, 0).count = 1;                           
                return false;                                           
            } else {                                                    
                CHILD(pg, n_items).page = child;                        
                CHILD(pg, n_items).count = 1;                           
                pg->n_items += 1;                                       
            }                                                           
        } else {                                                        
            CHILD(pg, n_items-1).count += 1;                            
        }                                                               
    } else {                                                            
        int max_items = MAX_LEAF_ITEMS_CHAR(elem_size);                           
        Assert(n_items <= max_items);                                   
        if (n_items == max_items) {                                     
            imcs_page_t* new_page = imcs_new_page();                    
            *root_page = new_page;
            new_page->is_leaf = true;                                  
            memcpy(new_page->u.val_char, val, val_len);                            
            memset(new_page->u.val_char + val_len, '\0', elem_size - val_len);                            
            new_page->n_items = 1;                                      
            return false;                                               
        } else {                                                        
            memcpy(&pg->u.val_char[n_items*elem_size], val, val_len);                            
            memset(&pg->u.val_char[n_items*elem_size] + val_len, '\0', elem_size - val_len);                            
            pg->n_items += 1;                                           
        }                                                               
    }                                                                   
    return true;                                                        
}                                                                       

                                                                        
void imcs_append_char(imcs_timeseries_t* ts, char const* val, size_t val_len)                
{                                                                       
    Assert(!ts->is_timestamp);
    if (ts->root_page == 0) {                                           
        ts->root_page = imcs_new_page();                                
        ts->root_page->is_leaf = true;                                  
        memcpy(ts->root_page->u.val_char, val, val_len);
        memset(ts->root_page->u.val_char + val_len, 0, ts->elem_size - val_len);                          
        ts->count = ts->root_page->n_items = 1;                              
    } else {                                                            
        imcs_page_t* root_page = ts->root_page;                         
        if (!imcs_append_page_char(&root_page, val, val_len, ts->elem_size)) {     
            imcs_page_t* new_root = imcs_new_page();                    
            new_root->is_leaf = false;                                  
            new_root->n_items = 2;                                      
            CHILD(new_root, 0).page = ts->root_page;                    
            CHILD(new_root, 0).count = ts->count;                       
            CHILD(new_root, 1).page = root_page;                        
            CHILD(new_root, 1).count = 1;                               
            ts->root_page = new_root;                                   
        }                                                               
        ts->count += 1;                                                 
    }                                                                   
}                                                                         

/* returns fals euon underflow */
static bool imcs_delete_page(imcs_timeseries_t* ts, imcs_page_t* pg, imcs_pos_t from, imcs_pos_t till)
{
    int n_items = pg->n_items;
    int elem_size = ts->elem_size;
    if (!pg->is_leaf) {                 
        int i;                                                          
        for (i = 0; i < n_items; i++) {                                 
            imcs_count_t count = CHILD(pg, i).count;                 
            if (from < count) {                                         
                int j = i;                                              
                do {                                                    
                    count = CHILD(pg, i).count;                         
                    if (imcs_delete_page(ts, CHILD(pg, i).page, from, till)) { 
                        if (till < count) {                            
                            CHILD(pg, i).count -= till - from + 1;      
                            break;                                      
                        }                                               
                        CHILD(pg, i).count = from;                      
                        j = i + 1;                                      
                        from = 0;                                       
                    } else { 
                        if (till < count) {                            
                            i += 1;
                            break;
                        }
                    }
                    till -= count;                                      
                } while (++i < n_items);                                

                if (i != j) {                                           
                    MOVE_CHILDREN(pg, j, pg, i, n_items-i); 
                    if (ts->is_timestamp) {
                        memmove(&pg->u.val_char[j*elem_size], &pg->u.val_char[i*elem_size], (n_items-i)*elem_size); 
                    }       
                    pg->n_items -= i - j;
                    if (pg->n_items == 0) { 
                        imcs_free_page(pg);
                        return false;
                    }
                }
                break;
            } else {                                                          
                from -= count;                               
                till -= count;
            }                   
        }                                            
    } else { 
        if (till >= (imcs_pos_t)n_items) {                            
            till = n_items-1;                                           
        }                                                               
        ts->count -= till - from + 1;       
        if ((int)(till - from + 1) == n_items) {                        
            imcs_free_page(pg);
            return false;
        } else {                                                        
            memmove(&pg->u.val_char[from*elem_size], &pg->u.val_char[(till+1)*elem_size], (n_items-till-1)*elem_size); 
            pg->n_items -= till - from + 1;                             
        }      
    }
    return true;
}

void imcs_delete(imcs_timeseries_t* ts, imcs_pos_t from, imcs_pos_t till)
{
    imcs_page_t* root_page = ts->root_page;                         
    if (root_page != NULL) {                                           
        if (!imcs_delete_page(ts, root_page, from, till)) { 
            Assert(ts->count == 0);
            ts->root_page = NULL;
        }
    }
}
        
static void imcs_prune(imcs_page_t* pg) 
{ 
    if (!pg->is_leaf) { 
        int i, n;
        for (i = 0, n = pg->n_items; i < n; i++) {
            imcs_prune(CHILD(pg, i).page);
        }
    }
    imcs_free_page(pg);
}

imcs_count_t imcs_delete_all(imcs_timeseries_t* ts)
{
    imcs_page_t* root_page = ts->root_page;                             
    imcs_count_t count = ts->count;
    if (root_page != NULL) {                                                   
        imcs_prune(root_page);
        ts->root_page = NULL;
    }
    ts->count = 0;
    return count;
}
        
