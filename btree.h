/*
 * Implementation of B-Tree storing timeseries data
 */
#ifndef __BTREE_H__
#define __BTREE_H__

#include "imcs.h"

#define OFFSETOF_ITEMS(TYPE) ((size_t)((imcs_page_t*)0)->u.val_##TYPE)
#define CHILD(pg, i) (*(imcs_node_t*)((char*)(pg) + imcs_page_size - sizeof(imcs_node_t)*((i) + 1)))
#define MAX_LEAF_ITEMS(TYPE) ((int)((imcs_page_size - OFFSETOF_ITEMS(TYPE))/sizeof(TYPE)))
#define MAX_NODE_ITEMS(TYPE) ((int)((imcs_page_size - OFFSETOF_ITEMS(TYPE))/(!is_timestamp ? sizeof(imcs_node_t) : sizeof(TYPE) + sizeof(imcs_node_t))))

#define MOVE_CHILDREN(dst_page, dst_index, src_page, src_index, size) memmove(&CHILD(dst_page, (dst_index) + (size) - 1), &CHILD(src_page, (src_index) + (size) - 1), (size)*sizeof(imcs_node_t))

#define OFFSETOF_ITEMS_CHAR       ((size_t)((imcs_page_t*)0)->u.val_char)
#define MAX_LEAF_ITEMS_CHAR(size) ((int)((imcs_page_size - OFFSETOF_ITEMS_CHAR)/(size)))
#define MAX_NODE_ITEMS_CHAR(size) ((int)((imcs_page_size - OFFSETOF_ITEMS_CHAR)/sizeof(imcs_node_t)))

typedef struct imcs_node_t {
    struct imcs_page_t_* page;
    uint64 count;
} imcs_node_t;

struct imcs_page_t_ { 
    uint32 n_items : 31;    
    uint32 is_leaf : 1;
    union {
        char   val_char[1];
        int8   val_int8[1];
        int16  val_int16[1];
        int32  val_int32[1];
        int64  val_int64[1];
        float  val_float[1];
        double val_double[1];
        imcs_node_t child[1]; /* filled from the end of the page: child[imcs_page_size / sizeof(imcs_node_t) - index] */
    } u;
};    

typedef struct imcs_iterator_stack_item_t_ {
    imcs_page_t* page;
    int pos;
} imcs_iterator_stack_item_t;

#define IMCS_STACK_SIZE 16

typedef struct imcs_iterator_context_t_ 
{
    int stack_size;
    int direction; /* used for tiemstamp join */
    imcs_iterator_stack_item_t stack[IMCS_STACK_SIZE];
} imcs_iterator_context_t;

typedef enum { 
    BOUNDARY_OPEN,
    BOUNDARY_INCLUSIVE,
    BOUNDARY_EXCLUSIVE,
    BOUNDARY_EXACT
} imcs_boundary_kind_t;

extern void imcs_subseq_random_access_iterator(imcs_iterator_h iterator, imcs_pos_t from, imcs_pos_t till);

extern imcs_iterator_h imcs_subseq(imcs_timeseries_t* ts, imcs_pos_t from, imcs_pos_t till);
extern imcs_iterator_h imcs_map(imcs_iterator_h ts, imcs_iterator_h map_iterator);

extern void imcs_delete(imcs_timeseries_t* ts, imcs_pos_t from, imcs_pos_t till);
extern imcs_count_t imcs_delete_all(imcs_timeseries_t* ts);

#define IMCS_BTREE_METHODS(TYPE)                                        \
    extern void imcs_append_##TYPE(imcs_timeseries_t* ts, TYPE val);    \
    extern bool imcs_first_##TYPE(imcs_timeseries_t* ts, TYPE* val);    \
    extern bool imcs_last_##TYPE(imcs_timeseries_t* ts, TYPE* val);     \
    extern imcs_iterator_h imcs_search_##TYPE(imcs_timeseries_t* ts, TYPE low, imcs_boundary_kind_t low_boundary, TYPE high, imcs_boundary_kind_t high_boundary); \
    extern bool imcs_search_page_##TYPE(imcs_page_t* root, imcs_iterator_h iterator, TYPE val, imcs_boundary_kind_t boundary, int level)

IMCS_BTREE_METHODS(int8);
IMCS_BTREE_METHODS(int16);
IMCS_BTREE_METHODS(int32);
IMCS_BTREE_METHODS(int64);
IMCS_BTREE_METHODS(float);
IMCS_BTREE_METHODS(double);

extern void imcs_append_char(imcs_timeseries_t* ts, char const* val, size_t val_len); 

#endif
