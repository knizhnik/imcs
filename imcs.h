#ifndef __IMCS_H__
#define __IMCS_H__

#include <postgres.h>
#include <access/hash.h>
#include <access/xact.h>
#include <funcapi.h>
#include <storage/ipc.h>
#include <miscadmin.h>
#include <utils/guc.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/typcache.h>
#include <catalog/pg_type.h>
#include <catalog/pg_attrdef.h>
#include <catalog/namespace.h>
#include <libpq/pqformat.h>

struct imcs_page_t_;
typedef struct imcs_page_t_ imcs_page_t;
typedef uint64 imcs_pos_t;
typedef uint64 imcs_count_t;

extern int  imcs_page_size;
extern int  imcs_tile_size;
extern bool imcs_sync_load;

#define IMCS_INFINITY (-1)

typedef union 
{ 
    int8  val_int8;
    int16 val_int16;
    int32 val_int32;
    int64 val_int64;
    float val_float;
    double val_double;
    char* val_ptr;
} imcs_key_t;

typedef union
{
    char arr_char[1];
    int8  arr_int8[1];
    int16 arr_int16[1];
    int32 arr_int32[1];
    int64 arr_int64[1];
    float arr_float[1];
    double arr_double[1];
} imcs_tile_t;

typedef enum 
{ 
    TID_int8,
    TID_int16,
    TID_int32,
    TID_date, 
    TID_int64,
    TID_time,
    TID_timestamp,
    TID_money,
    TID_float,
    TID_double,
    TID_char
} imcs_elem_typeid_t;

extern const imcs_elem_typeid_t imcs_underlying_type[];
extern const char const* imcs_type_mnems[];

typedef enum {
    IMCS_ASC_ORDER,
    IMCS_DESC_ORDER
} imcs_order_t;

struct imcs_iterator_t_;
typedef bool(*imcs_iterator_next_t)(struct imcs_iterator_t_* iterator);
typedef void(*imcs_iterator_reset_t)(struct imcs_iterator_t_* iterator);
typedef bool(*imcs_iterator_prepare_t)(struct imcs_iterator_t_* iterator);
typedef void(*imcs_iterator_merge_t)(struct imcs_iterator_t_* dst, struct imcs_iterator_t_* src);

/**
 * Timeseries header
 */
typedef struct imcs_timeseries_t
{
    imcs_page_t* root_page; /* root  of B-Tree */
    imcs_elem_typeid_t elem_type;
    bool is_timestamp;
    int elem_size;
    imcs_count_t count;
} imcs_timeseries_t;

typedef enum
{    
    FLAG_RANDOM_ACCESS = 1, /* it is timeseries stored as B-Tree, supporting random acess by position */
    FLAG_CONTEXT_FREE  = 2, /* each element can be calculated independetly: such timeseries allows concurrent execution */ 
    FLAG_PREPARED      = 4, /* result was already prepared by prepare() function during parallel query execution */
    FLAG_CONSTANT      = 8  /* timeries of repeated costant element */
} imcs_flags_t;

enum imcs_commands 
{
    imcs_cmd_parse,
    imcs_cmd_const,
    imcs_cmd_cast,
    imcs_cmd_add, 
    imcs_cmd_mul, 
    imcs_cmd_sub, 
    imcs_cmd_div, 
    imcs_cmd_mod, 
    imcs_cmd_pow, 
    imcs_cmd_and, 
    imcs_cmd_or, 
    imcs_cmd_xor, 
    imcs_cmd_concat, 
    imcs_cmd_cat, 
    imcs_cmd_eq, 
    imcs_cmd_ne, 
    imcs_cmd_ge, 
    imcs_cmd_le, 
    imcs_cmd_lt, 
    imcs_cmd_gt, 
    imcs_cmd_maxof, 
    imcs_cmd_minof, 
    imcs_cmd_neg, 
    imcs_cmd_not, 
    imcs_cmd_bit_not, 
    imcs_cmd_abs, 
    imcs_cmd_limit, 
    imcs_cmd_sin, 
    imcs_cmd_cos, 
    imcs_cmd_tan, 
    imcs_cmd_exp, 
    imcs_cmd_asin, 
    imcs_cmd_acos, 
    imcs_cmd_atan, 
    imcs_cmd_sqrt, 
    imcs_cmd_log, 
    imcs_cmd_ceil, 
    imcs_cmd_floor, 
    imcs_cmd_isnan, 
    imcs_cmd_wsum, 
    imcs_cmd_wavg, 
    imcs_cmd_corr, 
    imcs_cmd_cov, 
    imcs_cmd_norm, 
    imcs_cmd_thin, 
    imcs_cmd_iif, 
    imcs_cmd_if, 
    imcs_cmd_filter, 
    imcs_cmd_filter_pos, 
    imcs_cmd_filter_first_pos, 
    imcs_cmd_unique, 
    imcs_cmd_reverse, 
    imcs_cmd_diff, 
    imcs_cmd_diff0, 
    imcs_cmd_repeat, 
    imcs_cmd_count, 
    imcs_cmd_approxdc, 
    imcs_cmd_max, 
    imcs_cmd_min, 
    imcs_cmd_avg, 
    imcs_cmd_sum, 
    imcs_cmd_prd, 
    imcs_cmd_var, 
    imcs_cmd_dev, 
    imcs_cmd_all, 
    imcs_cmd_any, 
    imcs_cmd_median, 
    imcs_cmd_group_count, 
    imcs_cmd_group_approxdc, 
    imcs_cmd_group_max, 
    imcs_cmd_group_min, 
    imcs_cmd_group_avg, 
    imcs_cmd_group_sum, 
    imcs_cmd_group_var, 
    imcs_cmd_group_dev, 
    imcs_cmd_group_all, 
    imcs_cmd_group_any, 
    imcs_cmd_group_last, 
    imcs_cmd_group_first, 
    imcs_cmd_grid_max, 
    imcs_cmd_grid_min, 
    imcs_cmd_grid_avg, 
    imcs_cmd_grid_sum, 
    imcs_cmd_grid_var, 
    imcs_cmd_grid_dev, 
    imcs_cmd_window_max, 
    imcs_cmd_window_min, 
    imcs_cmd_window_avg, 
    imcs_cmd_window_sum, 
    imcs_cmd_window_var, 
    imcs_cmd_window_dev, 
    imcs_cmd_window_ema, 
    imcs_cmd_window_atr, 
    imcs_cmd_hash_count, 
    imcs_cmd_hash_dup_count, 
    imcs_cmd_hash_max, 
    imcs_cmd_hash_min, 
    imcs_cmd_hash_avg, 
    imcs_cmd_hash_sum, 
    imcs_cmd_hash_all, 
    imcs_cmd_hash_any, 
    imcs_cmd_top_max, 
    imcs_cmd_top_min, 
    imcs_cmd_top_max_pos, 
    imcs_cmd_top_min_pos, 
    imcs_cmd_cum_max, 
    imcs_cmd_cum_min, 
    imcs_cmd_cum_avg, 
    imcs_cmd_cum_sum, 
    imcs_cmd_cum_prd, 
    imcs_cmd_cum_var, 
    imcs_cmd_cum_dev, 
    imcs_cmd_histogram, 
    imcs_cmd_cross, 
    imcs_cmd_extrema, 
    imcs_cmd_stretch, 
    imcs_cmd_stretch0, 
    imcs_cmd_asof_join, 
    imcs_cmd_asof_join_pos, 
    imcs_cmd_join, 
    imcs_cmd_join_pos, 
    imcs_cmd_map, 
    imcs_cmd_union, 
    imcs_cmd_empty, 
    imcs_cmd_project, 
    imcs_cmd_project_agg, 
    imcs_cmd_year, 
    imcs_cmd_month, 
    imcs_cmd_mday, 
    imcs_cmd_wday, 
    imcs_cmd_hour, 
    imcs_cmd_minute, 
    imcs_cmd_second, 
    imcs_cmd_week, 
    imcs_cmd_quarter, 
    imcs_cmd_call, 
    imcs_cmd_to_array, 
    imcs_cmd_from_array,
    imcs_cmd_like,
    imcs_cmd_ilike,
    imcs_cmd_sort, 
    imcs_cmd_sort_pos, 
    imcs_cmd_rank, 
    imcs_cmd_dense_rank, 
    imcs_cmd_quantile, 
    imcs_cmd_last_command,
    
    imcs_cmd_int8_from = imcs_cmd_cast,
    imcs_cmd_int16_from = imcs_cmd_cast,
    imcs_cmd_int32_from = imcs_cmd_cast,
    imcs_cmd_int64_from = imcs_cmd_cast,
    imcs_cmd_float_from = imcs_cmd_cast,
    imcs_cmd_double_from = imcs_cmd_cast,

    imcs_cmd_stretch_int32 = imcs_cmd_stretch,
    imcs_cmd_stretch_int64 = imcs_cmd_stretch,
    imcs_cmd_stretch0_int32 = imcs_cmd_stretch0,
    imcs_cmd_stretch0_int64 = imcs_cmd_stretch0,
    imcs_cmd_asof_join_int32 = imcs_cmd_asof_join,
    imcs_cmd_asof_join_int64 = imcs_cmd_asof_join,
    imcs_cmd_join_int32 = imcs_cmd_join,
    imcs_cmd_join_int64 = imcs_cmd_join,

    imcs_cmd_int8_call = imcs_cmd_call,
    imcs_cmd_int16_call = imcs_cmd_call,
    imcs_cmd_int32_call = imcs_cmd_call,
    imcs_cmd_int64_call = imcs_cmd_call,
    imcs_cmd_float_call = imcs_cmd_call,
    imcs_cmd_double_call = imcs_cmd_call
};


/*
 * Timeseries iterator: it is used not only for iteration but for constructing pipe of different operations with sequence 
 */
typedef struct imcs_iterator_t_
{
    imcs_iterator_next_t next; /* method for obtaning next portion of values */
    imcs_iterator_reset_t reset; /* start iteration from beginning */
    imcs_iterator_prepare_t prepare; /* prepare iterator (used to start parallel processing) */
    imcs_iterator_merge_t merge; /* merge two iterators (used to merge results of parallel processing) */
    uint16 elem_size; /* size of element */
    uint16 tile_size; /* number of tile items */
    uint16 tile_offs; /* offset to first not handled tile item */ 
    uint8  rle_offs;  /* index within same RLE value */ 
    uint8  flags;     /* bitmap of imcs_flags_t flags */
    imcs_pos_t first_pos; /* first sequence number (inclusive) */
    imcs_pos_t next_pos;  /* sequence number of element returned by sudsequent invocation of next() function */
    imcs_pos_t last_pos;  /* last sequence number (inclusive) */
    struct imcs_iterator_t_* opd[3]; /*operands of sequence operator */
    imcs_elem_typeid_t elem_type; /* result element type */
    uint32 iterator_size; /* size fo iterator + tile data + context */
    imcs_timeseries_t* cs_hdr; /* header of stored timeseries, NULL for sequence iterators */
    void* context;
    imcs_tile_t tile;  /* tile of values */
} imcs_iterator_t, *imcs_iterator_h;    

void*              imcs_alloc(size_t size);
void               imcs_free(void* ptr);

imcs_timeseries_t* imcs_get_timeseries(char const* id, imcs_elem_typeid_t elem_type, bool is_timestamp, int elem_size, bool create);
imcs_page_t*       imcs_new_page(void);
void               imcs_free_page(imcs_page_t* pg);

imcs_iterator_h    imcs_new_iterator(size_t elem_size, size_t context_size);
imcs_iterator_h    imcs_clone_iterator(imcs_iterator_h iterator);
void               imcs_reset_iterator(imcs_iterator_h);

imcs_iterator_h    imcs_cast(imcs_iterator_h input, imcs_elem_typeid_t elem_type);
imcs_count_t       imcs_count(imcs_iterator_h input);

struct imcs_adt_parser_t;
typedef Datum (*imcs_adt_parse_t)(struct imcs_adt_parser_t* parser, char* value);

typedef struct imcs_adt_parser_t { 
    imcs_adt_parse_t parse;
    FmgrInfo	     proc;
    Oid              input_oid;
    Oid	             param_oid;
} imcs_adt_parser_t;

static inline imcs_iterator_h imcs_operand(imcs_iterator_h iterator) 
{
    return (iterator->opd[0] == NULL) ? imcs_clone_iterator(iterator) : iterator; /* clone leaves */
}
 

#endif
