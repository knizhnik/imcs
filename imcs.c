#include <unistd.h>
#include "imcs.h"
#include "btree.h"
#include "disk.h"
#include "func.h"
#include "smp.h"
#include "executor/spi.h"
#include "commands/trigger.h"
#include "utils/timestamp.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/nabstime.h"
#include "utils/syscache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "tsearch/ts_locale.h"
#if PG_VERSION_NUM>=90300
#include "access/htup_details.h"
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define IMCS_TRACE(cmd)  if (imcs_trace) elog(NOTICE, "IMCS command: %s", imcs_command_mnem[imcs_cmd_##cmd]); imcs_command_profile[imcs_cmd_##cmd] += 1

#define MAX_NUMELEM_LEN 32
#define OUTPUT_BUF_RESERVE 8

typedef struct imcs_free_page_t 
{ 
    struct imcs_free_page_t* next;
} imcs_free_page_t;

typedef struct imcs_state_t
{
	LWLockId	lock;	/* protects timeseries search/modification */
    imcs_free_page_t* free_pages; /* list of free B-Tree pages */
    size_t n_used_pages;
    imcs_disk_cache_t disk_cache;
} imcs_state_t;

typedef enum
{
    LOCK_NONE,
    LOCK_SHARED,
    LOCK_EXCLUSIVE
} imcs_lock_t;

static imcs_state_t* imcs;
static HTAB* imcs_hash;
static HTAB* imcs_dict;
static imcs_thread_pool_t* imcs_thread_pool;
static imcs_lock_t imcs_lock = LOCK_NONE;
static imcs_mutex_t* imcs_alloc_mutex;
static MemoryContext imcs_mem_ctx;
static imcs_tls_t* imcs_tls;
static imcs_error_handler_t* imcs_error_handlers;

static Datum  imcs_project_result_cache;
static size_t imcs_project_redundant_calls;
static size_t imcs_project_call_count;
static bool   imcs_project_caching = true;
static bool   imcs_substitute_nulls = false;
static bool   imcs_autoload = true;
static bool   imcs_serializable = true;
static bool   imcs_trace = false;

int imcs_cache_size = 0;
char* imcs_file_path;

int imcs_page_size = 4096;
int imcs_tile_size = 128;
int imcs_dict_size = IMCS_SMALL_DICTIONARY;

bool imcs_use_rle = false;
static int imcs_output_string_limit = 1024;
static bool imcs_flush_file;
static int shmem_size = 1024;
static int n_timeseries = 10000;
static int n_threads = 0;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static int imcs_command_profile[imcs_cmd_last_command];

const Oid imcs_elem_type_to_oid[] = {CHAROID, INT2OID, INT4OID, DATEOID, INT8OID, TIMEOID, TIMESTAMPOID, CASHOID, FLOAT4OID, FLOAT8OID, BPCHAROID};

const char* const imcs_type_mnems[] = {"char", "int2", "int4", "date", "int8", "time", "timestamp", "money", "float4", "float8", "bpchar"};
static const int imcs_type_mnem_lens[] = {4, 4, 4, 4, 4, 4, 8, 5, 6, 6, 6};

static const int imcs_type_sizeof[] = {1,2,4,4,8,8,8,8,4,8,0};

static const char* const imcs_command_mnem[] = 
{
    "parse",
    "const",
    "cast",
    "add", 
    "mul", 
    "sub", 
    "div", 
    "mod", 
    "pow", 
    "and", 
    "or", 
    "xor", 
    "concat", 
    "cat", 
    "eq", 
    "ne", 
    "ge", 
    "le", 
    "lt", 
    "gt", 
    "maxof", 
    "minof", 
    "neg", 
    "not", 
    "bit_not", 
    "abs", 
    "limit", 
    "sin", 
    "cos", 
    "tan", 
    "exp", 
    "asin", 
    "acos", 
    "atan", 
    "sqrt", 
    "log", 
    "ceil", 
    "floor", 
    "isnan", 
    "wsum", 
    "wavg", 
    "corr", 
    "cov", 
    "norm", 
    "thin", 
    "iif", 
    "if", 
    "filter", 
    "filter_pos", 
    "filter_first_pos", 
    "unique", 
    "reverse", 
    "diff", 
    "trend", 
    "repeat", 
    "count", 
    "approxdc", 
    "max", 
    "min", 
    "avg", 
    "sum", 
    "prd", 
    "var", 
    "dev", 
    "all", 
    "any", 
    "median", 
    "group_count", 
    "group_approxdc", 
    "group_max", 
    "group_min", 
    "group_avg", 
    "group_sum", 
    "group_var", 
    "group_dev", 
    "group_all", 
    "group_any", 
    "group_last", 
    "group_first", 
    "grid_max", 
    "grid_min", 
    "grid_avg", 
    "grid_sum", 
    "grid_var", 
    "grid_dev", 
    "window_max", 
    "window_min", 
    "window_avg", 
    "window_sum", 
    "window_var", 
    "window_dev", 
    "window_ema", 
    "window_atr", 
    "hash_count", 
    "hash_dup_count", 
    "hash_max", 
    "hash_min", 
    "hash_avg", 
    "hash_sum", 
    "hash_all", 
    "hash_any", 
    "top_max", 
    "top_min", 
    "top_max_pos", 
    "top_min_pos", 
    "cum_max", 
    "cum_min", 
    "cum_avg", 
    "cum_sum", 
    "cum_prd", 
    "cum_var", 
    "cum_dev", 
    "histogram", 
    "cross", 
    "extrema", 
    "stretch", 
    "stretch0", 
    "asof_join", 
    "asof_join_pos", 
    "join", 
    "join_pos", 
    "map", 
    "union", 
    "empty", 
    "project", 
    "project_agg", 
    "year", 
    "month", 
    "mday", 
    "wday", 
    "hour", 
    "minute", 
    "second", 
    "week", 
    "quarter", 
    "call", 
    "to_array", 
    "from_array",
    "like",
    "ilike",
    "sort",
    "sort_pos",
    "rank",
    "dense_rank",
    "quantile"
};

#define MB (1024*1024)
#define MAX_SQL_STMT_LEN 256
#define MAX_CUT_VALUES 16
#define IMCS_INIT_OUTPUT_BUF_SIZE (16*1024)
#define IMCS_MIN_OUTPUT_BUF_SIZE  (MAX_NUMELEM_LEN)

typedef struct { 
    char* id;
} imcs_hash_key_t;

typedef struct { 
    imcs_hash_key_t key;
    imcs_timeseries_t value;
} imcs_hash_entry_t;

imcs_dict_entry_t** imcs_dict_code_map;

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);


PG_FUNCTION_INFO_V1(columnar_store_initialized);
PG_FUNCTION_INFO_V1(columnar_store_lock);
PG_FUNCTION_INFO_V1(columnar_store_get);
PG_FUNCTION_INFO_V1(columnar_store_span);
PG_FUNCTION_INFO_V1(columnar_store_load);
PG_FUNCTION_INFO_V1(columnar_store_load_column);
PG_FUNCTION_INFO_V1(columnar_store_delete);
PG_FUNCTION_INFO_V1(columnar_store_truncate);
PG_FUNCTION_INFO_V1(columnar_store_insert_trigger);
PG_FUNCTION_INFO_V1(columnar_store_search_int8);
PG_FUNCTION_INFO_V1(columnar_store_search_int16);
PG_FUNCTION_INFO_V1(columnar_store_search_int32);
PG_FUNCTION_INFO_V1(columnar_store_search_int64);
PG_FUNCTION_INFO_V1(columnar_store_search_float);
PG_FUNCTION_INFO_V1(columnar_store_search_double);
PG_FUNCTION_INFO_V1(columnar_store_append_int8);
PG_FUNCTION_INFO_V1(columnar_store_append_int16);
PG_FUNCTION_INFO_V1(columnar_store_append_int32);
PG_FUNCTION_INFO_V1(columnar_store_append_int64);
PG_FUNCTION_INFO_V1(columnar_store_append_float);
PG_FUNCTION_INFO_V1(columnar_store_append_double);
PG_FUNCTION_INFO_V1(columnar_store_append_char);
PG_FUNCTION_INFO_V1(columnar_store_count);
PG_FUNCTION_INFO_V1(columnar_store_first_int8);
PG_FUNCTION_INFO_V1(columnar_store_first_int16);
PG_FUNCTION_INFO_V1(columnar_store_first_int32);
PG_FUNCTION_INFO_V1(columnar_store_first_int64);
PG_FUNCTION_INFO_V1(columnar_store_first_float);
PG_FUNCTION_INFO_V1(columnar_store_first_double);
PG_FUNCTION_INFO_V1(columnar_store_last_int8);
PG_FUNCTION_INFO_V1(columnar_store_last_int16);
PG_FUNCTION_INFO_V1(columnar_store_last_int32);
PG_FUNCTION_INFO_V1(columnar_store_last_int64);
PG_FUNCTION_INFO_V1(columnar_store_last_float);
PG_FUNCTION_INFO_V1(columnar_store_last_double);
PG_FUNCTION_INFO_V1(columnar_store_join_int8);
PG_FUNCTION_INFO_V1(columnar_store_join_int16);
PG_FUNCTION_INFO_V1(columnar_store_join_int32);
PG_FUNCTION_INFO_V1(columnar_store_join_int64);
PG_FUNCTION_INFO_V1(columnar_store_join_float);
PG_FUNCTION_INFO_V1(columnar_store_join_double);
PG_FUNCTION_INFO_V1(cs_used_memory);
PG_FUNCTION_INFO_V1(cs_delete_all);
PG_FUNCTION_INFO_V1(cs_parse_tid);
PG_FUNCTION_INFO_V1(cs_const_num);
PG_FUNCTION_INFO_V1(cs_const_dt);
PG_FUNCTION_INFO_V1(cs_const_str);
PG_FUNCTION_INFO_V1(cs_cast_tid);
PG_FUNCTION_INFO_V1(cs_type);
PG_FUNCTION_INFO_V1(cs_elem_size);
PG_FUNCTION_INFO_V1(cs_input_function);
PG_FUNCTION_INFO_V1(cs_output_function);
PG_FUNCTION_INFO_V1(cs_receive_function);
PG_FUNCTION_INFO_V1(cs_send_function);
PG_FUNCTION_INFO_V1(cs_add);
PG_FUNCTION_INFO_V1(cs_mul);
PG_FUNCTION_INFO_V1(cs_sub);
PG_FUNCTION_INFO_V1(cs_div);
PG_FUNCTION_INFO_V1(cs_mod);
PG_FUNCTION_INFO_V1(cs_pow);
PG_FUNCTION_INFO_V1(cs_and);
PG_FUNCTION_INFO_V1(cs_or);
PG_FUNCTION_INFO_V1(cs_xor);
PG_FUNCTION_INFO_V1(cs_concat);
PG_FUNCTION_INFO_V1(cs_cat);
PG_FUNCTION_INFO_V1(cs_cut);
PG_FUNCTION_INFO_V1(cs_as);
PG_FUNCTION_INFO_V1(cs_as_array);
PG_FUNCTION_INFO_V1(cs_eq);
PG_FUNCTION_INFO_V1(cs_ne);
PG_FUNCTION_INFO_V1(cs_ge);
PG_FUNCTION_INFO_V1(cs_le);
PG_FUNCTION_INFO_V1(cs_lt);
PG_FUNCTION_INFO_V1(cs_gt);
PG_FUNCTION_INFO_V1(cs_like);
PG_FUNCTION_INFO_V1(cs_ilike);
PG_FUNCTION_INFO_V1(cs_maxof);
PG_FUNCTION_INFO_V1(cs_minof);
PG_FUNCTION_INFO_V1(cs_neg);
PG_FUNCTION_INFO_V1(cs_not);
PG_FUNCTION_INFO_V1(cs_bit_not);
PG_FUNCTION_INFO_V1(cs_abs);
PG_FUNCTION_INFO_V1(cs_limit);
PG_FUNCTION_INFO_V1(cs_sin);
PG_FUNCTION_INFO_V1(cs_cos);
PG_FUNCTION_INFO_V1(cs_tan);
PG_FUNCTION_INFO_V1(cs_exp);
PG_FUNCTION_INFO_V1(cs_asin);
PG_FUNCTION_INFO_V1(cs_acos);
PG_FUNCTION_INFO_V1(cs_atan);
PG_FUNCTION_INFO_V1(cs_sqrt);
PG_FUNCTION_INFO_V1(cs_log);
PG_FUNCTION_INFO_V1(cs_ceil);
PG_FUNCTION_INFO_V1(cs_floor);
PG_FUNCTION_INFO_V1(cs_isnan);
PG_FUNCTION_INFO_V1(cs_wsum);
PG_FUNCTION_INFO_V1(cs_wavg);
PG_FUNCTION_INFO_V1(cs_corr);
PG_FUNCTION_INFO_V1(cs_cov);
PG_FUNCTION_INFO_V1(cs_norm);
PG_FUNCTION_INFO_V1(cs_thin);
PG_FUNCTION_INFO_V1(cs_iif);
PG_FUNCTION_INFO_V1(cs_if);
PG_FUNCTION_INFO_V1(cs_filter);
PG_FUNCTION_INFO_V1(cs_filter_pos);
PG_FUNCTION_INFO_V1(cs_filter_first_pos);
PG_FUNCTION_INFO_V1(cs_unique);
PG_FUNCTION_INFO_V1(cs_reverse);
PG_FUNCTION_INFO_V1(cs_diff);
PG_FUNCTION_INFO_V1(cs_trend);
PG_FUNCTION_INFO_V1(cs_repeat);
PG_FUNCTION_INFO_V1(cs_count);
PG_FUNCTION_INFO_V1(cs_approxdc);
PG_FUNCTION_INFO_V1(cs_max);
PG_FUNCTION_INFO_V1(cs_min);
PG_FUNCTION_INFO_V1(cs_avg);
PG_FUNCTION_INFO_V1(cs_sum);
PG_FUNCTION_INFO_V1(cs_any);
PG_FUNCTION_INFO_V1(cs_all);
PG_FUNCTION_INFO_V1(cs_prd);
PG_FUNCTION_INFO_V1(cs_var);
PG_FUNCTION_INFO_V1(cs_dev);
PG_FUNCTION_INFO_V1(cs_median);
PG_FUNCTION_INFO_V1(cs_group_count);
PG_FUNCTION_INFO_V1(cs_group_approxdc);
PG_FUNCTION_INFO_V1(cs_group_max);
PG_FUNCTION_INFO_V1(cs_group_min);
PG_FUNCTION_INFO_V1(cs_group_avg);
PG_FUNCTION_INFO_V1(cs_group_sum);
PG_FUNCTION_INFO_V1(cs_group_var);
PG_FUNCTION_INFO_V1(cs_group_dev);
PG_FUNCTION_INFO_V1(cs_group_any);
PG_FUNCTION_INFO_V1(cs_group_all);
PG_FUNCTION_INFO_V1(cs_group_last);
PG_FUNCTION_INFO_V1(cs_group_first);
PG_FUNCTION_INFO_V1(cs_grid_max);
PG_FUNCTION_INFO_V1(cs_grid_min);
PG_FUNCTION_INFO_V1(cs_grid_avg);
PG_FUNCTION_INFO_V1(cs_grid_sum);
PG_FUNCTION_INFO_V1(cs_grid_var);
PG_FUNCTION_INFO_V1(cs_grid_dev);
PG_FUNCTION_INFO_V1(cs_window_max);
PG_FUNCTION_INFO_V1(cs_window_min);
PG_FUNCTION_INFO_V1(cs_window_avg);
PG_FUNCTION_INFO_V1(cs_window_sum);
PG_FUNCTION_INFO_V1(cs_window_var);
PG_FUNCTION_INFO_V1(cs_window_dev);
PG_FUNCTION_INFO_V1(cs_window_ema);
PG_FUNCTION_INFO_V1(cs_window_atr);
PG_FUNCTION_INFO_V1(cs_hash_count);
PG_FUNCTION_INFO_V1(cs_hash_dup_count);
PG_FUNCTION_INFO_V1(cs_hash_max);
PG_FUNCTION_INFO_V1(cs_hash_min);
PG_FUNCTION_INFO_V1(cs_hash_avg);
PG_FUNCTION_INFO_V1(cs_hash_sum);
PG_FUNCTION_INFO_V1(cs_hash_any);
PG_FUNCTION_INFO_V1(cs_hash_all);
PG_FUNCTION_INFO_V1(cs_top_max);
PG_FUNCTION_INFO_V1(cs_top_min);
PG_FUNCTION_INFO_V1(cs_top_max_pos);
PG_FUNCTION_INFO_V1(cs_top_min_pos);
PG_FUNCTION_INFO_V1(cs_cum_max);
PG_FUNCTION_INFO_V1(cs_cum_min);
PG_FUNCTION_INFO_V1(cs_cum_avg);
PG_FUNCTION_INFO_V1(cs_cum_sum);
PG_FUNCTION_INFO_V1(cs_cum_prd);
PG_FUNCTION_INFO_V1(cs_cum_var);
PG_FUNCTION_INFO_V1(cs_cum_dev);
PG_FUNCTION_INFO_V1(cs_histogram);
PG_FUNCTION_INFO_V1(cs_cross);
PG_FUNCTION_INFO_V1(cs_extrema);
PG_FUNCTION_INFO_V1(cs_stretch);
PG_FUNCTION_INFO_V1(cs_stretch0);
PG_FUNCTION_INFO_V1(cs_asof_join);
PG_FUNCTION_INFO_V1(cs_asof_join_pos);
PG_FUNCTION_INFO_V1(cs_join);
PG_FUNCTION_INFO_V1(cs_join_pos);
PG_FUNCTION_INFO_V1(cs_map);
PG_FUNCTION_INFO_V1(cs_union);
PG_FUNCTION_INFO_V1(cs_empty);
PG_FUNCTION_INFO_V1(cs_project);
PG_FUNCTION_INFO_V1(cs_project_agg);
PG_FUNCTION_INFO_V1(cs_year);
PG_FUNCTION_INFO_V1(cs_month);
PG_FUNCTION_INFO_V1(cs_mday);
PG_FUNCTION_INFO_V1(cs_wday);
PG_FUNCTION_INFO_V1(cs_hour);
PG_FUNCTION_INFO_V1(cs_minute);
PG_FUNCTION_INFO_V1(cs_second);
PG_FUNCTION_INFO_V1(cs_week);
PG_FUNCTION_INFO_V1(cs_quarter);
PG_FUNCTION_INFO_V1(cs_call);
PG_FUNCTION_INFO_V1(cs_to_array);
PG_FUNCTION_INFO_V1(cs_from_array);
PG_FUNCTION_INFO_V1(cs_profile);
PG_FUNCTION_INFO_V1(cs_sort);
PG_FUNCTION_INFO_V1(cs_sort_pos);
PG_FUNCTION_INFO_V1(cs_rank);
PG_FUNCTION_INFO_V1(cs_dense_rank);
PG_FUNCTION_INFO_V1(cs_quantile);
PG_FUNCTION_INFO_V1(cs_str2code);
PG_FUNCTION_INFO_V1(cs_code2str);
PG_FUNCTION_INFO_V1(cs_cut_and_code2str);
PG_FUNCTION_INFO_V1(cs_dictionary_size);


Datum columnar_store_initialized(PG_FUNCTION_ARGS);
Datum columnar_store_get(PG_FUNCTION_ARGS);
Datum columnar_store_lock(PG_FUNCTION_ARGS);
Datum columnar_store_span(PG_FUNCTION_ARGS);
Datum columnar_store_load(PG_FUNCTION_ARGS);
Datum columnar_store_load_column(PG_FUNCTION_ARGS);
Datum columnar_store_delete(PG_FUNCTION_ARGS);
Datum columnar_store_truncate(PG_FUNCTION_ARGS);
Datum columnar_store_insert_trigger(PG_FUNCTION_ARGS);
Datum columnar_store_search_int8(PG_FUNCTION_ARGS);
Datum columnar_store_search_int16(PG_FUNCTION_ARGS);
Datum columnar_store_search_int32(PG_FUNCTION_ARGS);
Datum columnar_store_search_int64(PG_FUNCTION_ARGS);
Datum columnar_store_search_float(PG_FUNCTION_ARGS);
Datum columnar_store_search_double(PG_FUNCTION_ARGS);
Datum columnar_store_append_int8(PG_FUNCTION_ARGS);
Datum columnar_store_append_int16(PG_FUNCTION_ARGS);
Datum columnar_store_append_int32(PG_FUNCTION_ARGS);
Datum columnar_store_append_int64(PG_FUNCTION_ARGS);
Datum columnar_store_append_float(PG_FUNCTION_ARGS);
Datum columnar_store_append_double(PG_FUNCTION_ARGS);
Datum columnar_store_append_char(PG_FUNCTION_ARGS);
Datum columnar_store_count(PG_FUNCTION_ARGS);
Datum columnar_store_first_int8(PG_FUNCTION_ARGS);
Datum columnar_store_first_int16(PG_FUNCTION_ARGS);
Datum columnar_store_first_int32(PG_FUNCTION_ARGS);
Datum columnar_store_first_int64(PG_FUNCTION_ARGS);
Datum columnar_store_first_float(PG_FUNCTION_ARGS);
Datum columnar_store_first_double(PG_FUNCTION_ARGS);
Datum columnar_store_last_int8(PG_FUNCTION_ARGS);
Datum columnar_store_last_int16(PG_FUNCTION_ARGS);
Datum columnar_store_last_int32(PG_FUNCTION_ARGS);
Datum columnar_store_last_int64(PG_FUNCTION_ARGS);
Datum columnar_store_last_float(PG_FUNCTION_ARGS);
Datum columnar_store_last_double(PG_FUNCTION_ARGS);
Datum columnar_store_join_int8(PG_FUNCTION_ARGS);
Datum columnar_store_join_int16(PG_FUNCTION_ARGS);
Datum columnar_store_join_int32(PG_FUNCTION_ARGS);
Datum columnar_store_join_int64(PG_FUNCTION_ARGS);
Datum columnar_store_join_float(PG_FUNCTION_ARGS);
Datum columnar_store_join_double(PG_FUNCTION_ARGS);
Datum cs_used_memory(PG_FUNCTION_ARGS);
Datum cs_delete_all(PG_FUNCTION_ARGS);
Datum cs_parse_tid(PG_FUNCTION_ARGS);
Datum cs_const_num(PG_FUNCTION_ARGS);
Datum cs_const_dt(PG_FUNCTION_ARGS);
Datum cs_const_str(PG_FUNCTION_ARGS);
Datum cs_cast_tid(PG_FUNCTION_ARGS);
Datum cs_type(PG_FUNCTION_ARGS);
Datum cs_elem_size(PG_FUNCTION_ARGS);
Datum cs_input_function(PG_FUNCTION_ARGS);
Datum cs_output_function(PG_FUNCTION_ARGS);
Datum cs_receive_function(PG_FUNCTION_ARGS);
Datum cs_send_function(PG_FUNCTION_ARGS);
Datum cs_add(PG_FUNCTION_ARGS);
Datum cs_mul(PG_FUNCTION_ARGS);
Datum cs_sub(PG_FUNCTION_ARGS);
Datum cs_div(PG_FUNCTION_ARGS);
Datum cs_mod(PG_FUNCTION_ARGS);
Datum cs_pow(PG_FUNCTION_ARGS);
Datum cs_and(PG_FUNCTION_ARGS);
Datum cs_or(PG_FUNCTION_ARGS);
Datum cs_xor(PG_FUNCTION_ARGS);
Datum cs_concat(PG_FUNCTION_ARGS);
Datum cs_cat(PG_FUNCTION_ARGS);
Datum cs_cut(PG_FUNCTION_ARGS);
Datum cs_as(PG_FUNCTION_ARGS);
Datum cs_as_array(PG_FUNCTION_ARGS);
Datum cs_eq(PG_FUNCTION_ARGS);
Datum cs_ne(PG_FUNCTION_ARGS);
Datum cs_ge(PG_FUNCTION_ARGS);
Datum cs_le(PG_FUNCTION_ARGS);
Datum cs_lt(PG_FUNCTION_ARGS);
Datum cs_gt(PG_FUNCTION_ARGS);
Datum cs_like(PG_FUNCTION_ARGS);
Datum cs_ilike(PG_FUNCTION_ARGS);
Datum cs_maxof(PG_FUNCTION_ARGS);
Datum cs_minof(PG_FUNCTION_ARGS);
Datum cs_neg(PG_FUNCTION_ARGS);
Datum cs_not(PG_FUNCTION_ARGS);
Datum cs_bit_not(PG_FUNCTION_ARGS);
Datum cs_abs(PG_FUNCTION_ARGS);
Datum cs_limit(PG_FUNCTION_ARGS);
Datum cs_sin(PG_FUNCTION_ARGS);
Datum cs_cos(PG_FUNCTION_ARGS);
Datum cs_tan(PG_FUNCTION_ARGS);
Datum cs_exp(PG_FUNCTION_ARGS);
Datum cs_asin(PG_FUNCTION_ARGS);
Datum cs_acos(PG_FUNCTION_ARGS);
Datum cs_atan(PG_FUNCTION_ARGS);
Datum cs_sqrt(PG_FUNCTION_ARGS);
Datum cs_log(PG_FUNCTION_ARGS);
Datum cs_ceil(PG_FUNCTION_ARGS);
Datum cs_floor(PG_FUNCTION_ARGS);
Datum cs_isnan(PG_FUNCTION_ARGS);
Datum cs_wsum(PG_FUNCTION_ARGS);
Datum cs_wavg(PG_FUNCTION_ARGS);
Datum cs_corr(PG_FUNCTION_ARGS);
Datum cs_cov(PG_FUNCTION_ARGS);
Datum cs_norm(PG_FUNCTION_ARGS);
Datum cs_thin(PG_FUNCTION_ARGS);
Datum cs_iif(PG_FUNCTION_ARGS);
Datum cs_if(PG_FUNCTION_ARGS);
Datum cs_filter(PG_FUNCTION_ARGS);
Datum cs_filter_pos(PG_FUNCTION_ARGS);
Datum cs_filter_first_pos(PG_FUNCTION_ARGS);
Datum cs_unique(PG_FUNCTION_ARGS);
Datum cs_reverse(PG_FUNCTION_ARGS);
Datum cs_diff(PG_FUNCTION_ARGS);
Datum cs_trend(PG_FUNCTION_ARGS);
Datum cs_repeat(PG_FUNCTION_ARGS);
Datum cs_count(PG_FUNCTION_ARGS);
Datum cs_approxdc(PG_FUNCTION_ARGS);
Datum cs_max(PG_FUNCTION_ARGS);
Datum cs_min(PG_FUNCTION_ARGS);
Datum cs_avg(PG_FUNCTION_ARGS);
Datum cs_sum(PG_FUNCTION_ARGS);
Datum cs_any(PG_FUNCTION_ARGS);
Datum cs_all(PG_FUNCTION_ARGS);
Datum cs_prd(PG_FUNCTION_ARGS);
Datum cs_var(PG_FUNCTION_ARGS);
Datum cs_dev(PG_FUNCTION_ARGS);
Datum cs_median(PG_FUNCTION_ARGS);
Datum cs_group_count(PG_FUNCTION_ARGS);
Datum cs_group_approxdc(PG_FUNCTION_ARGS);
Datum cs_group_max(PG_FUNCTION_ARGS);
Datum cs_group_min(PG_FUNCTION_ARGS);
Datum cs_group_avg(PG_FUNCTION_ARGS);
Datum cs_group_sum(PG_FUNCTION_ARGS);
Datum cs_group_any(PG_FUNCTION_ARGS);
Datum cs_group_all(PG_FUNCTION_ARGS);
Datum cs_group_var(PG_FUNCTION_ARGS);
Datum cs_group_dev(PG_FUNCTION_ARGS);
Datum cs_group_last(PG_FUNCTION_ARGS);
Datum cs_group_first(PG_FUNCTION_ARGS);
Datum cs_grid_max(PG_FUNCTION_ARGS);
Datum cs_grid_min(PG_FUNCTION_ARGS);
Datum cs_grid_avg(PG_FUNCTION_ARGS);
Datum cs_grid_sum(PG_FUNCTION_ARGS);
Datum cs_grid_var(PG_FUNCTION_ARGS);
Datum cs_grid_dev(PG_FUNCTION_ARGS);
Datum cs_window_max(PG_FUNCTION_ARGS);
Datum cs_window_min(PG_FUNCTION_ARGS);
Datum cs_window_avg(PG_FUNCTION_ARGS);
Datum cs_window_sum(PG_FUNCTION_ARGS);
Datum cs_window_var(PG_FUNCTION_ARGS);
Datum cs_window_dev(PG_FUNCTION_ARGS);
Datum cs_window_ema(PG_FUNCTION_ARGS);
Datum cs_window_atr(PG_FUNCTION_ARGS);
Datum cs_hash_count(PG_FUNCTION_ARGS);
Datum cs_hash_dup_count(PG_FUNCTION_ARGS);
Datum cs_hash_max(PG_FUNCTION_ARGS);
Datum cs_hash_min(PG_FUNCTION_ARGS);
Datum cs_hash_avg(PG_FUNCTION_ARGS);
Datum cs_hash_sum(PG_FUNCTION_ARGS);
Datum cs_hash_any(PG_FUNCTION_ARGS);
Datum cs_hash_all(PG_FUNCTION_ARGS);
Datum cs_top_max(PG_FUNCTION_ARGS);
Datum cs_top_min(PG_FUNCTION_ARGS);
Datum cs_top_max_pos(PG_FUNCTION_ARGS);
Datum cs_top_min_pos(PG_FUNCTION_ARGS);
Datum cs_cum_max(PG_FUNCTION_ARGS);
Datum cs_cum_min(PG_FUNCTION_ARGS);
Datum cs_cum_avg(PG_FUNCTION_ARGS);
Datum cs_cum_sum(PG_FUNCTION_ARGS);
Datum cs_cum_prd(PG_FUNCTION_ARGS);
Datum cs_cum_var(PG_FUNCTION_ARGS);
Datum cs_cum_dev(PG_FUNCTION_ARGS);
Datum cs_histogram(PG_FUNCTION_ARGS);
Datum cs_cross(PG_FUNCTION_ARGS);
Datum cs_extrema(PG_FUNCTION_ARGS);
Datum cs_stretch(PG_FUNCTION_ARGS);
Datum cs_stretch0(PG_FUNCTION_ARGS);
Datum cs_asof_join(PG_FUNCTION_ARGS);
Datum cs_asof_join_pos(PG_FUNCTION_ARGS);
Datum cs_join(PG_FUNCTION_ARGS);
Datum cs_join_pos(PG_FUNCTION_ARGS);
Datum cs_map(PG_FUNCTION_ARGS);
Datum cs_union(PG_FUNCTION_ARGS);
Datum cs_empty(PG_FUNCTION_ARGS);
Datum cs_project(PG_FUNCTION_ARGS);
Datum cs_project_agg(PG_FUNCTION_ARGS);
Datum cs_year(PG_FUNCTION_ARGS);
Datum cs_month(PG_FUNCTION_ARGS);
Datum cs_mday(PG_FUNCTION_ARGS);
Datum cs_wday(PG_FUNCTION_ARGS);
Datum cs_hour(PG_FUNCTION_ARGS);
Datum cs_minute(PG_FUNCTION_ARGS);
Datum cs_second(PG_FUNCTION_ARGS);
Datum cs_week(PG_FUNCTION_ARGS);
Datum cs_quarter(PG_FUNCTION_ARGS);
Datum cs_call(PG_FUNCTION_ARGS);
Datum cs_to_array(PG_FUNCTION_ARGS);
Datum cs_from_array(PG_FUNCTION_ARGS);
Datum cs_profile(PG_FUNCTION_ARGS);
Datum cs_sort(PG_FUNCTION_ARGS);
Datum cs_sort_pos(PG_FUNCTION_ARGS);
Datum cs_rank(PG_FUNCTION_ARGS);
Datum cs_dense_rank(PG_FUNCTION_ARGS);
Datum cs_quantile(PG_FUNCTION_ARGS);
Datum cs_str2code(PG_FUNCTION_ARGS);
Datum cs_code2str(PG_FUNCTION_ARGS);
Datum cs_cut_and_code2str(PG_FUNCTION_ARGS);
Datum cs_dictionary_size(PG_FUNCTION_ARGS);

void imcs_ereport(int err_code, char const* err_msg,...)
{
    static char err_buf[IMCS_MAX_ERROR_MSG_LEN];
    va_list args;
    imcs_error_handler_t* hnd = imcs_tls != NULL ? (imcs_error_handler_t*)imcs_tls->get(imcs_tls) : NULL;
    va_start(args, err_msg);
    if (hnd != NULL) { 
        vsprintf(hnd->err_msg, err_msg, args);
        va_end(args);
        hnd->err_code = err_code;
        longjmp(hnd->unwind_buf, 1);
    } else {
        vsprintf(err_buf, err_msg, args);
        va_end(args);
        ereport(ERROR, (errcode(err_code), errmsg(err_buf)));
    }
}

static void imcs_shmem_startup(void);

static uint32 imcs_hash_fn(const void *key, Size keysize)
{
	char const* id = ((imcs_hash_key_t*)key)->id;
    uint32 h = 0;
    while (*id != 0) { 
        h = h*31 + *id++;
    }
    return h;
}

static int imcs_match_fn(const void *key1, const void *key2, Size keysize)
{
    return strcmp(((imcs_hash_key_t*)key1)->id, ((imcs_hash_key_t*)key2)->id);
}

static void* imcs_keycopy_fn(void *dest, const void *src, Size keysize)
{ 
    imcs_hash_key_t* dk = (imcs_hash_key_t*)dest;
    imcs_hash_key_t* sk = (imcs_hash_key_t*)src;
    dk->id = (char*)ShmemAlloc(strlen(sk->id)+1);
    if (dk->id == NULL) { 
        imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory for hash entry");
    }
    strcpy(dk->id, sk->id);
    return dk;
}



static void imcs_init_hash() 
{
	static HASHCTL info;
	info.keysize = sizeof(imcs_hash_key_t);
	info.entrysize = sizeof(imcs_hash_entry_t);
	info.hash = imcs_hash_fn;
	info.match = imcs_match_fn;
	info.keycopy = imcs_keycopy_fn;
	imcs_hash = ShmemInitHash("imcs hash",
							  n_timeseries, n_timeseries*10,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_KEYCOPY);
}

static uint32 imcs_dict_hash_fn(const void *key, Size keysize)
{
    imcs_dict_key_t const* dk = (imcs_dict_key_t const*)key;
    char* val = dk->val;
    size_t len = dk->len;
    size_t i;
    uint32 h = 0;
    for (i = 0; i < len; i++) { 
        h = h*31 + val[i];
    }
    return h;
}

static int imcs_dict_match_fn(const void *key1, const void *key2, Size keysize)
{
    imcs_dict_key_t const* dk1 = (imcs_dict_key_t const*)key1;
    imcs_dict_key_t const* dk2 = (imcs_dict_key_t const*)key2;
    return dk1->len == dk2->len ? memcmp(dk1->val, dk2->val, dk1->len) : dk1->len - dk2->len;
}

static void* imcs_dict_keycopy_fn(void *dest, const void *src, Size keysize)
{ 
    imcs_dict_key_t* dk = (imcs_dict_key_t*)dest;
    imcs_dict_key_t* sk = (imcs_dict_key_t*)src;
    dk->val = (char*)ShmemAlloc(sk->len);
    if (dk->val == NULL) { 
        imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory for dictionary entry");
    }
    dk->len = sk->len;
    memcpy(dk->val, sk->val, sk->len);
    return dk;
}



static void imcs_init_dict() 
{
    if (imcs_dict_size != 0) { 
        static HASHCTL info;
        info.keysize = sizeof(imcs_dict_key_t);
        info.entrysize = sizeof(imcs_dict_entry_t);
        info.hash = imcs_dict_hash_fn;
        info.match = imcs_dict_match_fn;
        info.keycopy = imcs_dict_keycopy_fn;
        imcs_dict = ShmemInitHash("imcs dictionary",
                                  imcs_dict_size, imcs_dict_size,
                                  &info,
                                  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_KEYCOPY);
        imcs_dict_code_map = (imcs_dict_entry_t**)ShmemAlloc(imcs_dict_size*sizeof(imcs_dict_entry_t*));
        if (imcs_dict_code_map == NULL) { 
            imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory for dictionary map");
        }
    }
}

static void imcs_executor_end(QueryDesc *queryDesc)
{
    if (CurrentMemoryContext == TopTransactionContext) { 
        imcs_project_redundant_calls = 0;
        imcs_project_call_count = 0;
        if (!imcs_serializable && imcs && imcs_lock != LOCK_NONE) { 
            if (LWLockHeldByMe(imcs->lock)) { 
                LWLockRelease(imcs->lock);
            }
            imcs_lock = LOCK_NONE;
        }
        if (imcs_mem_ctx) {
            MemoryContextReset(imcs_mem_ctx);
        }     
    }
    if (prev_ExecutorEnd) { 
		prev_ExecutorEnd(queryDesc);
    } else { 
		standard_ExecutorEnd(queryDesc);
    }    
}


static void imcs_trans_callback(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT) { 
        imcs_project_redundant_calls = 0;
        imcs_project_call_count = 0;
        if (imcs && imcs_lock != LOCK_NONE) { 
            if (LWLockHeldByMe(imcs->lock)) { 
                LWLockRelease(imcs->lock);
            }
            imcs_lock = LOCK_NONE;
        }
        if (imcs_mem_ctx) {
            MemoryContextReset(imcs_mem_ctx);
        }
        if (event == XACT_EVENT_COMMIT && imcs_flush_file) { 
            imcs_disk_flush();
        }
    }
}


imcs_timeseries_t* imcs_get_timeseries(char const* id, imcs_elem_typeid_t elem_type, bool is_timestamp, int elem_size, bool create)
{
	imcs_timeseries_t* ts;
    imcs_hash_entry_t* entry;
    imcs_hash_key_t key;
	bool found;
    int autoload_attempts = imcs_autoload ? 2 : 0;

    if (id == NULL) { 
        return NULL;
    }
    if (imcs == NULL) { 
        imcs_ereport(ERRCODE_LOCK_NOT_AVAILABLE, "Columnar store was not properly initialized, please check that imcs plugin was added to shared_preload_libraries list");
    }
    if (create) { 
        if (imcs_lock != LOCK_EXCLUSIVE) { 
            if (imcs_lock != LOCK_NONE) { 
                LWLockRelease(imcs->lock);
            }
            LWLockAcquire(imcs->lock, LW_EXCLUSIVE);
            imcs_lock = LOCK_EXCLUSIVE;
        } 
    } else if (imcs_lock == LOCK_NONE) {         
        LWLockAcquire(imcs->lock, LW_SHARED);  
        imcs_lock = LOCK_SHARED;
    }                              
  Retry:                                     
    key.id = (char*)id;
    entry = (imcs_hash_entry_t*)hash_search(imcs_hash, &key, HASH_FIND, NULL);
    if (entry == NULL) { 
        if (!create) { 
            if (autoload_attempts && elem_size != 0) { /* elem_size == 0 when imcs_get_timeseries is called from columnar_store_initialized */
                char const* sep = strchr(id, '-');
                if (sep != NULL) { 
                    size_t table_name_len = sep - id;
                    char* table_name = (char*)palloc(table_name_len + 1);
                    int rc;
                    memcpy(table_name, id, table_name_len);
                    table_name[table_name_len] = '\0';
                    key.id = table_name;                    
                    if (autoload_attempts == 2 && !hash_search(imcs_hash, &key, HASH_FIND, NULL)) { 
                        /* load all table */
                        char stmt[MAX_SQL_STMT_LEN];
                        SPI_connect();    
                        sprintf(stmt, "select %s_load()", table_name);
                        rc = SPI_execute(stmt, true, 0);
                        SPI_finish();
                        if (rc != SPI_OK_SELECT) { 
                            elog(ERROR, "Select failed with status %d", rc);
                        } 
                        autoload_attempts = 1;
                        goto Retry;
                    } else if (autoload_attempts == 1) { 
                        /* load particular column */
                        char stmt[MAX_SQL_STMT_LEN];
                        char* column_name = (char*)sep + 1;
                        sep = strchr(column_name, '-');
                        if (sep != NULL) { 
                            int column_name_len = sep - column_name;
                            char* buf = (char*)palloc(column_name_len + 1);
                            memcpy(buf, column_name, column_name_len);
                            column_name = buf;
                            column_name[column_name_len] = '\0';
                        }
                        SPI_connect();    
                        sprintf(stmt, "select %s_load_column('%s')", table_name, column_name);
                        rc = SPI_execute(stmt, true, 0);
                        SPI_finish();
                        if (rc != SPI_OK_SELECT) { 
                            elog(ERROR, "Select failed with status %d", rc);
                        }
                        autoload_attempts = 0; /* do not make more than one attempt of autoloading data */
                        goto Retry;                        
                    }
                }
            }
            return NULL;
        }
        /* Find or create an entry with desired hash code */
        entry = (imcs_hash_entry_t*)hash_search(imcs_hash, &key, HASH_ENTER, &found);
        ts = &entry->value;
        if (!found) { 
            /* New entry, initialize it */
            ts->root_page = NULL;
            ts->count = 0;
            ts->elem_type = elem_type;
            ts->elem_size = elem_size;
            ts->is_timestamp = is_timestamp;
        }
    } else { 
        ts = &entry->value;
    }
    if (elem_size != 0 /* elem_size == 0 when imcs_get_timeseries is called from columnar_store_initialized */
        && (ts->elem_type != elem_type ||
            ts->elem_size != elem_size ||
            ts->is_timestamp != is_timestamp))
    {
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "data format was changed"); 
    }
    return ts;
}

/* imcs_alloc can be concurrently invoked from multiple threads, so as far as MemoryContextAlloc is non reetrant we have to use mutex here 
 */
void* imcs_alloc(size_t size) 
{
    void* ptr;
    imcs_alloc_mutex->lock(imcs_alloc_mutex);
    ptr = MemoryContextAlloc(imcs_mem_ctx, size);
    imcs_alloc_mutex->unlock(imcs_alloc_mutex);
    return ptr;
}

/* align returned memory on 16-byte boundary to allow use of SSE vector instructions.
 * This memory should not be deallocated using imcs_free
 */
void* imcs_alloc_aligned(size_t size) 
{
    char* ptr;
    imcs_alloc_mutex->lock(imcs_alloc_mutex);
    ptr = (char*)MemoryContextAlloc(imcs_mem_ctx, size + 16);
    ptr += -(size_t)ptr & 15;
    imcs_alloc_mutex->unlock(imcs_alloc_mutex);
    return ptr;
}

void imcs_free(void* ptr) 
{ 
    imcs_alloc_mutex->lock(imcs_alloc_mutex);
    pfree(ptr);
    imcs_alloc_mutex->unlock(imcs_alloc_mutex);
}

/* This function is called in context protected by imcs->lock */
#ifndef IMCS_DISK_SUPPORT
uint64 imcs_used_memory(void)
{
    return imcs == NULL ? 0 : imcs->n_used_pages*imcs_page_size;
}

imcs_page_t* imcs_new_page(void)
{
    imcs_free_page_t* pg = imcs->free_pages;
    if (pg == NULL) { 
        pg = (imcs_free_page_t*)ShmemAlloc(imcs_page_size);        
        if (pg == NULL) { 
            imcs_ereport(ERRCODE_OUT_OF_MEMORY, "not enough shared memory");
        }
    } else { 
        imcs->free_pages = pg->next;
    }
    imcs->n_used_pages += 1;
    return (imcs_page_t*)pg;
}

/* This function is called in context protected by imcs->lock */
void imcs_free_page(imcs_page_t* page) 
{ 
    imcs_free_page_t* pg = (imcs_free_page_t*)page;
    pg->next = imcs->free_pages;
    imcs->free_pages = pg;
    imcs->n_used_pages -= 1;
}
#endif

void imcs_reset_iterator(imcs_iterator_h iterator)
{
    int i;
    iterator->tile_size = iterator->tile_offs = 0;  
    iterator->next_pos = iterator->first_pos;   
    for (i = 0; i < 3; i++) { 
        if (iterator->opd[i]) { 
            iterator->opd[i]->reset(iterator->opd[i]);
        }
    }
}

imcs_iterator_h imcs_new_iterator(size_t elem_size, size_t context_size)
{
    size_t tile_size = MAXALIGN(elem_size*imcs_tile_size);
    size_t iterator_size = sizeof(imcs_iterator_t) + tile_size + context_size;
    imcs_iterator_h iterator = (imcs_iterator_h)imcs_alloc_aligned(iterator_size);
    iterator->flags = 0;
    iterator->cs_hdr = NULL;
    iterator->opd[0] = NULL;
    iterator->opd[1] = NULL;
    iterator->opd[2] = NULL; 
    iterator->next_pos = 0;                                            
    iterator->first_pos = 0;                                           
    iterator->last_pos = IMCS_INFINITY;                             
    iterator->tile_size = iterator->tile_offs = 0; 
    iterator->elem_size = elem_size;
    iterator->prepare = NULL;
    iterator->merge = NULL;
    iterator->reset = imcs_reset_iterator;
    iterator->iterator_size = iterator_size;
    iterator->context = (char*)(iterator+1) +  tile_size;
    return iterator;
}

imcs_iterator_h imcs_clone_iterator(imcs_iterator_h iterator) 
{
    imcs_iterator_h clone = (imcs_iterator_h)imcs_alloc_aligned(iterator->iterator_size);
    memcpy(clone, iterator, iterator->iterator_size);
    clone->context = (char*)clone + ((char*)iterator->context - (char*)iterator);
    return clone;
}
    
/*
 * Module load callback
 */
void _PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the cs_* functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("imcs.shmem_size",
                            "Size of shared memory (Mb) used by columnar store.",
							NULL,
							&shmem_size,
							8*1024,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("imcs.n_timeseries",
                            "Estimation for number of timeseries.",
							NULL,
							&n_timeseries,
							10000,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("imcs.dictionary_size",
                            "Size of dictionary used for varying length fields.",
							NULL,
							&imcs_dict_size,
							IMCS_SMALL_DICTIONARY,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("imcs.n_threads",
                            "Number of threads for concurrent execution of query (0 - autodetect number of CPUs).",
							NULL,
							&n_threads,
							0,
							0,
							100,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("imcs.page_size",
                            "Timeseries B-Tree page size.",
							NULL,
							&imcs_page_size,
							4096,
							128,
							64*1024,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("imcs.use_rle",
                             "Use RLE compression for chararacter types.",
                             NULL,
                             &imcs_use_rle,
                             false,
                             PGC_POSTMASTER,
                             0,
                             NULL,
                             NULL,
                             NULL);
    
#ifdef IMCS_DISK_SUPPORT
	DefineCustomIntVariable("imcs.cache_size",
                            "Size of IMCS disk cache.",
							NULL,
							&imcs_cache_size,
							256*1024, /* 1Gb cache for 4kb pages */
							8,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("imcs.flush_file",
                             "Flush changes to the file during commit.",
                             NULL,
                             &imcs_flush_file,
                             true,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);
    
	DefineCustomStringVariable("imcs.file_path",
                            "Path to IMCS disk file or partition.",
							NULL,
							&imcs_file_path,
							"imcs.dbs",
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
#endif

	DefineCustomIntVariable("imcs.tile_size",
                            "Number of elements in tile.",
							NULL,
							&imcs_tile_size,
#ifdef IMCS_DISK_SUPPORT
							1024, /* use large tiles for disk mode to minize number of page accesses */
#else
							128,
#endif
							1,
							10000,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("imcs.project_caching",
                             "Caches cs_project results to avoid redundant calculations in (cs_project(...)).* expression.",
                             NULL,
                             &imcs_project_caching,
                             true,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

	DefineCustomBoolVariable("imcs.substitute_nulls",
                             "Substitutes NULLs with 0 while loading data in columnar store",
                             NULL,
                             &imcs_substitute_nulls,
                             false,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

	DefineCustomBoolVariable("imcs.autoload",
                             "Automatically loads data in columnar store",
                             NULL,
                             &imcs_autoload,
                             true,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

	DefineCustomBoolVariable("imcs.serializable",
                             "Hold locks till the end of transaction to provide serializable isolation level",
                             NULL,
                             &imcs_serializable,
                             true,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

	DefineCustomBoolVariable("imcs.trace",
                             "Trace IMCS commands",
                             NULL,
                             &imcs_trace,
                             false,
                             PGC_USERSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

	DefineCustomIntVariable("imcs.output_string_limit",
                            "Limit for length of timeseries string representation.",
							NULL,
							&imcs_output_string_limit,
							1000,
							0,
							1000000000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in imcs_shmem_startup().
	 */
	RequestAddinShmemSpace((size_t)shmem_size*MB);
	RequestAddinLWLocks(1);
    
	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = imcs_shmem_startup;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = imcs_executor_end;
}

/*
 * Module unload callback
 */
void _PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
    ExecutorEnd_hook = prev_ExecutorEnd;

    if (imcs_thread_pool) { 
        imcs_thread_pool->destroy(imcs_thread_pool);
        imcs_thread_pool = NULL;
    }
    if (imcs_tls) {
        imcs_tls->destroy(imcs_tls);
    }
    free(imcs_error_handlers);
    if (imcs_alloc_mutex) { 
        imcs_alloc_mutex->destroy(imcs_alloc_mutex);
        imcs_alloc_mutex = NULL;
    }
    UnregisterXactCallback(imcs_trans_callback, NULL);          
    imcs_disk_close();
}

static void imcs_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook) { 
		prev_shmem_startup_hook();
    }

	/* reset in case this is a restart within the postmaster */
	imcs = NULL;
	imcs_hash = NULL;
	imcs_dict = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	imcs = ShmemInitStruct("imcs",
						   sizeof(imcs_state_t),
						   &found);

	if (!found)
	{
		/* First time through ... */
		imcs->lock = LWLockAssign();
        imcs->free_pages = NULL;
        imcs->n_used_pages = 0;
        imcs_disk_initialize(&imcs->disk_cache);
	}
    imcs_disk_open();
    imcs_init_hash();
    imcs_init_dict();
    imcs_alloc_mutex = imcs_create_mutex();
    /* operator's pipe should exist until end of query execution.
     * So we can not use default memory context and have to create own own memory context which is reset by ExecutorEnd_hook
     */
    imcs_mem_ctx = AllocSetContextCreate(TopMemoryContext,
                                         "IMCS tempory memory",
                                         ALLOCSET_DEFAULT_MINSIZE,
                                         ALLOCSET_DEFAULT_INITSIZE,
                                         ALLOCSET_DEFAULT_MAXSIZE);
	LWLockRelease(AddinShmemInitLock);
    RegisterXactCallback(imcs_trans_callback, NULL);          
}


/* function for timeseries of scalar type */
#define IMCS_APPLY(func, elem_type, params)                             \
    IMCS_TRACE(func);                                                   \
    switch (elem_type) {                                                \
      case TID_int8:                                                    \
        result = imcs_##func##_int8 params;                             \
        break;                                                          \
      case TID_int16:                                                   \
        result = imcs_##func##_int16 params;                            \
        break;                                                          \
      case TID_int32:                                                   \
      case TID_date:                                                    \
        result = imcs_##func##_int32 params;                            \
        break;                                                          \
      case TID_int64:                                                   \
      case TID_time:                                                    \
      case TID_timestamp:                                               \
      case TID_money:                                                   \
        result = imcs_##func##_int64 params;                            \
        break;                                                          \
      case TID_float:                                                   \
        result = imcs_##func##_float params;                            \
        break;                                                          \
      case TID_double:                                                  \
        result = imcs_##func##_double params;                           \
        break;                                                          \
      default:                                                          \
        imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "operation is not supported for timeseries of CHAR(N) type"); \
        result = NULL;                                                  \
    }                                                       

/* Functions for timeseries of integer type */
#define IMCS_APPLY_INT(func, elem_type, params)                         \
    IMCS_TRACE(func);                                                   \
    switch (elem_type) {                                                \
      case TID_int8:                                                    \
        result = imcs_##func##_int8 params;                             \
        break;                                                          \
      case TID_int16:                                                   \
        result = imcs_##func##_int16 params;                            \
        break;                                                          \
      case TID_int32:                                                   \
        result = imcs_##func##_int32 params;                            \
        break;                                                          \
      case TID_int64:                                                   \
        result = imcs_##func##_int64 params;                            \
        break;                                                          \
      default:                                                          \
        imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "operation is supported for timeseries of integer type"); \
        result = NULL;                                                  \
    }                                                       

/* Function returning void */
#define IMCS_APPLY_VOID(func, elem_type, params)                        \
    IMCS_TRACE(func);                                                   \
    switch (elem_type) {                                                \
      case TID_int8:                                                    \
        imcs_##func##_int8 params;                                      \
        break;                                                          \
      case TID_int16:                                                   \
        imcs_##func##_int16 params;                                     \
        break;                                                          \
      case TID_int32:                                                   \
      case TID_date:                                                    \
        imcs_##func##_int32 params;                                     \
        break;                                                          \
      case TID_int64:                                                   \
      case TID_time:                                                    \
      case TID_timestamp:                                               \
      case TID_money:                                                   \
        imcs_##func##_int64 params;                                     \
        break;                                                          \
      case TID_float:                                                   \
        imcs_##func##_float params;                                     \
        break;                                                          \
      case TID_double:                                                  \
        imcs_##func##_double params;                                    \
        break;                                                          \
      default:                                                          \
        imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "operation is not supported for timeseries of CHAR(N) type"); \
    }                                                       

/* Function for timeseries of scalar or character type */
#define IMCS_APPLY_CHAR(func, elem_type, params)    \
    IMCS_TRACE(func);                               \
    switch (elem_type) {                            \
      case TID_int8:                                \
        result = imcs_##func##_int8 params;         \
        break;                                      \
      case TID_int16:                               \
        result = imcs_##func##_int16 params;        \
        break;                                      \
      case TID_int32:                               \
      case TID_date:                                \
        result = imcs_##func##_int32 params;        \
        break;                                      \
      case TID_int64:                               \
      case TID_time:                                \
      case TID_timestamp:                           \
      case TID_money:                               \
        result = imcs_##func##_int64 params;        \
        break;                                      \
      case TID_float:                               \
        result = imcs_##func##_float params;        \
        break;                                      \
      case TID_double:                              \
        result = imcs_##func##_double params;       \
        break;                                      \
      case TID_char:                                \
        result = imcs_##func##_char params;         \
        break;                                      \
      default:                                      \
        result = NULL;                              \
        Assert(false);                              \
    }                                                       

/* Polymorphic binary function */
#define IMCS_BINARY_ANY_OP(func)                                        \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);       \
    imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);      \
    imcs_iterator_h result = imcs_##func(left, right);                  \
    IMCS_TRACE(func);                                                   \
    PG_RETURN_POINTER(result);                                          \
}

/* Group by functions */
#define IMCS_GROUP_OP(func)                                             \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_iterator_h group_by = (imcs_iterator_h)PG_GETARG_POINTER(1);   \
    imcs_iterator_h result;                                             \
    IMCS_APPLY(func, input->elem_type, (input, group_by));              \
    PG_RETURN_POINTER(result);                                          \
}

/* Binary operations for timeseries of scalar type */
#define IMCS_BINARY_OP(func)                                            \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);       \
    imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);      \
    imcs_iterator_h result;                                             \
    if (left->elem_type != right->elem_type) {                          \
        if (left->elem_type < right->elem_type) {                       \
            left = imcs_cast(left, right->elem_type, right->elem_size); \
        } else {                                                        \
            right = imcs_cast(right, left->elem_type, left->elem_size); \
        }                                                               \
    }                                                                   \
    IMCS_APPLY(func, left->elem_type, (left, right));                   \
    PG_RETURN_POINTER(result);                                          \
}

/* Binary operations for timeseries of integer type */
#define IMCS_BINARY_INT_OP(func)                                        \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);       \
    imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);      \
    imcs_iterator_h result;                                             \
    if (left->elem_type != right->elem_type) {                          \
        if (left->elem_type < right->elem_type) {                       \
            left = imcs_cast(left, right->elem_type, right->elem_size); \
        } else {                                                        \
            right = imcs_cast(right, left->elem_type, left->elem_size); \
        }                                                               \
    }                                                                   \
    IMCS_APPLY_INT(func, left->elem_type, (left, right));               \
    PG_RETURN_POINTER(result);                                          \
}

/* Binary functions returning scalar */
#define IMCS_BINARY_SCALAR_OP(func)                                     \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);       \
    imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);      \
    imcs_iterator_h result;                                             \
    double val;                                                         \
    if (left->elem_type != right->elem_type) {                          \
        if (left->elem_type < right->elem_type) {                       \
            left = imcs_cast(left, right->elem_type, right->elem_size); \
        } else {                                                        \
            right = imcs_cast(right, left->elem_type, left->elem_size); \
        }                                                               \
    }                                                                   \
    IMCS_APPLY(func, left->elem_type, (left, right));                   \
    result = imcs_parallel_iterator(result);                            \
    if (!imcs_next_double(result, &val)) {                              \
        PG_RETURN_NULL();                                               \
    } else {                                                            \
        PG_RETURN_FLOAT8(val);                                          \
    }                                                                   \
}

/* Binary functions for timeseries of scalar or character type */
#define IMCS_BINARY_CHAR_OP(func)                                       \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);       \
    imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);      \
    imcs_iterator_h result;                                             \
    if (left->elem_type != right->elem_type) {                          \
        if (left->elem_type < right->elem_type) {                       \
            left = imcs_cast(left, right->elem_type, right->elem_size); \
        } else {                                                        \
            right = imcs_cast(right, left->elem_type, left->elem_size); \
        }                                                               \
    }                                                                   \
    IMCS_APPLY_CHAR(func, left->elem_type, (left, right));              \
    PG_RETURN_POINTER(result);                                          \
}

/* Polymorphic unary function */
#define IMCS_UNARY_ANY_OP(func)                                         \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_iterator_h result = imcs_##func(input);                        \
    IMCS_TRACE(func);                                                   \
    PG_RETURN_POINTER(result);                                          \
}

/* Unary function for timeseries of scalar type */
#define IMCS_UNARY_OP(func)                                             \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_iterator_h result;                                             \
    IMCS_APPLY(func, input->elem_type, (input));                        \
    PG_RETURN_POINTER(result);                                          \
}

/* Unary function for timeseries of integer type */
#define IMCS_UNARY_INT_OP(func)                                         \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_iterator_h result;                                             \
    IMCS_APPLY_INT(func, input->elem_type, (input));                    \
    PG_RETURN_POINTER(result);                                          \
}

/* Unary function for timeseries of scalar or character type */
#define IMCS_UNARY_CHAR_OP(func)                                        \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_iterator_h result;                                             \
    IMCS_APPLY_CHAR(func, input->elem_type, (input));                   \
    PG_RETURN_POINTER(result);                                          \
}

/* Function of timeseries and integer parameters (interval) */
#define IMCS_INTERVAL_OP(func)                                          \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    int32 interval = PG_GETARG_INT32(1);                                \
    imcs_iterator_h result;                                             \
    IMCS_APPLY(func, input->elem_type, (input, interval));              \
    PG_RETURN_POINTER(result);                                          \
}

#define IMCS_SORT_OP(func)                                              \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_order_t order = PG_GETARG_BOOL(1) ? IMCS_ASC_ORDER : IMCS_DESC_ORDER; \
    imcs_iterator_h result;                                             \
    IMCS_APPLY(func, input->elem_type, (input, order));                 \
    PG_RETURN_POINTER(result);                                          \
}

/* Top functions */
#define IMCS_TOP_OP(func)                                               \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    int32 top = PG_GETARG_INT32(1);                                     \
    imcs_iterator_h result;                                             \
    IMCS_APPLY(func, input->elem_type, (input, top));                   \
    result = imcs_parallel_iterator(result);                            \
    PG_RETURN_POINTER(result);                                          \
}

/* Unary mathematical functions */
#define IMCS_MATH_FUNC(func)                                            \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    if (input->elem_type != TID_double) {                               \
        input = imcs_cast(input, TID_double, 0);                        \
    }                                                                   \
    IMCS_TRACE(func);                                                   \
    PG_RETURN_POINTER(imcs_func_double(input, &func));                  \
}

/* Binary mathematical functions */
#define IMCS_MATH_FUNC2(func)                                           \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);       \
    imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);      \
    double result;                                                      \
    if (left->elem_type != TID_double) {                                \
        left = imcs_cast(left, TID_double, 0);                          \
    }                                                                   \
    if (right->elem_type != TID_double) {                               \
        right = imcs_cast(right, TID_double, 0);                        \
    }                                                                   \
    IMCS_TRACE(func);                                                   \
    PG_RETURN_POINTER(imcs_func2_double(input, &func));                 \
}

/* Grand aggregates */
#define IMCS_AGGREGATE(func)                                            \
    Datum cs_##func(PG_FUNCTION_ARGS)                                   \
    {                                                                   \
        imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);  \
        imcs_iterator_h result;                                         \
        IMCS_APPLY(func, input->elem_type, (input));                    \
        result = imcs_parallel_iterator(result);                        \
        switch (result->elem_type) {                                    \
          case TID_int8:                                                \
          {                                                             \
              int8 val = 0;                                             \
              if (imcs_next_int8(result, &val)) {                       \
                  PG_RETURN_FLOAT8((double)val);                        \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_int16:                                               \
          {                                                             \
              int16 val = 0;                                            \
              if (imcs_next_int16(result, &val)) {                      \
                  PG_RETURN_FLOAT8((double)val);                        \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_int32:                                               \
          case TID_date:                                                \
          {                                                             \
              int32 val = 0;                                            \
              if (imcs_next_int32(result, &val)) {                      \
                  PG_RETURN_FLOAT8((double)val);                        \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_int64:                                               \
          case TID_time:                                                \
          case TID_timestamp:                                           \
          case TID_money:                                               \
          {                                                             \
              int64 val = 0;                                            \
              if (imcs_next_int64(result, &val)) {                      \
                  PG_RETURN_FLOAT8((double)val);                        \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_float:                                               \
          {                                                             \
              float val = 0;                                            \
              if (imcs_next_float(result, &val)) {                      \
                  PG_RETURN_FLOAT8((double)val);                        \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_double:                                              \
          {                                                             \
              double val = 0;                                           \
              if (imcs_next_double(result, &val)) {                     \
                  PG_RETURN_FLOAT8(val);                                \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          default:                                                      \
            Assert(false);                                              \
        }                                                               \
        PG_RETURN_NULL();                                               \
    }

#define IMCS_INT_AGGREGATE(func)                                        \
    Datum cs_##func(PG_FUNCTION_ARGS)                                   \
    {                                                                   \
        imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);  \
        imcs_iterator_h result;                                         \
        IMCS_APPLY_INT(func, input->elem_type, (input));                \
        result = imcs_parallel_iterator(result);                        \
        switch (result->elem_type) {                                    \
          case TID_int8:                                                \
          {                                                             \
              int8 val = 0;                                             \
              if (imcs_next_int8(result, &val)) {                       \
                  PG_RETURN_INT64(val);                                 \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_int16:                                               \
          {                                                             \
              int16 val = 0;                                            \
              if (imcs_next_int16(result, &val)) {                      \
                  PG_RETURN_INT64(val);                                 \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_int32:                                               \
          {                                                             \
              int32 val = 0;                                            \
              if (imcs_next_int32(result, &val)) {                      \
                  PG_RETURN_INT64(val);                                 \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          case TID_int64:                                               \
          {                                                             \
              int64 val = 0;                                            \
              if (imcs_next_int64(result, &val)) {                      \
                  PG_RETURN_INT64(val);                                 \
              } else {                                                  \
                  PG_RETURN_NULL();                                     \
              }                                                         \
          }                                                             \
          default:                                                      \
            Assert(false);                                              \
        }                                                               \
        PG_RETURN_NULL();                                               \
    }

/* Hash aggregates */
#define IMCS_HASH_AGG(func)                                             \
Datum cs_hash_##func(PG_FUNCTION_ARGS)                                  \
{                                                                       \
     imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);     \
     imcs_iterator_h group_by = (imcs_iterator_h)PG_GETARG_POINTER(1);  \
     TupleDesc resultTupleDesc;                                         \
     Datum outValues[2];                                                \
     bool nulls[2] = {false, false};                                    \
     imcs_iterator_h result[2];                                         \
     get_call_result_type(fcinfo, NULL, &resultTupleDesc);              \
     IMCS_APPLY_VOID(hash_##func, input->elem_type, (result, input, group_by)); \
     result[0] = imcs_parallel_iterator(result[0]);                     \
     outValues[0] = PointerGetDatum(result[0]);                         \
     outValues[1] = PointerGetDatum(result[1]);                         \
     PG_RETURN_POINTER(HeapTupleGetDatum(heap_form_tuple(resultTupleDesc, outValues, nulls))); \
}


typedef struct {     
    imcs_pos_t interval;
} imcs_visitor_context_t;

/* Checks that all operators in subtree are reentrant and have the same boundaries */
static bool imcs_parallel_execution_possible_for_operator(imcs_iterator_h iterator, imcs_visitor_context_t* ctx) 
{ 
    if (iterator == NULL) { 
        return true;
    }
    if (iterator->flags & FLAG_CONTEXT_FREE) { 
        if (iterator->flags & FLAG_RANDOM_ACCESS) {
            Assert(iterator->last_pos != IMCS_INFINITY);
            if (ctx->interval > iterator->last_pos - iterator->first_pos + 1) { 
                ctx->interval = iterator->last_pos - iterator->first_pos + 1;
            }
        } else { 
            int i;
            for (i = 0; i < 3; i++) { 
                if (!imcs_parallel_execution_possible_for_operator(iterator->opd[i], ctx)) { 
                    return false;
                }
            }            
        }
        return true;
    } 
    return false;
}

/* Creates thread specific branch of execution tree. Each of N threads will be given 1/N of underlying timeseries. */
static imcs_iterator_h imcs_clone_tree(imcs_iterator_h iterator, int worker_id, uint64 interval) 
{ 
    if (iterator == NULL) { 
        return NULL;
    } else { 
        imcs_iterator_h clone = imcs_clone_iterator(iterator);
        if (iterator->flags & FLAG_RANDOM_ACCESS) { 
            imcs_subseq_random_access_iterator(clone, worker_id*interval, (worker_id+1)*interval-1);
        } else { 
            int i;
            for (i = 0; i < 3; i++) { 
                clone->opd[i] = imcs_clone_tree(iterator->opd[i], worker_id, interval);
            }
        }
        return clone;
    }
}

static void imcs_merge_job_results(void* arg, void* result)
{
     imcs_iterator_h par_iterator = (imcs_iterator_h)arg;
     imcs_iterator_h iterator = (imcs_iterator_h)result;
     Assert(par_iterator->iterator_size == iterator->iterator_size);
     if (par_iterator->opd[1] == NULL) {
         par_iterator->opd[1] = iterator;
     } else { 
         par_iterator->opd[1]->merge(par_iterator->opd[1], iterator);
     }    
}

static void imcs_parallel_job(int worker_id, int n_workers, void* arg)
{
    imcs_error_handlers[worker_id].err_code = ERRCODE_SUCCESSFUL_COMPLETION;
    imcs_tls->set(imcs_tls, &imcs_error_handlers[worker_id]);
    if (!setjmp(imcs_error_handlers[worker_id].unwind_buf)) { 
        imcs_iterator_h par_iterator = (imcs_iterator_h)arg;
        imcs_iterator_h iterator = par_iterator->opd[0];
        uint64 interval = (par_iterator->last_pos - par_iterator->first_pos + n_workers)/n_workers; /* round up */
        imcs_iterator_h clone_iterator = imcs_clone_tree(iterator, worker_id, interval);
        if (clone_iterator->prepare(clone_iterator)) { 
            imcs_thread_pool->merge(imcs_thread_pool, imcs_merge_job_results, clone_iterator);
        }
    } 
}

static bool imcs_parallel_execute(imcs_iterator_h iterator)
{
    bool result;
    size_t ctx_offs;
    imcs_iterator_h opd[2];
    int i;
    imcs_thread_pool->execute(imcs_thread_pool, imcs_parallel_job, iterator);    
    if (iterator->opd[1] == NULL) { 
        return false;
    }
    if (imcs_error_handlers != NULL) { 
        for (i = 0; i < n_threads;  i++) { 
            if (imcs_error_handlers[i].err_code != ERRCODE_SUCCESSFUL_COMPLETION) { 
                ereport(ERROR, (errcode(imcs_error_handlers[i].err_code), errmsg(imcs_error_handlers[i].err_msg)));
            }            
        }
    }
    Assert(iterator->iterator_size == iterator->opd[1]->iterator_size);
    ctx_offs = (char*)iterator->opd[1]->context - (char*)iterator->opd[1];
    opd[0] = iterator->opd[0]->opd[0]; /* save original operands */
    opd[1] = iterator->opd[0]->opd[1];
    memcpy(iterator, iterator->opd[1], iterator->iterator_size);  /* opd[1] contains thread-specific operands */ 
    iterator->opd[0] = opd[0];         /* restore original operands */
    iterator->opd[1] = opd[1];
    iterator->context = (char*)iterator + ctx_offs;
    iterator->flags |= FLAG_PREPARED;
    result = iterator->next(iterator);
    iterator->flags &= ~FLAG_PREPARED;
    return result;
}

static bool imcs_is_unlimited(imcs_iterator_h iterator) 
{
    int i;
    int n_operands = 0;
    if (iterator->flags & FLAG_CONSTANT) {
        return true;
    }
    for (i = 0; i < 3; i++) {
        if (iterator->opd[i] != NULL) { 
            n_operands += 1;
            if (!imcs_is_unlimited(iterator->opd[i])) {
                return false;
            }
        }
    }
    return n_operands != 0 && iterator->last_pos == IMCS_INFINITY;
}

imcs_iterator_h imcs_parallel_iterator(imcs_iterator_h iterator) 
{
    imcs_visitor_context_t ctx;
    if (imcs_is_unlimited(iterator)) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Value can not be caclualted for unbounded sequence");    
    } 
    if (n_threads == 1 || iterator->merge == NULL) {                  
        return iterator;
    }    
    if (imcs_thread_pool == NULL) { 
        imcs_thread_pool = imcs_create_thread_pool(n_threads);
        n_threads = imcs_thread_pool->get_number_of_threads(imcs_thread_pool);
        imcs_error_handlers = (imcs_error_handler_t*)malloc(sizeof(imcs_error_handler_t)*n_threads);
        imcs_tls = imcs_create_tls();
    }
    ctx.interval = IMCS_INFINITY;                    
    if (imcs_parallel_execution_possible_for_operator(iterator->opd[0], &ctx) 
        && imcs_parallel_execution_possible_for_operator(iterator->opd[1], &ctx) 
        && ctx.interval != IMCS_INFINITY
        && ctx.interval > imcs_thread_pool->get_number_of_threads(imcs_thread_pool))
    {
        imcs_iterator_h par = imcs_clone_iterator(iterator);
        par->opd[0] = iterator;
        par->opd[1] = NULL; /* here result of merge will be stored */
        par->next = imcs_parallel_execute;
        par->first_pos = 0;
        par->last_pos = ctx.interval-1;
        return par;
    }
    return iterator;
}

static int64 imcs_date2timestamp(int64 date)
{
#ifdef HAVE_INT64_TIMESTAMP
    return date*USECS_PER_DAY;
#else
    return date*SECS_PER_DAY;
#endif
}
static int64 imcs_timestamp2date(int64 timestamp)
{
#ifdef HAVE_INT64_TIMESTAMP
    return timestamp/USECS_PER_DAY;
#else
    return timestamp/SECS_PER_DAY;
#endif
}

static int64 imcs_timestamp2time(int64 timestamp)
{
#ifdef HAVE_INT64_TIMESTAMP
    return timestamp%USECS_PER_DAY;
#else
    return timestamp%SECS_PER_DAY;
#endif
}


static double imcs_money2double(double money) { 
    return money/100.0;
}

static double imcs_double2money(double money) { 
    return round(money*100.0);
}


imcs_iterator_h imcs_cast(imcs_iterator_h input, imcs_elem_typeid_t elem_type, int elem_size) 
{
    imcs_iterator_h result;
    if (elem_type == input->elem_type) { 
        return input;
    }
    if (elem_type == TID_timestamp && input->elem_type == TID_date) {
        IMCS_TRACE(cast);
        result = imcs_func_int64(imcs_int64_from_int32(input), imcs_date2timestamp);
    } else if (elem_type == TID_time && input->elem_type == TID_timestamp) { 
        IMCS_TRACE(cast);
        result = imcs_func_int64(input, imcs_timestamp2time);
    } else if (elem_type == TID_date && input->elem_type == TID_timestamp) { 
        IMCS_TRACE(cast);
        result = imcs_int32_from_int64(imcs_func_int64(input, imcs_timestamp2date));
    } else if (elem_type == TID_money && input->elem_type == TID_double) { 
        IMCS_TRACE(cast);
        result = imcs_int64_from_double(imcs_func_double(input, imcs_double2money));
    } else if (elem_type == TID_money && input->elem_type == TID_float) { 
        IMCS_TRACE(cast);
        result = imcs_int64_from_double(imcs_func_double(imcs_double_from_float(input), imcs_double2money));
    } else if (elem_type == TID_double && input->elem_type == TID_money) { 
        IMCS_TRACE(cast);
        result = imcs_func_double(imcs_double_from_int64(input), imcs_money2double);
    } else if (elem_type == TID_float && input->elem_type == TID_money) { 
        IMCS_TRACE(cast);
        result = imcs_float_from_double(imcs_func_double(imcs_double_from_int64(input), imcs_money2double));
    } else if (imcs_underlying_type[elem_type] == imcs_underlying_type[input->elem_type]) { 
        result = imcs_clone_iterator(input);
    } else { 
        switch (elem_type) {
          case TID_int8:                                       
            IMCS_APPLY(int8_from, input->elem_type, (input));
            break;                                              
          case TID_int16:                                       
            IMCS_APPLY(int16_from, input->elem_type, (input));
            break;                                              
          case TID_int32:                                       
          case TID_date:                                
            IMCS_APPLY(int32_from, input->elem_type, (input));
            break;                                              
          case TID_int64:                                       
          case TID_time:                                
          case TID_timestamp:                           
          case TID_money:                           
            IMCS_APPLY(int64_from, input->elem_type, (input));
            break;                                              
          case TID_float:                                       
            IMCS_APPLY(float_from, input->elem_type, (input));
            break;                                              
          case TID_double:                                      
            IMCS_APPLY(double_from, input->elem_type, (input));
            break;  
          case TID_char:
            IMCS_TRACE(cast);                                                   
            result = imcs_cast_to_char(input, elem_size);
            break;             
          default:                                              
            imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "cast to timeseries of CHAR(N) is not supported"); 
        }                    
    }
    result->elem_type = elem_type;
    return result;
}

/* Checks if columnar store was initialized and mark it as initialized */
Datum columnar_store_initialized(PG_FUNCTION_ARGS)
{
    char const* table_name = PG_GETARG_CSTRING(0);
    bool initialize = PG_GETARG_BOOL(1);
    imcs_timeseries_t* ts = imcs_get_timeseries(table_name, TID_int8, false, 0, initialize);
    bool is_initialized = ts != NULL && ts->count != 0;
    if (initialize) { 
        ts->count = 1;
    }
    PG_RETURN_BOOL(is_initialized);
}
    
Datum columnar_store_get(PG_FUNCTION_ARGS)                              
{                                                                       
    char const* cs_id = PG_GETARG_CSTRING(0);                           
    imcs_iterator_h search_result = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(2); 
    int elem_size = PG_GETARG_INT32(3);
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, false, elem_size, false); 
    if (ts == NULL) {                                                   
        PG_RETURN_NULL();                                               
    }                                                                   
    PG_RETURN_POINTER(imcs_subseq(ts, search_result->first_pos, search_result->last_pos));
}

Datum columnar_store_lock(PG_FUNCTION_ARGS)                              
{                        
    if (imcs != NULL)
    {
        if (imcs_lock != LOCK_EXCLUSIVE) { 
            if (imcs_lock != LOCK_NONE) { 
                LWLockRelease(imcs->lock);
            }
            LWLockAcquire(imcs->lock, LW_EXCLUSIVE);
            imcs_lock = LOCK_EXCLUSIVE;
        }
    }
    PG_RETURN_VOID();
}                  
                             
Datum columnar_store_span(PG_FUNCTION_ARGS)                              
{                                                                       
    char const* cs_id = PG_GETARG_CSTRING(0);                           
    int64 from = PG_GETARG_INT64(1); 
    int64 till = PG_GETARG_INT64(2); 
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(3);     
    bool is_timestamp = PG_GETARG_BOOL(4);
    int elem_size = PG_GETARG_INT32(5);
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, is_timestamp, elem_size, false); 
    if (ts == NULL) {                                                   
        PG_RETURN_NULL();                                               
    }                                          
    if (from < 0) from = ts->count + from;
    if (till < 0) till = ts->count + till;
    PG_RETURN_POINTER(imcs_subseq(ts, from, till));
}


#define IMCS_SEARCH(TYPE,PG_TYPE)                                       \
Datum columnar_store_search_##TYPE(PG_FUNCTION_ARGS)                    \
{                                                                       \
    char const* cs_id = PG_GETARG_CSTRING(0);                           \
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(3); \
    int elem_size = imcs_type_sizeof[elem_type];                        \
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, true, elem_size, false); \
    imcs_iterator_h result = NULL;                                      \
    TYPE low = 0, high = 0;                                             \
    imcs_boundary_kind_t low_boundary = BOUNDARY_OPEN;                  \
    imcs_boundary_kind_t high_boundary = BOUNDARY_OPEN;                 \
    imcs_count_t limit = PG_ARGISNULL(4) ? 0 : PG_GETARG_INT64(4);      \
    if (ts == NULL) {                                                   \
        PG_RETURN_NULL();                                               \
    }                                                                   \
    if (!PG_ARGISNULL(1)) {                                             \
        low = PG_GETARG_##PG_TYPE(1);                                   \
        low_boundary = BOUNDARY_INCLUSIVE;                              \
    }                                                                   \
    if (!PG_ARGISNULL(2)) {                                             \
        high = PG_GETARG_##PG_TYPE(2);                                  \
        high_boundary = BOUNDARY_INCLUSIVE;                             \
    }                                                                   \
    result = imcs_search_##TYPE(ts, low, low_boundary, high, high_boundary, limit); \
    if (result == NULL) {                                               \
        PG_RETURN_NULL();                                               \
    } else {                                                            \
        PG_RETURN_POINTER(result);                                      \
    }                                                                   \
}

IMCS_SEARCH(int8,CHAR);
IMCS_SEARCH(int16,INT16);
IMCS_SEARCH(int32,INT32);
IMCS_SEARCH(int64,INT64);
IMCS_SEARCH(float,FLOAT4);
IMCS_SEARCH(double,FLOAT8);

Datum columnar_store_delete(PG_FUNCTION_ARGS)                    
{                                                                       
    char const* cs_id = PG_GETARG_CSTRING(0);                           
    imcs_iterator_h search_result = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(2); 
    bool is_timestamp = PG_GETARG_BOOL(3);                              
    int elem_size = PG_GETARG_INT32(4);                                 
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, is_timestamp, elem_size, true); 
    imcs_delete(ts, search_result->first_pos, search_result->last_pos);
    PG_RETURN_VOID();
}

#define IMCS_APPEND(TYPE,PG_TYPE)                                       \
Datum columnar_store_append_##TYPE(PG_FUNCTION_ARGS)                    \
{                                                                       \
    char const* cs_id = PG_GETARG_CSTRING(0);                           \
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(2); \
    bool is_timestamp = PG_GETARG_BOOL(3);                              \
    int elem_size = PG_GETARG_INT32(4);                                 \
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, is_timestamp, elem_size, true); \
    if (PG_ARGISNULL(1)) {                                              \
        if (imcs_substitute_nulls) {                                    \
            imcs_append_##TYPE(ts, 0);                                  \
        } else {                                                        \
            imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "NULL values are not supported by columnar store"); \
        }                                                               \
    } else {                                                            \
        imcs_append_##TYPE(ts, PG_GETARG_##PG_TYPE(1));                 \
    }                                                                   \
    PG_RETURN_VOID();                                                   \
}

IMCS_APPEND(int8,CHAR);
IMCS_APPEND(int16,INT16);
IMCS_APPEND(int32,INT32);
IMCS_APPEND(int64,INT64);
IMCS_APPEND(float,FLOAT4);
IMCS_APPEND(double,FLOAT8);

Datum columnar_store_append_char(PG_FUNCTION_ARGS)                    
{                                                                       
    char const* cs_id = PG_GETARG_CSTRING(0);                           
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(2); 
    bool is_timestamp = PG_GETARG_BOOL(3);                              
    int elem_size = PG_GETARG_INT32(4);                                 
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, is_timestamp, elem_size, true); 
    if (elem_size < 0) { /* varying string */
        bool found;
        imcs_dict_key_t key;
        imcs_dict_entry_t* entry;
        if (PG_ARGISNULL(1)) { /* substitute NULL with empty string */
            if (imcs_substitute_nulls) {                                    
                key.val = NULL;
                key.len = 0;
            } else {                                                        
                imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "NULL values are not supported by columnar store"); 
            }                                                               
        } else { 
            text* t = PG_GETARG_TEXT_P(1);
            key.val = (char*)VARDATA(t);
            key.len = VARSIZE(t) - VARHDRSZ;
        }                            
        entry = (imcs_dict_entry_t*)hash_search(imcs_dict, &key, HASH_ENTER, &found);
        if (!found) { 
            entry->code = hash_get_num_entries(imcs_dict);
            if (entry->code >= imcs_dict_size) {  
                imcs_ereport(ERRCODE_OUT_OF_MEMORY, "IMSC dictionary limit exceeded");
            }   
            imcs_dict_code_map[entry->code] = entry;
        }
        if (imcs_dict_size <= IMCS_SMALL_DICTIONARY) { 
            imcs_append_int16(ts, (int16)entry->code);
        } else { 
            imcs_append_int32(ts, (int32)entry->code);
        }
    } else { 
        if (PG_ARGISNULL(1)) {                                              
            if (imcs_substitute_nulls) {                                    
                imcs_append_char(ts, NULL, 0); /* substitute NULL with empty string */
            } else {                                                        
                imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "NULL values are not supported by columnar store"); 
            }                                                               
        } else {                                                            
            text* t = PG_GETARG_TEXT_P(1);
            int len = VARSIZE(t) - VARHDRSZ;
            if (len > elem_size) { 
                imcs_ereport(ERRCODE_STRING_DATA_LENGTH_MISMATCH, "String length %d is larger then element size %d", len, elem_size);             
            }
            imcs_append_char(ts, (char*)VARDATA(t), len);                     
        }
    }                                                                   
    PG_RETURN_VOID();                                                   
}


Datum columnar_store_count(PG_FUNCTION_ARGS)                  
{                                                                       
    char const* cs_id = PG_GETARG_CSTRING(0);                           
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(1); 
    int elem_size = imcs_type_sizeof[elem_type];                        
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, true, elem_size, false); 
    if (ts == NULL) {                   
        PG_RETURN_NULL();                                               
    } else {                                                            
        PG_RETURN_INT64(ts->count);                                       
    }                                                                   
}

#define IMCS_BOUNDARY(TYPE,PG_TYPE,MNEM)                                \
Datum columnar_store_##MNEM##_##TYPE(PG_FUNCTION_ARGS)                  \
{                                                                       \
    char const* cs_id = PG_GETARG_CSTRING(0);                           \
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(1); \
    int elem_size = PG_GETARG_INT32(2);                                 \
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, true, elem_size, false); \
    TYPE val;                                                           \
    if (ts == NULL || !imcs_##MNEM##_##TYPE(ts, &val)) {                \
        PG_RETURN_NULL();                                               \
    } else {                                                            \
        PG_RETURN_##PG_TYPE(val);                                       \
    }                                                                   \
}

IMCS_BOUNDARY(int8,CHAR,first);
IMCS_BOUNDARY(int16,INT16,first);
IMCS_BOUNDARY(int32,INT32,first);
IMCS_BOUNDARY(int64,INT64,first);
IMCS_BOUNDARY(float,FLOAT4,first);
IMCS_BOUNDARY(double,FLOAT8,first);

IMCS_BOUNDARY(int8,CHAR,last);
IMCS_BOUNDARY(int16,INT16,last);
IMCS_BOUNDARY(int32,INT32,last);
IMCS_BOUNDARY(int64,INT64,last);
IMCS_BOUNDARY(float,FLOAT4,last);
IMCS_BOUNDARY(double,FLOAT8,last);

#define IMCS_JOIN(TYPE)                                                 \
Datum columnar_store_join_##TYPE(PG_FUNCTION_ARGS)                      \
{                                                                       \
    char const* cs_id = PG_GETARG_CSTRING(0);                           \
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(1); \
    int elem_size = PG_GETARG_INT32(2);                                 \
    imcs_timeseries_t* ts = imcs_get_timeseries(cs_id, elem_type, true, elem_size, false); \
    imcs_iterator_h join_with = (imcs_iterator_h)PG_GETARG_POINTER(3);  \
    int direction = PG_GETARG_INT32(4);                                 \
    if (ts == NULL) {                                                   \
        PG_RETURN_NULL();                                               \
    } else {                                                            \
        PG_RETURN_POINTER(imcs_join_unsorted_##TYPE(ts, join_with, direction)); \
    }                                                                   \
}

IMCS_JOIN(int8);
IMCS_JOIN(int16);
IMCS_JOIN(int32);
IMCS_JOIN(int64);
IMCS_JOIN(float);
IMCS_JOIN(double);

static Datum imcs_parse_adt(imcs_adt_parser_t* parser, char* value, size_t size)
{
    return InputFunctionCall(&parser->proc, value, parser->param_oid, -1);
}

static imcs_adt_parser_t* imcs_new_adt_parser(Oid type, FunctionCallInfo fcinfo)
{
    imcs_adt_parser_t* parser = (imcs_adt_parser_t*)imcs_alloc(sizeof(imcs_adt_parser_t));    
	getTypeInputInfo(type, &parser->input_oid, &parser->param_oid);			
    fmgr_info_cxt(parser->input_oid, &parser->proc, fcinfo->flinfo->fn_mcxt);
    parser->parse = imcs_parse_adt;
    return parser;
}

static Datum imcs_parse_dict(imcs_adt_parser_t* parser, char* value, size_t size)
{
    imcs_dict_key_t key; 
    imcs_dict_entry_t* entry;
    key.val = value;
    key.len = size;
    entry = (imcs_dict_entry_t*)hash_search(imcs_dict, &key, HASH_FIND, NULL);
    if (entry == NULL) { 
        imcs_ereport(ERRCODE_NO_DATA_FOUND, "String '%.*s' not found in dictionary", (int)size, value);
    }
    if (imcs_dict_size <= IMCS_SMALL_DICTIONARY) { 
        PG_RETURN_INT16((int16)entry->code);                   
    } else { 
        PG_RETURN_INT32((int32)entry->code);                   
    }
}

static imcs_adt_parser_t* imcs_new_dict_parser()
{
    imcs_adt_parser_t* parser = (imcs_adt_parser_t*)imcs_alloc(sizeof(imcs_adt_parser_t));    
    parser->parse = imcs_parse_dict;
    return parser;
}

Datum cs_parse_tid(PG_FUNCTION_ARGS)
{
    text* t = PG_GETARG_TEXT_P(0);
    char* txt = VARDATA(t);
    size_t text_len = VARSIZE(t) - VARHDRSZ;
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(1);
    int elem_size = PG_GETARG_INT32(2);
    imcs_iterator_h result;
    if (elem_type == TID_char && *txt != '{') { 
        /* make it possible to compare column with string */
        char* elem = (char*)palloc(elem_size);
        if (elem_size < text_len) { 
            imcs_ereport(ERRCODE_STRING_DATA_LENGTH_MISMATCH, "CHAR literal too long"); 
        } else { 
            memcpy(elem, txt, text_len);
            memset(elem + text_len, '\0', elem_size - text_len);
        }
        result = imcs_const_char(elem, elem_size);
        pfree(elem);
        IMCS_TRACE(const);
    } else {
        IMCS_TRACE(parse);
        char* str = (char*)imcs_alloc(text_len+1);
        memcpy(str, txt, text_len);
        str[text_len] = '\0';
        switch (elem_type) { 
        case TID_date:
            result = imcs_adt_parse_int32(str, imcs_new_adt_parser(DATEOID, fcinfo));
            break;
        case TID_time:
            result = imcs_adt_parse_int64(str, imcs_new_adt_parser(TIMEOID, fcinfo));
            break;
        case TID_timestamp:
            result = imcs_adt_parse_int64(str, imcs_new_adt_parser(TIMESTAMPOID, fcinfo));
            break;
        case TID_money:
            result = imcs_adt_parse_int64(str, imcs_new_adt_parser(CASHOID, fcinfo));
            break;
        default:        
            IMCS_APPLY_CHAR(parse, elem_type, (str, elem_size));
        }
        result->elem_type = elem_type;
    }
    PG_RETURN_POINTER(result);
}
Datum cs_const_str(PG_FUNCTION_ARGS)
{
    text* t = PG_GETARG_TEXT_P(0);
    char* str = (char*)VARDATA(t);
    int len = VARSIZE(t) - VARHDRSZ;
    int elem_size = PG_GETARG_INT32(1);
    char* elem = (char*)palloc(elem_size);
    imcs_iterator_h result;
    if (elem_size < len) { 
        imcs_ereport(ERRCODE_STRING_DATA_LENGTH_MISMATCH, "CHAR literal too long"); 
    } else { 
        memcpy(elem, str, len);
        memset(elem + len, '\0', elem_size - len);
    }
    result = imcs_const_char(elem, elem_size);
    pfree(elem);
    IMCS_TRACE(const);
    PG_RETURN_POINTER(result);
}
Datum cs_const_num(PG_FUNCTION_ARGS)
{
    double val = PG_GETARG_FLOAT8(0);
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(1);
    imcs_iterator_h result;
    IMCS_TRACE(const);
    switch (elem_type) { 
      case TID_int8:
        result = imcs_const_int8((int8)val);        
        break;
      case TID_int16:
        result = imcs_const_int16((int16)val);        
        break;
      case TID_int32:
      case TID_date:
        result = imcs_const_int32((int32)val);        
        break;
      case TID_int64:
      case TID_time:
      case TID_timestamp:
      case TID_money:
        result = imcs_const_int64((int64)val);        
        break;
      case TID_float:
        result = imcs_const_float((float)val);
        break;
      case TID_double:
        result = imcs_const_double(val);
        break;
      default:
        result = NULL;
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "numeric value expected");
    }
    result->elem_type = elem_type;
    PG_RETURN_POINTER(result);
}
Datum cs_const_dt(PG_FUNCTION_ARGS)
{
    int64 val = PG_GETARG_INT64(0);
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(1);
    imcs_iterator_h result;
    IMCS_TRACE(const);
    switch (elem_type) { 
      case TID_int8:
        result = imcs_const_int8((int8)val);        
        break;
      case TID_int16:
        result = imcs_const_int16((int16)val);        
        break;
      case TID_date:
        val = imcs_timestamp2date(val);
        /* no break */
      case TID_int32:
        result = imcs_const_int32((int32)val);        
        break;
      case TID_time:
        val = imcs_timestamp2time(val);
        /* no break */
      case TID_int64:
      case TID_timestamp:
      case TID_money:
        result = imcs_const_int64(val);        
        break;
      case TID_float:
        result = imcs_const_float((float)val);
        break;
      case TID_double:
        result = imcs_const_double((double)val);
        break;
      default:
        result = NULL;
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "numeric value expected");
    }
    result->elem_type = elem_type;
    PG_RETURN_POINTER(result);
}
Datum cs_cast_tid(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_elem_typeid_t elem_type = (imcs_elem_typeid_t)PG_GETARG_INT32(1);
    int elem_size = (imcs_elem_typeid_t)PG_GETARG_INT32(2);
    imcs_iterator_h result = imcs_cast(input, elem_type, elem_size);
    PG_RETURN_POINTER(result);
}
Datum cs_type(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    PG_RETURN_INT32(input->elem_type);
}
Datum cs_elem_size(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    PG_RETURN_INT32(input->elem_size);
}

Datum cs_input_function(PG_FUNCTION_ARGS)
{
    char const* cstr = PG_GETARG_CSTRING(0);
    imcs_elem_typeid_t elem_type;
    imcs_iterator_h result;
    int elem_size = 0;
    int n = 0;
    char* str;
    if (cstr == NULL) { 
        PG_RETURN_NULL();
    }
    str = (char*)imcs_alloc(strlen(cstr)+1);
    strcpy(str, cstr);
    if (strncmp(str, "bpchar", 6) == 0) { 
        elem_type = TID_char;
        if (sscanf(str+6, "%d:%n", &elem_size, &n) != 1) { 
            imcs_ereport(ERRCODE_SYNTAX_ERROR, "failed to parse timeseries '%s'", str);
        }
        n += 6; /* strlen("bpchar") */
    } else if (strncmp(str, "varchar:", 8) == 0) { 
        if (imcs_dict_size == 0) { 
            imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Failed to parse VARCHAR timeseries because there is no dictionary"); 
        }
        result = (imcs_dict_size <= IMCS_SMALL_DICTIONARY) 
            ? imcs_adt_parse_int16(str+8, imcs_new_dict_parser())
            : imcs_adt_parse_int32(str+8, imcs_new_dict_parser());
        PG_RETURN_POINTER(result);
    } else { 
        char* col = strchr(str, ':');
        if (col == NULL) { 
            imcs_ereport(ERRCODE_SYNTAX_ERROR, "failed to parse timeseries '%s'", str);
        }
        n = col - str;
        for (elem_type = TID_int8; n != imcs_type_mnem_lens[elem_type] || strncmp(imcs_type_mnems[elem_type], str, n) != 0; elem_type++) { 
            if (elem_type == TID_char) { /* last TID */
                imcs_ereport(ERRCODE_SYNTAX_ERROR, "invalid element type name %.*s", n, str);
            }
        }
        n += 1; /* skip column */
    }
    switch (elem_type) { 
      case TID_date:
        result = imcs_adt_parse_int32(str+n, imcs_new_adt_parser(DATEOID, fcinfo));
        break;
      case TID_time:
        result = imcs_adt_parse_int64(str+n, imcs_new_adt_parser(TIMEOID, fcinfo));
        break;
      case TID_timestamp:
        result = imcs_adt_parse_int64(str+n, imcs_new_adt_parser(TIMESTAMPOID, fcinfo));
        break;
      case TID_money:
        result = imcs_adt_parse_int64(str+n, imcs_new_adt_parser(CASHOID, fcinfo));
        break;
      default:
        IMCS_APPLY_CHAR(parse, elem_type, (str+n, elem_size));
    }
    result->elem_type = elem_type;
    PG_RETURN_POINTER(result);
}

    
Datum cs_output_function(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    size_t allocated = imcs_output_string_limit == 0 ? IMCS_INIT_OUTPUT_BUF_SIZE 
        : imcs_output_string_limit < IMCS_MIN_OUTPUT_BUF_SIZE ? IMCS_MIN_OUTPUT_BUF_SIZE : imcs_output_string_limit;
    char* buf = imcs_alloc(allocated);
    char* new_buf;
    size_t used = sprintf(buf, "%s:", imcs_type_mnems[input->elem_type]); /* timeseries element type prefix */
    char sep = '{';
    bool truncated = false;
    size_t output_limit = imcs_output_string_limit - 1; /* transform 0 into infinity */

    if (imcs_is_unlimited(input)) { 
        input = imcs_limit(input, 0, 0); /* print only first element of timeseries of repeated concstant value, because this timeseries has infinite length */
        truncated = true;
    }

    switch (input->elem_type) { 
      case TID_int8:
      {
          int8 val;
          while (imcs_next_int8(input, &val)) { 
              if (used + MAX_NUMELEM_LEN > allocated) { 
                  if (allocated >= output_limit) { 
                      truncated = true;
                      break;
                  }
                  new_buf = imcs_alloc(allocated*=2);
                  memcpy(new_buf, buf, used);
                  imcs_free(buf);
                  buf = new_buf;
              }
              buf[used++] = sep;
              used += sprintf(&buf[used], "%d", val);
              sep = ',';
          }
          break;
      }
      case TID_int16:
      {
          int16 val;
          if (input->flags & FLAG_TRANSLATED) { 
              used = sprintf(buf, "varchar:");
              while (imcs_next_int16(input, &val)) { 
                  imcs_dict_entry_t* entry;
                  Assert((uint16)val < imcs_dict_size);
                  entry = imcs_dict_code_map[(uint16)val];
                  if (used + entry->key.len + OUTPUT_BUF_RESERVE > allocated) { 
                      if (allocated >= output_limit) { 
                          truncated = true;
                          break;
                      }
                      new_buf = imcs_alloc(allocated*=2);
                      memcpy(new_buf, buf, used);
                      imcs_free(buf);
                      buf = new_buf;
                  }
                  buf[used++] = sep;
                  memcpy(buf + used, entry->key.val, entry->key.len);
                  used += entry->key.len;
                  sep = ',';
              }
          } else {               
              while (imcs_next_int16(input, &val)) { 
                  if (used + MAX_NUMELEM_LEN > allocated) { 
                      if (allocated >= output_limit) { 
                          truncated = true;
                          break;
                      }
                      new_buf = imcs_alloc(allocated*=2);
                      memcpy(new_buf, buf, used);
                      imcs_free(buf);
                      buf = new_buf;
                  }
                  buf[used++] = sep;
                  used += sprintf(&buf[used], "%d", val);
                  sep = ',';
              }
          }
          break;
      }
      case TID_int32:
      {
          int32 val;
          if (input->flags & FLAG_TRANSLATED) { 
              used = sprintf(buf, "varchar:");
              while (imcs_next_int32(input, &val)) { 
                  imcs_dict_entry_t* entry;
                  Assert((uint32)val < imcs_dict_size);
                  entry = imcs_dict_code_map[(uint32)val];
                  if (used + entry->key.len + OUTPUT_BUF_RESERVE > allocated) { 
                      if (allocated >= output_limit) { 
                          truncated = true;
                          break;
                      }
                      new_buf = imcs_alloc(allocated*=2);
                      memcpy(new_buf, buf, used);
                      imcs_free(buf);
                      buf = new_buf;
                  }
                  buf[used++] = sep;
                  memcpy(buf + used, entry->key.val, entry->key.len);
                  used += entry->key.len;
                  sep = ',';
              }
          } else {               
              while (imcs_next_int32(input, &val)) { 
                  if (used + MAX_NUMELEM_LEN > allocated) { 
                      if (allocated >= output_limit) { 
                          truncated = true;
                          break;
                      }
                      new_buf = imcs_alloc(allocated*=2);
                      memcpy(new_buf, buf, used);
                      imcs_free(buf);
                      buf = new_buf;
                  }
                  buf[used++] = sep;
                  used += sprintf(&buf[used], "%d", val);
                  sep = ',';
              }
          }
          break;
      }
      case TID_int64:
      {
          int64 val;
          while (imcs_next_int64(input, &val)) { 
              if (used + MAX_NUMELEM_LEN > allocated) { 
                  if (allocated >= output_limit) { 
                      truncated = true;
                      break;
                  }
                  new_buf = imcs_alloc(allocated*=2);
                  memcpy(new_buf, buf, used);
                  imcs_free(buf);
                  buf = new_buf;
              }
              buf[used++] = sep;
              used += sprintf(&buf[used], "%lld", (long long)val);
              sep = ',';
          }
          break;
      }
      case TID_float:
      {
          float val;
          int ndig = FLT_DIG + extra_float_digits;
          if (ndig < 1) { 
              ndig = 1;         
          }
          while (imcs_next_float(input, &val)) { 
              if (used + MAX_NUMELEM_LEN > allocated) { 
                  if (allocated >= output_limit) { 
                      truncated = true;
                      break;
                  }
                  new_buf = imcs_alloc(allocated*=2);
                  memcpy(new_buf, buf, used);
                  imcs_free(buf);
                  buf = new_buf;
              }
              buf[used++] = sep;
              used += sprintf(&buf[used], "%.*g", ndig, val);
              sep = ',';
          }
          break;
      }
      case TID_double:
      {
          double val;
          int ndig = DBL_DIG + extra_float_digits;
          if (ndig < 1) { 
              ndig = 1;         
          }
          while (imcs_next_double(input, &val)) { 
              if (used + MAX_NUMELEM_LEN > allocated) { 
                  if (allocated >= output_limit) { 
                      truncated = true;
                      break;
                  }
                  new_buf = imcs_alloc(allocated*=2);
                  memcpy(new_buf, buf, used);
                  imcs_free(buf);
                  buf = new_buf;
              }
              buf[used++] = sep;
              used += sprintf(&buf[used], "%.*g", ndig, val);
              sep = ',';
          }
          break;
      }
      case TID_char:
      {
          int elem_size = input->elem_size;
          used = sprintf(buf, "bpchar%d:", elem_size);
          while (true) { 
              if (used + elem_size + OUTPUT_BUF_RESERVE > allocated) { 
                  if (allocated >= output_limit) { 
                      truncated = true;
                      break;
                  }
                  new_buf = imcs_alloc(allocated*=2);
                  memcpy(new_buf, buf, used);
                  imcs_free(buf);
                  buf = new_buf;
              }
              buf[used] = sep;
              if (!imcs_next_char(input, &buf[used+1])) { 
                  break;
              }
              used += 1;
              buf[used+elem_size] = '\0';
              used += strlen(&buf[used]);
              sep = ',';
          }
          break;
      }
      case TID_date:
      {
          Oid type_out;
          bool is_varlena;
          int32 val;
          getTypeOutputInfo(DATEOID, &type_out, &is_varlena);
          while (imcs_next_int32(input, &val)) { 
              char* str = OidOutputFunctionCall(type_out, Int32GetDatum(val));
              size_t len = strlen(str);
              if (used + len + OUTPUT_BUF_RESERVE > allocated) { 
                  if (allocated >= output_limit) { 
                      truncated = true;
                      break;
                  }
                  new_buf = imcs_alloc(allocated*=2);
                  memcpy(new_buf, buf, used);
                  imcs_free(buf);
                  buf = new_buf;
              }
              buf[used++] = sep;
              memcpy(buf+used, str, len);
              pfree(str);
              used += len;
              sep = ',';
          }
          break;
      }
      case TID_time:
      case TID_timestamp:
      case TID_money:
      {
          Oid type_out;
          bool is_varlena;
          int64 val;
          getTypeOutputInfo(imcs_elem_type_to_oid[input->elem_type], &type_out, &is_varlena);
          while (imcs_next_int64(input, &val)) { 
              char* str = OidOutputFunctionCall(type_out, Int64GetDatum(val));
              size_t len = strlen(str);
              char* comma = strchr(str, ',');
              if (comma != NULL) { 
                  len += 2;
              }
              if (used + len + OUTPUT_BUF_RESERVE > allocated) { 
                  if (allocated >= output_limit) { 
                      truncated = true;
                      break;
                  }
                  new_buf = imcs_alloc(allocated*=2);
                  memcpy(new_buf, buf, used);
                  imcs_free(buf);
                  buf = new_buf;
              }
              buf[used++] = sep;
              if (comma != NULL) { 
                  buf[used] = '"';
                  memcpy(buf+used+1, str, len-2);
                  buf[used+len-1] = '"';
              } else { 
                  memcpy(buf+used, str, len);
              }
              pfree(str);
              used += len;
              sep = ',';
          }
          break;
      }
      default:
          Assert(false);                  
    }
    if (sep == '{') {  
        buf[used++] = '{';
    } 
    if (truncated) { 
        strcpy(buf + used, ",...");
        used += 4;
    }
    buf[used++] = '}';
    buf[used] = '\0';
    PG_RETURN_CSTRING(buf);
}

Datum cs_receive_function(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
    size_t      count;
    size_t      i;

    imcs_iterator_h iterator = imcs_new_iterator(pq_getmsgint(buf, 2), 0);
    iterator->elem_type = (imcs_elem_typeid_t)pq_getmsgint(buf, 1);
    count = (size_t)pq_getmsgint64(buf);
    
    switch (iterator->elem_type) { 
      case TID_int16:
      { 
          int16* arr = (int16*)imcs_alloc(sizeof(int16)*count);
          for (i = 0; i < count; i++) { 
              arr[i] = (int16)pq_getmsgint(buf, 2);
          }
          imcs_from_array(iterator, arr, count);
          break;
      }
      case TID_int32:
      case TID_date:
      {
          int32* arr = (int32*)imcs_alloc(sizeof(int32)*count);
          for (i = 0; i < count; i++) { 
              arr[i] = pq_getmsgint(buf, 4);
          }
          imcs_from_array(iterator, arr, count);
          break;
      }          
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
      {
          int64* arr = (int64*)imcs_alloc(sizeof(int64)*count);
          for (i = 0; i < count; i++) { 
              arr[i] = pq_getmsgint64(buf);
          }
          imcs_from_array(iterator, arr, count);
          break;
      }
      case TID_float:
      {
          float* arr = (float*)imcs_alloc(sizeof(float)*count);
          for (i = 0; i < count; i++) { 
              arr[i] = pq_getmsgfloat4(buf);
          }
          imcs_from_array(iterator, arr, count);
          break;
      }
      case TID_double:
      {
          double* arr = (double*)imcs_alloc(sizeof(double)*count);
          for (i = 0; i < count; i++) { 
              arr[i] = pq_getmsgfloat8(buf);
          }
          imcs_from_array(iterator, arr, count);
          break;
      }
      case TID_char:
      case TID_int8:
     {		
          int   n_bytes;
          char* str = pq_getmsgtext(buf, count*iterator->elem_size, &n_bytes);
          Assert(n_bytes == count*iterator->elem_size);
          imcs_from_array(iterator, str, count);
          break;
      }
      default:
        Assert(false);
    }
    PG_RETURN_POINTER(iterator);
}
Datum cs_send_function(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_count_t count = (size_t)imcs_count(input);
	StringInfoData buf;

    input->reset(input);
	pq_begintypsend(&buf);
	pq_sendint(&buf, input->elem_size, 2);
	pq_sendint(&buf, input->elem_type, 1);
	pq_sendint64(&buf, count);
    switch (input->elem_type) { 
      case TID_int16:
      { 
          int16 val;
          while (imcs_next_int16(input, &val)) {
              pq_sendint(&buf, val, 2);
          }
          break;
      }
      case TID_int32:
      case TID_date:
      {
          int32 val;
          while (imcs_next_int32(input, &val)) {
              pq_sendint(&buf, val, 4);
          }
          break;
      }          
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
      {
          int64 val;
          while (imcs_next_int64(input, &val)) {
              	pq_sendint64(&buf, val);
          }
          break;
      }
      case TID_float:
      {
          float val;
          while (imcs_next_float(input, &val)) {
              pq_sendfloat4(&buf, val);
          }
          break;
      }
      case TID_double:
      {
          double val;
          while (imcs_next_double(input, &val)) {
              pq_sendfloat8(&buf, val);
          }
          break;
      }
      case TID_int8:
      case TID_char:
      {
          char* textbuf = (char*)imcs_alloc(count*input->elem_size);
          imcs_to_array(input, textbuf, count);
          pq_sendtext(&buf, textbuf, count);
          imcs_free(textbuf);
          break;
      }
      default:
        Assert(false);
    }
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

IMCS_BINARY_CHAR_OP(add)
IMCS_BINARY_OP(mul)
IMCS_BINARY_OP(sub)
IMCS_BINARY_OP(div)
IMCS_BINARY_OP(mod)
IMCS_BINARY_OP(pow)
IMCS_BINARY_OP(maxof)
IMCS_BINARY_OP(minof)
IMCS_BINARY_INT_OP(and)
IMCS_BINARY_INT_OP(or)
IMCS_BINARY_INT_OP(xor)

/* If one of arguments is null, just return another */
Datum cs_concat(PG_FUNCTION_ARGS)                                       
{                                                                       
    if (PG_ARGISNULL(0)) { 
        if (PG_ARGISNULL(1)) { 
            PG_RETURN_NULL();
        } else { 
            PG_RETURN_DATUM(PG_GETARG_DATUM(1));
        } 
    } else if (PG_ARGISNULL(1)) {  
        PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    } else { 
        imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);   
        imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);  
        imcs_iterator_h result = imcs_concat(left, right);              
        IMCS_TRACE(concat);
        PG_RETURN_POINTER(result);                                      
    }
}

IMCS_BINARY_ANY_OP(cat)

IMCS_BINARY_CHAR_OP(eq)
IMCS_BINARY_CHAR_OP(ne)
IMCS_BINARY_CHAR_OP(gt)
IMCS_BINARY_CHAR_OP(ge)
IMCS_BINARY_CHAR_OP(lt)
IMCS_BINARY_CHAR_OP(le)

IMCS_UNARY_OP(neg)
IMCS_UNARY_OP(abs)

IMCS_UNARY_INT_OP(not)
IMCS_UNARY_INT_OP(bit_not)

Datum cs_norm(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);  
    imcs_iterator_h result;
    IMCS_APPLY(norm, input->elem_type, (input));                        
    result = imcs_parallel_iterator(result);                
    PG_RETURN_POINTER(result);
}

Datum cs_limit(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_pos_t from = PG_GETARG_INT64(1);
    imcs_pos_t till = PG_GETARG_INT64(2);
    imcs_iterator_h result = imcs_limit(input, from, till);
    IMCS_TRACE(limit);
    PG_RETURN_POINTER(result);
}

IMCS_MATH_FUNC(sin)
IMCS_MATH_FUNC(cos)
IMCS_MATH_FUNC(tan)
IMCS_MATH_FUNC(exp)
IMCS_MATH_FUNC(asin)
IMCS_MATH_FUNC(acos)
IMCS_MATH_FUNC(atan)
IMCS_MATH_FUNC(sqrt)
IMCS_MATH_FUNC(log)
IMCS_MATH_FUNC(ceil)
IMCS_MATH_FUNC(floor)

Datum cs_isnan(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);  
    imcs_iterator_h result = NULL;
    switch (input->elem_type) { 
      case TID_float:
        result = imcs_isnan_float(input);
        break;
      case TID_double:
        result = imcs_isnan_double(input);
        break;
      default:
        imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "cs_isnan is defined only for timeseries of float4 or float8 types"); 
    }                                                     
    PG_RETURN_POINTER(result);
}


IMCS_BINARY_SCALAR_OP(wsum)
IMCS_BINARY_SCALAR_OP(wavg)
IMCS_BINARY_SCALAR_OP(corr)
IMCS_BINARY_SCALAR_OP(cov)

Datum cs_thin(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    int origin = PG_GETARG_INT32(1);
    int step = PG_GETARG_INT32(2);
    imcs_iterator_h result;
    IMCS_APPLY_CHAR(thin, input->elem_type, (input, origin, step));
    PG_RETURN_POINTER(result);
}

Datum cs_iif(PG_FUNCTION_ARGS)
{
    imcs_iterator_h cond = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h then = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h otherwise = (imcs_iterator_h)PG_GETARG_POINTER(2);
    imcs_iterator_h result;
    if (then->elem_type < otherwise->elem_type) { 
        then = imcs_cast(then, otherwise->elem_type, otherwise->elem_size);
    } else if (then->elem_type > otherwise->elem_type) { 
        otherwise = imcs_cast(otherwise, then->elem_type, then->elem_size);
    }
    IMCS_APPLY_CHAR(iif, then->elem_type, (cond, then, otherwise));
    PG_RETURN_POINTER(result);
}
Datum cs_if(PG_FUNCTION_ARGS)
{
    imcs_iterator_h cond = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h then = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h otherwise = (imcs_iterator_h)PG_GETARG_POINTER(2);
    imcs_iterator_h result;
    if (then->elem_type < otherwise->elem_type) { 
        then = imcs_cast(then, otherwise->elem_type, otherwise->elem_size);
    } else if (then->elem_type > otherwise->elem_type) { 
        otherwise = imcs_cast(otherwise, then->elem_type, then->elem_size);
    }
    IMCS_APPLY_CHAR(if, then->elem_type, (cond, then, otherwise));
    PG_RETURN_POINTER(result);
}

Datum cs_filter(PG_FUNCTION_ARGS)
{
    imcs_iterator_h cond = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h result;
    IMCS_APPLY_CHAR(filter, input->elem_type, (cond, input));    
    PG_RETURN_POINTER(result);
}

IMCS_UNARY_ANY_OP(filter_pos)
IMCS_UNARY_CHAR_OP(unique)
IMCS_UNARY_CHAR_OP(reverse)
IMCS_UNARY_OP(diff)
IMCS_UNARY_OP(trend)
IMCS_INTERVAL_OP(repeat)

Datum cs_filter_first_pos(PG_FUNCTION_ARGS)
{
    imcs_iterator_h cond = (imcs_iterator_h)PG_GETARG_POINTER(0);
    int32 n = PG_GETARG_INT32(1); 
    imcs_iterator_h result = imcs_filter_first_pos(cond, n);
    result = imcs_parallel_iterator(result);                            
    PG_RETURN_POINTER(result);
}

Datum cs_count(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_count_t count = imcs_count(input);
    PG_RETURN_INT64(count);
}

imcs_count_t imcs_count(imcs_iterator_h iterator)          
{                                                                       
    imcs_count_t count;                                          
    if (iterator->flags & FLAG_RANDOM_ACCESS) { 
        Assert(iterator->last_pos != IMCS_INFINITY);
        count = iterator->last_pos - iterator->first_pos + 1;
    } else {         
        imcs_iterator_h result = imcs_count_iterator(iterator);
        IMCS_TRACE(count);
        result = imcs_parallel_iterator(result);                        
        if (!imcs_next_int64(result, (int64*)&count)) {  
            count = 0;
        }
    }                                                                   
    return count;
}                                                                       

Datum cs_approxdc(PG_FUNCTION_ARGS)                                       
{                                                                   
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);  
    int64 count = 0;                                         
    imcs_iterator_h result = imcs_approxdc(input);                                         
    IMCS_TRACE(approxdc);
    result = imcs_parallel_iterator(result);                
    imcs_next_int64(result, &count);                                
    PG_RETURN_INT64(count);  
}

Datum cs_like(PG_FUNCTION_ARGS)                                       
{                                                             
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);  
    text* t = PG_GETARG_TEXT_P(1);
    int len = VARSIZE(t) - VARHDRSZ;
    char* pattern = (char*)imcs_alloc(len+1);
    memcpy(pattern, VARDATA(t), len);
    pattern[len] = '\0';
    IMCS_TRACE(like);
    PG_RETURN_POINTER(imcs_like(input, pattern));
}

Datum cs_ilike(PG_FUNCTION_ARGS)                                       
{                                                             
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);  
    text* t = PG_GETARG_TEXT_P(1);
    int len = VARSIZE(t) - VARHDRSZ;
    char* pattern = (char*)imcs_alloc(len+1);
    memcpy(pattern, VARDATA(t), len);
    pattern[len] = '\0';
    IMCS_TRACE(ilike);
    PG_RETURN_POINTER(imcs_ilike(input, pattern));
}

IMCS_AGGREGATE(max)
IMCS_AGGREGATE(min)
IMCS_AGGREGATE(sum)
IMCS_AGGREGATE(avg)
IMCS_AGGREGATE(prd)
IMCS_AGGREGATE(var)
IMCS_AGGREGATE(dev)
IMCS_AGGREGATE(median)

IMCS_INT_AGGREGATE(any)
IMCS_INT_AGGREGATE(all)

IMCS_UNARY_ANY_OP(group_count)
IMCS_BINARY_ANY_OP(group_approxdc)
IMCS_GROUP_OP(group_max)
IMCS_GROUP_OP(group_min)
IMCS_GROUP_OP(group_sum)
IMCS_GROUP_OP(group_any)
IMCS_GROUP_OP(group_all)
IMCS_GROUP_OP(group_avg)
IMCS_GROUP_OP(group_var)
IMCS_GROUP_OP(group_dev)
IMCS_GROUP_OP(group_first)
IMCS_GROUP_OP(group_last)

IMCS_INTERVAL_OP(grid_max)
IMCS_INTERVAL_OP(grid_min)
IMCS_INTERVAL_OP(grid_sum)
IMCS_INTERVAL_OP(grid_avg)
IMCS_INTERVAL_OP(grid_var)
IMCS_INTERVAL_OP(grid_dev)

IMCS_INTERVAL_OP(window_max)
IMCS_INTERVAL_OP(window_min)
IMCS_INTERVAL_OP(window_sum)
IMCS_INTERVAL_OP(window_avg)
IMCS_INTERVAL_OP(window_var)
IMCS_INTERVAL_OP(window_dev)
IMCS_INTERVAL_OP(window_ema)
IMCS_INTERVAL_OP(window_atr)


Datum cs_hash_count(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    TupleDesc resultTupleDesc;
    Datum outValues[2];
    bool nulls[2] = {false, false};
    imcs_iterator_h result[2];                              
    get_call_result_type(fcinfo, NULL, &resultTupleDesc);
    imcs_hash_count(result, input);
    result[0] = imcs_parallel_iterator(result[0]);              
    outValues[0] = PointerGetDatum(result[0]);
    outValues[1] = PointerGetDatum(result[1]);
    IMCS_TRACE(hash_count);
    PG_RETURN_POINTER(HeapTupleGetDatum(heap_form_tuple(resultTupleDesc, outValues, nulls)));
}
Datum cs_hash_dup_count(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h group_by = (imcs_iterator_h)PG_GETARG_POINTER(1);
    int min_occ = PG_GETARG_INT32(2);
    TupleDesc resultTupleDesc;
    Datum outValues[2];
    bool nulls[2] = {false, false};
    imcs_iterator_h result[2]; 
    if (min_occ <= 0) { 
        imcs_ereport(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "min_occurrences should be positive number");
    }
    get_call_result_type(fcinfo, NULL, &resultTupleDesc);
    imcs_hash_dup_count(result, input, group_by, min_occ);
    result[0] = imcs_parallel_iterator(result[0]);              
    outValues[0] = PointerGetDatum(result[0]);
    outValues[1] = PointerGetDatum(result[1]);
    IMCS_TRACE(hash_dup_count);
    PG_RETURN_POINTER(HeapTupleGetDatum(heap_form_tuple(resultTupleDesc, outValues, nulls)));
}

IMCS_HASH_AGG(max)
IMCS_HASH_AGG(min)
IMCS_HASH_AGG(sum)
IMCS_HASH_AGG(any)
IMCS_HASH_AGG(all)
IMCS_HASH_AGG(avg)

IMCS_SORT_OP(rank)
IMCS_SORT_OP(dense_rank)
IMCS_SORT_OP(sort)
IMCS_SORT_OP(sort_pos)
IMCS_INTERVAL_OP(quantile)

IMCS_TOP_OP(top_max)
IMCS_TOP_OP(top_min)
IMCS_TOP_OP(top_max_pos)
IMCS_TOP_OP(top_min_pos)

IMCS_UNARY_OP(cum_max)
IMCS_UNARY_OP(cum_min)
IMCS_UNARY_OP(cum_sum)
IMCS_UNARY_OP(cum_avg)
IMCS_UNARY_OP(cum_prd)
IMCS_UNARY_OP(cum_var)
IMCS_UNARY_OP(cum_dev)

Datum cs_histogram(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);    
    imcs_iterator_h result;
    double min_val = PG_GETARG_FLOAT8(1);
    double max_val = PG_GETARG_FLOAT8(2);
    int n_intervals = PG_GETARG_INT32(3);
    IMCS_APPLY(histogram, input->elem_type, (input, min_val, max_val, n_intervals));
    result = imcs_parallel_iterator(result);
    PG_RETURN_POINTER(result);
}

IMCS_INTERVAL_OP(cross)
IMCS_INTERVAL_OP(extrema)

Datum cs_stretch(PG_FUNCTION_ARGS)
{
    imcs_iterator_h ts1 = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h ts2 = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h vals = (imcs_iterator_h)PG_GETARG_POINTER(2);
    double filler = PG_GETARG_FLOAT8(3);
    imcs_iterator_h result = NULL;
    switch (ts1->elem_type) { 
      case TID_int32:
      case TID_date:
        IMCS_APPLY(stretch_int32, vals->elem_type, (ts1, ts2, vals, filler));
        break;
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        IMCS_APPLY(stretch_int64, vals->elem_type, (ts1, ts2, vals, filler));
        break;
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "First and second arguments of cs_stretch should be timeseries of int4, int8, date, time or timestamp type");
    }
    PG_RETURN_POINTER(result);
}
Datum cs_stretch0(PG_FUNCTION_ARGS)
{
    imcs_iterator_h ts1 = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h ts2 = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h vals = (imcs_iterator_h)PG_GETARG_POINTER(2);
    double filler = PG_GETARG_FLOAT8(3);
    imcs_iterator_h result = NULL;
    switch (ts1->elem_type) { 
      case TID_int32:
      case TID_date:
        IMCS_APPLY(stretch0_int32, vals->elem_type, (ts1, ts2, vals, filler));
        break;
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        IMCS_APPLY(stretch0_int64, vals->elem_type, (ts1, ts2, vals, filler));
        break;
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "First and second arguments of cs_stretch0 should be timeseries of int4, int8, date, time or timestamp type");
    }
    PG_RETURN_POINTER(result);
}
Datum cs_asof_join(PG_FUNCTION_ARGS)
{
    imcs_iterator_h ts1 = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h ts2 = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h vals = (imcs_iterator_h)PG_GETARG_POINTER(2);
    imcs_iterator_h result = NULL;
    switch (ts1->elem_type) { 
      case TID_int32:
      case TID_date:
        IMCS_APPLY(asof_join_int32, vals->elem_type, (ts1, ts2, vals));
        break;
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        IMCS_APPLY(asof_join_int64, vals->elem_type, (ts1, ts2, vals));
        break;
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "First and second arguments of cs_asof_join should be timeseries of int4, int8, date, time or timestamp type");
    }
    PG_RETURN_POINTER(result);
}
Datum cs_asof_join_pos(PG_FUNCTION_ARGS)
{
    imcs_iterator_h ts1 = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h ts2 = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h result = NULL;
    IMCS_TRACE(asof_join_pos);
    switch (ts1->elem_type) { 
      case TID_int32:
      case TID_date:
        result = imcs_asof_join_pos_int32(ts1, ts2);
        break;
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        result = imcs_asof_join_pos_int64(ts1, ts2);
        break;
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "First and second arguments of cs_asof_join_pos should be timeseries of int4, int8, date, time or timestamp type");
    }
    PG_RETURN_POINTER(result);
}
Datum cs_join(PG_FUNCTION_ARGS)
{
    imcs_iterator_h ts1 = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h ts2 = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h vals = (imcs_iterator_h)PG_GETARG_POINTER(2);
    imcs_iterator_h result = NULL;
    switch (ts1->elem_type) { 
      case TID_int32:
      case TID_date:
        IMCS_APPLY(join_int32, vals->elem_type, (ts1, ts2, vals));
        break;
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        IMCS_APPLY(join_int64, vals->elem_type, (ts1, ts2, vals));
        break;
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "First and second arguments of cs_join should be timeseries of int4, int8, date, time or timestamp type");
    }
    PG_RETURN_POINTER(result);
}
Datum cs_join_pos(PG_FUNCTION_ARGS)
{
    imcs_iterator_h ts1 = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h ts2 = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h result = NULL;
    IMCS_TRACE(join_pos);
    switch (ts1->elem_type) { 
      case TID_int32:
      case TID_date:
        result = imcs_join_pos_int32(ts1, ts2);
        break;
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        result = imcs_join_pos_int64(ts1, ts2);
        break;
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "First and second arguments of cs_join_pos should be timeseries of int4, int8, date, time or timestamp type");
    }
    PG_RETURN_POINTER(result);
}


static imcs_iterator_h imcs_project(imcs_iterator_h input, imcs_iterator_h positions)
{
    imcs_iterator_h result;
    if (input->cs_hdr) {
        result = imcs_map(input, imcs_operand(positions));
    } else {
        IMCS_APPLY_CHAR(map, input->elem_type, (input, positions));
    }
    return result;
}

Datum cs_map(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h positions = (imcs_iterator_h)PG_GETARG_POINTER(1);
    PG_RETURN_POINTER(imcs_project(input, positions));
}
Datum cs_union(PG_FUNCTION_ARGS)
{
    imcs_iterator_h left = (imcs_iterator_h)PG_GETARG_POINTER(0);
    imcs_iterator_h right = (imcs_iterator_h)PG_GETARG_POINTER(1);
    imcs_iterator_h result;
    IMCS_APPLY(union, left->elem_type, (left, right));
    PG_RETURN_POINTER(result);
}
Datum cs_empty(PG_FUNCTION_ARGS)
{
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);
    bool has_next = false;
    IMCS_TRACE(empty);
    switch (input->elem_type) {
      case TID_int8:
      { 
          int8 val;
          has_next = imcs_next_int8(input, &val);
          break;
      }
      case TID_int16:
      { 
          int16 val;
          has_next = imcs_next_int16(input, &val);
          break;
      }
      case TID_int32:
      case TID_date:
      { 
          int32 val;
          has_next = imcs_next_int32(input, &val);
          break;
      }
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
      { 
          int64 val;
          has_next = imcs_next_int64(input, &val);
          break;
      }
      case TID_float:
      { 
          float val;
          has_next = imcs_next_float(input, &val);
          break;
      }
      case TID_double:
      { 
          double val;
          has_next = imcs_next_double(input, &val);
          break;
      }
      case TID_char:
      { 
          char* val = (char*)imcs_alloc(input->elem_size);
          has_next = imcs_next_char(input, val);
          imcs_free(val);
          break;
      }
    }
    PG_RETURN_BOOL(!has_next);
}

typedef struct { 
    int               n_iterators;
    int               n_timeseries;
    imcs_iterator_h*  iterators;
    Datum*            values;
    bool*             nulls;
    TupleDesc         desc;
} imcs_project_context;

Datum cs_project(PG_FUNCTION_ARGS)
{ 
    HeapTupleHeader t = NULL;
    imcs_iterator_h positions = PG_ARGISNULL(1) ? (imcs_iterator_h)NULL : (imcs_iterator_h)PG_GETARG_POINTER(1);
    bool disable_caching = PG_GETARG_BOOL(2);
    FuncCallContext* funcctx;
    imcs_project_context* usrfctx;
    int i;
    Datum elem;
    HeapTuple tuple;
    bool is_first_call = SRF_IS_FIRSTCALL();
    bool is_null = false;
    Oid	argtype;
    int n_attrs = 0;
    int n_iters;
    TupleDesc attr_desc = NULL;
    Oid	timeseries_oid = 0;

    if (PG_ARGISNULL(0)) { 
        PG_RETURN_NULL();
    }
    if (is_first_call) { 
        char typtype;
        timeseries_oid = TypenameGetTypid("timeseries");
        argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
        typtype = get_typtype(argtype);
        if (typtype == 'c' || typtype == 'p') { 
            t = PG_GETARG_HEAPTUPLEHEADER(0); 
            attr_desc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(t), HeapTupleHeaderGetTypMod(t));
            n_attrs = attr_desc->natts;
        } else { 
            if (argtype != timeseries_oid) { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "First argument of cs_project should be compound type or timeseries");
            }
            n_attrs = 1;
        }
    } else { 
        t = PG_GETARG_HEAPTUPLEHEADER(0); 
    }
    if (!disable_caching && imcs_project_caching) { 
        if (is_first_call) { 
            if (imcs_project_call_count == 1) { 
                /* cs_project() is redundantly called second time in (cs_project(...)).* expression - PostgreSQL behavour */
                /* This condition may be true also when cs_project() is used twice in the same query - disable caching in this case */ 
                imcs_project_redundant_calls = n_attrs; /* number of attributes of projected tuple */
            } else if (imcs_project_redundant_calls > 1 && imcs_project_call_count >= imcs_project_redundant_calls) { 
                /* iteratation is not yet completed, but first function call is encountered */
                imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "Multiple usage of cs_project_agg in select column list are not supported");
            }
        }
        if (imcs_project_redundant_calls > 1) { 
            Assert(imcs_project_call_count > 0);
            if (imcs_project_call_count < imcs_project_redundant_calls && !is_first_call) {
                imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "Multiple usage of cs_project in select column list are not supported");
            }
            if (imcs_project_call_count++ % imcs_project_redundant_calls != 0) {
                funcctx = is_first_call ? SRF_FIRSTCALL_INIT() : SRF_PERCALL_SETUP();
                if (attr_desc != NULL) { 
                    ReleaseTupleDesc(attr_desc);
                }
                if (imcs_project_result_cache) { 
                    SRF_RETURN_NEXT(funcctx, imcs_project_result_cache);    
                } else { 
                    if (imcs_project_call_count % imcs_project_redundant_calls == 0) { /* end of traversal */
                        imcs_project_redundant_calls = 0;
                        imcs_project_call_count = 0;
                    }
                    SRF_RETURN_DONE(funcctx);   
                }
            }
        } else { 
            imcs_project_call_count += 1;
        }
    }
    if (is_first_call)
    {
        MemoryContext oldcontext;
        int n_timeseries = 0;

        IMCS_TRACE(project);
        imcs_project_call_count = 1; /* initialize counter */
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);       
        usrfctx = (imcs_project_context*)palloc(sizeof(imcs_project_context));
        usrfctx->iterators = (imcs_iterator_h*)palloc(sizeof(imcs_iterator_h)*n_attrs);
        usrfctx->values = (Datum*)palloc(sizeof(Datum)*n_attrs);
        usrfctx->nulls = (bool*)palloc(sizeof(bool)*n_attrs);
        usrfctx->desc = CreateTemplateTupleDesc(n_attrs, false);
        funcctx->user_fctx = usrfctx;
        usrfctx->n_iterators = n_attrs;
        for (i = 0; i < n_attrs; i++) {
            Datum datum;
            imcs_iterator_h attr_iterator;
            int attnum;
            int atttypid;
            char const* attname;
            if (attr_desc != NULL) { 
                Form_pg_attribute attr = attr_desc->attrs[i];
                datum = GetAttributeByNum(t, attr->attnum, &usrfctx->nulls[i]);
                if (attr->atttypid != timeseries_oid) { 
                    usrfctx->iterators[i] = NULL;
                    usrfctx->values[i] = datum;
                    TupleDescInitEntry(usrfctx->desc, attr->attnum, attr->attname.data, attr->atttypid, attr->atttypmod, attr->attndims);
                    continue;
                } else if (usrfctx->nulls[i]) { /* if of some of iterators is null, then return null */
                    is_null = true;
                    break;
                }
                attnum = attr->attnum;
                attname = attr->attname.data;
            } else { 
                datum = PG_GETARG_DATUM(0);
                attnum = 1;
                attname = "timeseries";
                usrfctx->nulls[i] = false;
            }
            attr_iterator = (imcs_iterator_h)DatumGetPointer(datum);   
            if (positions != NULL) { 
                if (i+1 < n_attrs) {
                    imcs_iterator_h tee_iterators[2];
                    imcs_tee(tee_iterators, positions);
                    usrfctx->iterators[i] = imcs_project(attr_iterator, tee_iterators[0]);
                    positions = tee_iterators[1];
                } else { 
                    usrfctx->iterators[i] = imcs_project(attr_iterator, positions);
                }
            } else { 
                usrfctx->iterators[i] = imcs_operand(attr_iterator);
            } 
            atttypid = (usrfctx->iterators[i]->flags & FLAG_TRANSLATED) ? TEXTOID : imcs_elem_type_to_oid[usrfctx->iterators[i]->elem_type];
            TupleDescInitEntry(usrfctx->desc, attnum, attname, atttypid, -1, 0);
            n_timeseries += 1;
        }  
        usrfctx->n_timeseries = n_timeseries;
        if (!is_null) { 
            TupleDescGetAttInMetadata(usrfctx->desc);
        }
        MemoryContextSwitchTo(oldcontext);      
        if (attr_desc != NULL) { 
            ReleaseTupleDesc(attr_desc);
        }
    } 
    funcctx = SRF_PERCALL_SETUP();
    usrfctx = (imcs_project_context*)funcctx->user_fctx;
    imcs_project_result_cache = 0; /* 0 means end of set */
    if (is_null || (!is_first_call && usrfctx->n_timeseries == 0)) { 
        SRF_RETURN_DONE(funcctx);      
    }
    for (i = 0, n_iters = usrfctx->n_iterators; i < n_iters; i++) { 
        if (usrfctx->iterators[i] != NULL) { 
            switch (usrfctx->iterators[i]->elem_type) {
            case TID_int8:
                { 
                    int8 val;
                    if (!imcs_next_int8(usrfctx->iterators[i], &val)) { 
                        SRF_RETURN_DONE(funcctx);      
                    }
                    usrfctx->values[i] = Int8GetDatum(val);
                    break;
                }
            case TID_int16:
                { 
                    int16 val;
                    if (!imcs_next_int16(usrfctx->iterators[i], &val)) { 
                        SRF_RETURN_DONE(funcctx);      
                    }
                    usrfctx->values[i] = (usrfctx->iterators[i]->flags & FLAG_TRANSLATED) 
                        ? PointerGetDatum(cstring_to_text_with_len(imcs_dict_code_map[(uint16)val]->key.val, imcs_dict_code_map[(uint16)val]->key.len))
                        : Int16GetDatum(val);
                    break;
                }
            case TID_int32:
            case TID_date:
                { 
                    int32 val;
                    if (!imcs_next_int32(usrfctx->iterators[i], &val)) { 
                        SRF_RETURN_DONE(funcctx);      
                    }
                    usrfctx->values[i] = (usrfctx->iterators[i]->flags & FLAG_TRANSLATED) 
                        ? PointerGetDatum(cstring_to_text_with_len(imcs_dict_code_map[val]->key.val, imcs_dict_code_map[val]->key.len))
                        : Int32GetDatum(val);
                    break;
                }
            case TID_int64:
            case TID_time:                                
            case TID_timestamp:                           
            case TID_money:                           
                { 
                    int64 val;
                    if (!imcs_next_int64(usrfctx->iterators[i], &val)) { 
                        SRF_RETURN_DONE(funcctx);      
                    }
                    usrfctx->values[i] = Int64GetDatum(val);
                    break;
                }
            case TID_float:
                { 
                    float val;
                    if (!imcs_next_float(usrfctx->iterators[i], &val)) { 
                        SRF_RETURN_DONE(funcctx);      
                    }
                    usrfctx->values[i] = Float4GetDatum(val);
                    break;
                }
            case TID_double:
                { 
                    double val;
                    if (!imcs_next_double(usrfctx->iterators[i], &val)) { 
                        SRF_RETURN_DONE(funcctx);      
                    }
                    usrfctx->values[i] = Float8GetDatum(val);
                    break;
                }
            case TID_char:
                { 
                    char* val = (char*)imcs_alloc(usrfctx->iterators[i]->elem_size+1);
                    if (!imcs_next_char(usrfctx->iterators[i], val)) { 
                        SRF_RETURN_DONE(funcctx);      
                    }
                    val[usrfctx->iterators[i]->elem_size] = '\0';
                    usrfctx->values[i] = CStringGetTextDatum(val);
                    break;
                }
            default:
                Assert(false);
            } 
        }
    }
    tuple = heap_form_tuple(usrfctx->desc, usrfctx->values, usrfctx->nulls);
    elem = HeapTupleGetDatum(tuple);
    imcs_project_result_cache = elem;
    SRF_RETURN_NEXT(funcctx, elem);    
}

typedef struct { 
    imcs_iterator_h iterators[2];
    Datum           values[2];
    bool            nulls[2];
    TupleDesc       desc;
} imcs_project_agg_context;

Datum cs_project_agg(PG_FUNCTION_ARGS)
{
    HeapTupleHeader t = PG_ARGISNULL(0) ? NULL : PG_GETARG_HEAPTUPLEHEADER(0); 
    imcs_iterator_h positions = PG_ARGISNULL(1) ? (imcs_iterator_h)NULL : (imcs_iterator_h)PG_GETARG_POINTER(1);
    bool disable_caching = PG_GETARG_BOOL(2);
    FuncCallContext* funcctx;
    imcs_project_agg_context* usrfctx;
    int i, size;
    Datum elem;
    HeapTuple tuple;
    bytea* arr;
    void* dst;
    bool is_null = false;
    bool is_first_call = SRF_IS_FIRSTCALL();
    MemoryContext oldcontext;
    TupleDesc attr_desc;
    Oid	timeseries_oid;
    
    if (t == NULL) { 
        PG_RETURN_NULL();
    }
    if (!disable_caching && imcs_project_caching) { 
        if (is_first_call) { 
            if (imcs_project_call_count == 1) { 
                /* cs_project_agg() is redundantly called second time in (cs_project_agg(...)).* expression - PostgreSQL behavour */
                /* This condition may be true also when cs_project() is used twice in the ssame query - disable caching in this case */ 
                imcs_project_redundant_calls = 2; /* number of attributes of projected tuple */
                funcctx = SRF_FIRSTCALL_INIT(); /* context will be needed for SRF_RETURN_NEXT */
            } else if (imcs_project_redundant_calls != 0 && imcs_project_call_count >= imcs_project_redundant_calls) { 
                /* iteratation is not yet completed, but first function call is encountered */
                imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "Multiple usage of cs_project_agg in select column list are not supported");
            }
        }
        if (imcs_project_redundant_calls != 0) { 
            Assert(imcs_project_call_count > 0);
            if (imcs_project_call_count < 2 && !is_first_call) {
                imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "Multiple usage of cs_project_agg in select column list are not supported");
            }
            if (imcs_project_call_count++ % 2 != 0) { /* 2 - number of attributes */
                funcctx = SRF_PERCALL_SETUP();
                if (imcs_project_result_cache) { 
                    SRF_RETURN_NEXT(funcctx, imcs_project_result_cache);    
                } else { 
                    if (imcs_project_call_count % 2 == 0) { /* end of traversal */
                        imcs_project_redundant_calls = 0;
                        imcs_project_call_count = 0;
                    }
                    SRF_RETURN_DONE(funcctx);   
                }
            }
        } else { 
            imcs_project_call_count += 1;
        }
    }
    if (is_first_call)
    {
        IMCS_TRACE(project_agg);
        funcctx = SRF_FIRSTCALL_INIT();
        imcs_project_call_count = 1; /* initialize counter */
        attr_desc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(t), HeapTupleHeaderGetTypMod(t));
        timeseries_oid = TypenameGetTypid("timeseries");
        if (attr_desc->natts != 2) { 
            imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "Expect record with two columns");
        }
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);       
        usrfctx = (imcs_project_agg_context*)palloc(sizeof(imcs_project_agg_context));
        get_call_result_type(fcinfo, NULL, &usrfctx->desc);
        funcctx->user_fctx = usrfctx;
        for (i = 0; i < 2; i++) { 
            Form_pg_attribute attr = attr_desc->attrs[i];
            Datum datum = GetAttributeByNum(t, attr->attnum, &usrfctx->nulls[i]);
            if (attr->atttypid != timeseries_oid) {
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "Expect column with timeseries type");
            } else if (usrfctx->nulls[i]) { 
                is_null = true;
                break;
            } else { 
                imcs_iterator_h attr_iterator = (imcs_iterator_h)DatumGetPointer(datum);   
                if (positions != NULL) { 
                    if (i == 0) {
                        imcs_iterator_h tee_iterators[2];
                        imcs_tee(tee_iterators, positions);
                        usrfctx->iterators[i] = imcs_project(attr_iterator, tee_iterators[0]);
                        positions = tee_iterators[1];
                    } else { 
                        usrfctx->iterators[i] = imcs_project(attr_iterator, positions);
                    }
                } else { 
                    usrfctx->iterators[i] = imcs_operand(attr_iterator);
                } 
            }
        }  
        ReleaseTupleDesc(attr_desc);
        MemoryContextSwitchTo(oldcontext);      
    }
    funcctx = SRF_PERCALL_SETUP();
    imcs_project_result_cache = 0; /* 0 means end of set */
    if (is_null) { 
        SRF_RETURN_DONE(funcctx);      
    }
    usrfctx = (imcs_project_agg_context*)funcctx->user_fctx;
    switch (usrfctx->iterators[0]->elem_type) {
      case TID_int8:
          { 
              int8 val;
              if (!imcs_next_int8(usrfctx->iterators[0], &val)) { 
                  SRF_RETURN_DONE(funcctx);      
              }
              usrfctx->values[0] = Float8GetDatum((double)val);
              break;
          }
      case TID_int16:
          { 
              int16 val;
              if (!imcs_next_int16(usrfctx->iterators[0], &val)) { 
                  SRF_RETURN_DONE(funcctx);      
              }
              usrfctx->values[0] = Float8GetDatum((double)val);
              break;
          }
      case TID_int32:
      case TID_date:
          { 
              int32 val;
              if (!imcs_next_int32(usrfctx->iterators[0], &val)) { 
                  SRF_RETURN_DONE(funcctx);      
              }
              usrfctx->values[0] = Float8GetDatum((double)val);
              break;
          }
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
          { 
              int64 val;
              if (!imcs_next_int64(usrfctx->iterators[0], &val)) { 
                  SRF_RETURN_DONE(funcctx);      
              }
              usrfctx->values[0] = Float8GetDatum((double)val);
              break;
          }
      case TID_float:
          { 
              float val;
              if (!imcs_next_float(usrfctx->iterators[0], &val)) { 
                  SRF_RETURN_DONE(funcctx);      
              }
              usrfctx->values[0] = Float8GetDatum((double)val);
              break;
          }
      case TID_double:
          { 
              double val;
              if (!imcs_next_double(usrfctx->iterators[0], &val)) { 
                  SRF_RETURN_DONE(funcctx);      
              }
              usrfctx->values[0] = Float8GetDatum(val);
              break;
          }
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "Scalar value is expected");
    }

    size = VARHDRSZ + usrfctx->iterators[1]->elem_size;
    arr = (bytea*)palloc(size); 
    SET_VARSIZE(arr, size);
    dst = VARDATA(arr);
    usrfctx->values[1] = PointerGetDatum(arr);

    switch (usrfctx->iterators[1]->elem_type) {
      case TID_int8:
        if (!imcs_next_int8(usrfctx->iterators[1], (int8*)dst)) { 
            SRF_RETURN_DONE(funcctx);      
        }
        break;
      case TID_int16:
        if (!imcs_next_int16(usrfctx->iterators[1], (int16*)dst)) { 
            SRF_RETURN_DONE(funcctx);      
        }
        break;
      case TID_int32:
      case TID_date:
        if (!imcs_next_int32(usrfctx->iterators[1], (int32*)dst)) { 
            SRF_RETURN_DONE(funcctx);      
        }
        break;
      case TID_int64:
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        if (!imcs_next_int64(usrfctx->iterators[1], (int64*)dst)) { 
            SRF_RETURN_DONE(funcctx);      
        }
        break;
      case TID_float:
        if (!imcs_next_float(usrfctx->iterators[1], (float*)dst)) { 
            SRF_RETURN_DONE(funcctx);      
        }
        break;
      case TID_double:
        if (!imcs_next_double(usrfctx->iterators[1], (double*)dst)) { 
            SRF_RETURN_DONE(funcctx);      
        }
        break;
      case TID_char:
        if (!imcs_next_char(usrfctx->iterators[1], VARDATA(arr))) { 
            SRF_RETURN_DONE(funcctx);      
        }
        break;
      default:
        Assert(false);
    }
    tuple = heap_form_tuple(usrfctx->desc, usrfctx->values, usrfctx->nulls);
    elem = HeapTupleGetDatum(tuple);
    imcs_project_result_cache = elem;
    SRF_RETURN_NEXT(funcctx, elem);    
}

static imcs_elem_typeid_t imcs_oid_to_typeid(int oid)
{
    switch (oid) { 
      case BOOLOID:
      case CHAROID:
        return TID_int8;
      case INT2OID:
        return TID_int16;
      case INT4OID:
        return TID_int32;
      case DATEOID:
        return TID_date;
      case INT8OID:
        return TID_int64;
      case TIMEOID:
        return TID_time;
      case TIMESTAMPOID:
        return TID_timestamp;
      case CASHOID:
        return TID_money;
      case FLOAT4OID:
        return TID_float;
      case FLOAT8OID:
        return TID_double;
      case BPCHAROID:
      case VARCHAROID:
      case TEXTOID:
        return TID_char;
      default:
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "Unsupported type oid %d", oid);
    }
    return TID_int8; 
}
    
Datum columnar_store_load(PG_FUNCTION_ARGS)
{
    char const* table_name = PG_GETARG_CSTRING(0);
    int id_attnum = PG_GETARG_INT32(1);
    int timestamp_attnum = PG_GETARG_INT32(2);
    bool already_sorted = PG_GETARG_BOOL(3);
    char const* filter = PG_GETARG_CSTRING(4);
    int table_name_len = strlen(table_name);
    int i, j, n_attrs;
    int64 n_records = 0;
    Oid* attr_type_oid;
    imcs_elem_typeid_t* attr_type;
    int* attr_size;
    char** attr_name;
    char** cs_id_prefix;
    int* cs_id_prefix_len;
    SPIPlanPtr plan;
    Portal portal;
    bool isnull;
    int cs_id_max_len = 256;
    char* cs_id = (char*)palloc(cs_id_max_len);
    int rc;
    int len;
    text* t;
    Datum* values;
    bool* nulls;
    imcs_timeseries_t* ts;
    char stmt[MAX_SQL_STMT_LEN];

    SPI_connect();
    
    sprintf(stmt, "select attname,atttypid,attlen,atttypmod from pg_class,pg_attribute,pg_type where pg_class.relname='%s' and pg_class.oid=pg_attribute.attrelid and pg_attribute.atttypid=pg_type.oid and attnum>0 order by attnum", table_name);

    rc = SPI_execute(stmt, true, 0);
    if (rc != SPI_OK_SELECT) { 
        elog(ERROR, "Select failed with status %d", rc);
    }
    n_attrs = SPI_processed;
    if (n_attrs == 0) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Table %s doesn't exist", table_name); 
    }
    attr_type_oid = (imcs_elem_typeid_t*)palloc(n_attrs*sizeof(imcs_elem_typeid_t));
    attr_type = (Oid*)palloc(n_attrs*sizeof(Oid));
    attr_size = (int*)palloc(n_attrs*sizeof(int));
    attr_name = (char**)palloc(n_attrs*sizeof(char*));
    cs_id_prefix = (char**)palloc(n_attrs*sizeof(char*));
    cs_id_prefix_len = (int*)palloc(n_attrs*sizeof(int));

    values = (Datum*)palloc(sizeof(Datum)*n_attrs);
    nulls = (bool*)palloc(sizeof(bool)*n_attrs);

    for (i = 0; i < n_attrs; i++) {
        HeapTuple spi_tuple = SPI_tuptable->vals[i];
        TupleDesc spi_tupdesc = SPI_tuptable->tupdesc;
        attr_name[i] = SPI_getvalue(spi_tuple, spi_tupdesc, 1);
        attr_type_oid[i] = DatumGetObjectId(SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull));
        attr_type[i] = imcs_oid_to_typeid(attr_type_oid[i]);
        attr_size[i] = DatumGetInt16(SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull));
        if (attr_size[i] < 0) { /* attlen=-1: varying type, extract size from atttypmod */
            attr_size[i] = DatumGetInt32(SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull)) - VARHDRSZ;
            if (attr_size[i] < 0 && id_attnum != i+1) {
                if (imcs_dict_size == 0) { 
                    imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Size of attribute %s is not statically known", attr_name[i]); 
                }
            }
        }
        cs_id_prefix_len[i] = table_name_len + strlen(attr_name[i]) + 1;
        cs_id_prefix[i] = (char*)palloc(cs_id_prefix_len[i]+1);
        sprintf(cs_id_prefix[i], "%s-%s", table_name, attr_name[i]);
        SPI_freetuple(spi_tuple);
    }
    SPI_freetuptable(SPI_tuptable);

    ts = imcs_get_timeseries(cs_id_prefix[timestamp_attnum-1], attr_type[timestamp_attnum-1], true, attr_size[timestamp_attnum-1], true);
    
    if (ts->count != 0 && filter == NULL) { /* always try to load records when filter is specified */
        SPI_finish();
        PG_RETURN_INT64(0);
    }    
    if (id_attnum != 0) { /* in case of single timeseries, dummy hash entry to check if timeseries was already initialized is not needed: use entry for timestamp */
        ts->count = 1;
    }

    len = sprintf(stmt, "select * from %s", table_name);
    if (filter != NULL) { 
        len += sprintf(stmt + len, " where %s", filter);
    } 
    if (!already_sorted) { 
        sprintf(stmt + len, " order by %s", attr_name[timestamp_attnum-1]);
    }
    plan = SPI_prepare(stmt, 0, NULL);	
    portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);


    while (true) {
        SPI_cursor_fetch(portal, true, 1);
        if (SPI_processed) { 
            HeapTuple spi_tuple = SPI_tuptable->vals[0];
            TupleDesc spi_tupdesc = SPI_tuptable->tupdesc;
            char* id = NULL;
            char* id_cstr = NULL;
            int id_len = 0;
            n_records += 1;
            heap_deform_tuple(spi_tuple, spi_tupdesc, values, nulls);
            if (id_attnum != 0) { 
                if (nulls[id_attnum-1]) {
                    imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "Timseries identifier can not be NULL");
                }
                if (attr_type[id_attnum-1] != TID_char) { 
                    id = id_cstr = SPI_getvalue(spi_tuple, spi_tupdesc, id_attnum);
                    id_len = strlen(id);
                } else { 
                    t = DatumGetTextP(values[id_attnum-1]);
                    id = (char*)VARDATA(t);
                    id_len = VARSIZE(t) - VARHDRSZ;
                    if (attr_type_oid[id_attnum-1] == BPCHAROID) { 
                        while (id_len != 0 && id[id_len-1] == ' ') { 
                            id_len -= 1;
                        }
                    }
                }
            }
            i = timestamp_attnum - 1; /* start with timestamp because it the only attribute which append can fail because of out-of-order date */
            for (j = 0; j < n_attrs; j++, i = (i + 1) % n_attrs) {
                if (nulls[i]) {
                    if (imcs_substitute_nulls) { 
                        values[i] = 0;
                    } else { 
                        imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "NULL values are not supported by columnar store");
                    }
                }
                if (i+1 != id_attnum) { 
                    bool is_timestamp = i+1 == timestamp_attnum;
                    char *str;
                    if (id_attnum != 0) { 
                        int prefix_len = cs_id_prefix_len[i];
                        while (cs_id_max_len < prefix_len + id_len + 2) { 
                            cs_id_max_len *= 2;
                        }
                        pfree(cs_id);
                        cs_id = (char*)palloc(cs_id_max_len);
                        
                        memcpy(cs_id, cs_id_prefix[i], prefix_len);
                        cs_id[prefix_len] = '-';
                        memcpy(cs_id + prefix_len + 1, id, id_len);
                        cs_id[prefix_len + id_len + 1] = '\0';
                    } else { 
                        cs_id = cs_id_prefix[i];
                    }
                    ts = imcs_get_timeseries(cs_id, attr_type[i], is_timestamp, attr_size[i], true);
                    switch (attr_type[i]) { 
                      case TID_int8:
                        imcs_append_int8(ts, DatumGetChar(values[i]));
                        break;
                      case TID_int16:
                        imcs_append_int16(ts, DatumGetInt16(values[i]));
                        break;
                      case TID_int32:
                      case TID_date:
                        imcs_append_int32(ts, DatumGetInt32(values[i]));
                        break;
                      case TID_int64:
                      case TID_time:
                      case TID_timestamp:
                      case TID_money:                           
                        imcs_append_int64(ts, DatumGetInt64(values[i]));
                        break;
                      case TID_float:
                        imcs_append_float(ts, DatumGetFloat4(values[i]));
                        break;
                      case TID_double:
                        imcs_append_double(ts, DatumGetFloat8(values[i]));
                        break;                    
                      case TID_char:
                        if (attr_size[i] < 0) { /* varying string */
                            bool found;
                            imcs_dict_key_t key;
                            imcs_dict_entry_t* entry;
                            if (nulls[i]) { /* substitute NULL with empty string */
                                key.val = NULL;
                                key.len = 0;
                            } else { 
                                t = DatumGetTextP(values[i]);
                                key.val = (char*)VARDATA(t);
                                key.len = VARSIZE(t) - VARHDRSZ;
                            }                            
                            entry = (imcs_dict_entry_t*)hash_search(imcs_dict, &key, HASH_ENTER, &found);
                            if (!found) { 
                                entry->code = hash_get_num_entries(imcs_dict);
                                if (entry->code >= imcs_dict_size) {  
                                    imcs_ereport(ERRCODE_OUT_OF_MEMORY, "IMSC dictionary limit exceeded");
                                }   
                                imcs_dict_code_map[entry->code] = entry;
                            }
                            if (imcs_dict_size <= IMCS_SMALL_DICTIONARY) { 
                                imcs_append_int16(ts, (int16)entry->code);
                            } else { 
                                imcs_append_int32(ts, (int32)entry->code);
                            }
                        } else { 
                            if (nulls[i]) { /* substitute NULL with empty string */
                                imcs_append_char(ts, NULL, 0);
                            } else { 
                                t = DatumGetTextP(values[i]);
                                str = (char*)VARDATA(t);
                                len = VARSIZE(t) - VARHDRSZ;
                                if (attr_type_oid[i] == BPCHAROID) { 
                                    while (len != 0 && str[len-1] == ' ') { 
                                        len -= 1;
                                    }
                                }
                                if (len > attr_size[i]) { 
                                    imcs_ereport(ERRCODE_STRING_DATA_LENGTH_MISMATCH, "String length %d is larger then element size %d for attribute %s", len, attr_size[i], attr_name[i]);             
                                }
                                imcs_append_char(ts, str, len);
                            }
                        }
                        break;
                      default:
                        Assert(false);
                    }
                }
            }
            if (id_cstr != NULL) { 
                pfree(id_cstr);
            }
            SPI_freetuple(spi_tuple);
            SPI_freetuptable(SPI_tuptable);
        } else {
            break;
        }
    }
    SPI_cursor_close(portal);
    SPI_finish();
    PG_RETURN_INT64(n_records);
}    

Datum columnar_store_load_column(PG_FUNCTION_ARGS)
{
    char const* table_name = PG_GETARG_CSTRING(0);
    char const* column_name = PG_GETARG_CSTRING(3);
    int id_attnum = PG_GETARG_INT32(1);
    int timestamp_attnum = PG_GETARG_INT32(2);
    int table_name_len = strlen(table_name);
    int i, n_attrs;
    int64 n_records = 0;
    Oid* attr_type_oid;
    imcs_elem_typeid_t* attr_type;
    int* attr_size;
    char** attr_name;
    char* cs_id_prefix;
    int cs_id_prefix_len;
    SPIPlanPtr plan;
    Portal portal;
    bool isnull;
    int cs_id_max_len = 256;
    char* cs_id = (char*)palloc(cs_id_max_len);
    int rc;
    int len;
    text* t;
    Datum value;
    int column_id = -1;
    imcs_timeseries_t* ts;
    char stmt[MAX_SQL_STMT_LEN];

    SPI_connect();
    
    sprintf(stmt, "select attname,atttypid,attlen,atttypmod from pg_class,pg_attribute,pg_type where pg_class.relname='%s' and pg_class.oid=pg_attribute.attrelid and pg_attribute.atttypid=pg_type.oid and attnum>0 order by attnum", table_name);

    rc = SPI_execute(stmt, true, 0);
    if (rc != SPI_OK_SELECT) { 
        elog(ERROR, "Select failed with status %d", rc);
    }
    n_attrs = SPI_processed;
    if (n_attrs == 0) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Table %s doesn't exist", table_name); 
    }
    attr_type_oid = (imcs_elem_typeid_t*)palloc(n_attrs*sizeof(imcs_elem_typeid_t));
    attr_type = (Oid*)palloc(n_attrs*sizeof(Oid));
    attr_size = (int*)palloc(n_attrs*sizeof(int));
    attr_name = (char**)palloc(n_attrs*sizeof(char*));

    for (i = 0; i < n_attrs; i++) {
        HeapTuple spi_tuple = SPI_tuptable->vals[i];
        TupleDesc spi_tupdesc = SPI_tuptable->tupdesc;
        attr_name[i] = SPI_getvalue(spi_tuple, spi_tupdesc, 1);
        attr_type_oid[i] = DatumGetObjectId(SPI_getbinval(spi_tuple, spi_tupdesc, 2, &isnull));
        attr_type[i] = imcs_oid_to_typeid(attr_type_oid[i]);
        attr_size[i] = DatumGetInt16(SPI_getbinval(spi_tuple, spi_tupdesc, 3, &isnull));
        if (attr_size[i] < 0) { /* attlen=-1: varying type, extract size from atttypmod */
            attr_size[i] = DatumGetInt32(SPI_getbinval(spi_tuple, spi_tupdesc, 4, &isnull)) - VARHDRSZ;
            if (attr_size[i] < 0 && id_attnum != i+1) {
                if (imcs_dict_size == 0) { 
                    imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Size of attribute %s is not statically known", attr_name[i]); 
                }
            }
        }
        if (strcmp(attr_name[i], column_name) == 0) { 
            column_id = i;            
            cs_id_prefix_len = table_name_len + strlen(attr_name[i]) + 1;
            cs_id_prefix = (char*)palloc(cs_id_prefix_len+1);
            sprintf(cs_id_prefix, "%s-%s", table_name, attr_name[i]);
        }
        SPI_freetuple(spi_tuple);
    }
    SPI_freetuptable(SPI_tuptable);
    if (column_id < 0 || column_id == id_attnum-1 || column_id+1 == timestamp_attnum-1) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Column %s of table %s can not be individually imported", column_name, table_name); 
    }
    if (id_attnum != 0) { 
        len = sprintf(stmt, "select %s,%s from %s order by %s", column_name, attr_name[id_attnum-1], table_name, attr_name[timestamp_attnum-1]);
    } else { 
        len = sprintf(stmt, "select %s from %s order by %s", column_name, table_name, attr_name[timestamp_attnum-1]);
    }
    plan = SPI_prepare(stmt, 0, NULL);	
    portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);

    while (true) {
        SPI_cursor_fetch(portal, true, 1);
        if (SPI_processed) { 
            HeapTuple spi_tuple = SPI_tuptable->vals[0];
            TupleDesc spi_tupdesc = SPI_tuptable->tupdesc;
            char* id = NULL;
            int id_len = 0;
            n_records += 1;

            value = SPI_getbinval(spi_tuple, spi_tupdesc, 1, &isnull);
            if (isnull) {
                if (imcs_substitute_nulls) { 
                    value = 0;
                } else { 
                    imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "NULL values are not supported by columnar store");
                }
            }
            if (id_attnum != 0) { 
                id = SPI_getvalue(spi_tuple, spi_tupdesc, 2);
                id_len = strlen(id);
                if (attr_type_oid[id_attnum-1] == BPCHAROID) { 
                    while (id_len != 0 && id[id_len-1] == ' ') { 
                        id_len -= 1;
                    }
                }
                while (cs_id_max_len < cs_id_prefix_len + id_len + 2) { 
                    cs_id_max_len *= 2;
                }
                pfree(cs_id);
                cs_id = (char*)palloc(cs_id_max_len);
                
                memcpy(cs_id, cs_id_prefix, cs_id_prefix_len);
                cs_id[cs_id_prefix_len] = '-';
                memcpy(cs_id + cs_id_prefix_len + 1, id, id_len);
                cs_id[cs_id_prefix_len + id_len + 1] = '\0';
                pfree(id);
            } else { 
                cs_id = cs_id_prefix;
            }
            ts = imcs_get_timeseries(cs_id, attr_type[column_id], false, attr_size[column_id], true);
            switch (attr_type[column_id]) { 
            case TID_int8:
                imcs_append_int8(ts, DatumGetChar(value));
                break;
            case TID_int16:
                imcs_append_int16(ts, DatumGetInt16(value));
                break;
            case TID_int32:
            case TID_date:
                imcs_append_int32(ts, DatumGetInt32(value));
                break;
            case TID_int64:
            case TID_time:
            case TID_timestamp:
            case TID_money:                           
                imcs_append_int64(ts, DatumGetInt64(value));
                break;
            case TID_float:
                imcs_append_float(ts, DatumGetFloat4(value));
                break;
            case TID_double:
                imcs_append_double(ts, DatumGetFloat8(value));
                break;                    
            case TID_char:
                if (attr_size[column_id] < 0) { /* varying string */
                    bool found;
                    imcs_dict_key_t key;
                    imcs_dict_entry_t* entry;
                    if (isnull) { /* substitute NULL with empty string */
                        key.val = NULL;
                        key.len = 0;
                    } else { 
                        t = DatumGetTextP(value);
                        key.val = (char*)VARDATA(t);
                        key.len = VARSIZE(t) - VARHDRSZ;
                    }                            
                    entry = (imcs_dict_entry_t*)hash_search(imcs_dict, &key, HASH_ENTER, &found);
                    if (!found) { 
                        entry->code = hash_get_num_entries(imcs_dict);
                        if (entry->code >= imcs_dict_size) {  
                            imcs_ereport(ERRCODE_OUT_OF_MEMORY, "IMSC dictionary limit exceeded");
                        }   
                        imcs_dict_code_map[entry->code] = entry;
                    }
                    if (imcs_dict_size <= IMCS_SMALL_DICTIONARY) { 
                        imcs_append_int16(ts, (int16)entry->code);
                    } else { 
                        imcs_append_int32(ts, (int32)entry->code);
                    }
                } else { 
                    if (isnull) { /* substitute NULL with empty string */
                        imcs_append_char(ts, NULL, 0);
                    } else { 
                        char* str;
                        t = DatumGetTextP(value);
                        str = (char*)VARDATA(t);
                        len = VARSIZE(t) - VARHDRSZ;
                        if (attr_type_oid[column_id] == BPCHAROID) { 
                            while (len != 0 && str[len-1] == ' ') { 
                                len -= 1;
                            }
                        }
                        if (len > attr_size[column_id]) { 
                            imcs_ereport(ERRCODE_STRING_DATA_LENGTH_MISMATCH, "String length %d is larger then element size %d for attribute %s", len, attr_size[i], attr_name[i]);             
                        }
                        imcs_append_char(ts, str, len);
                    }
                }
                break;
            default:
                Assert(false);
            }
        } else { 
            break;
        }
    }
    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);
    SPI_finish();
    PG_RETURN_INT64(n_records);
}    

Datum columnar_store_insert_trigger(PG_FUNCTION_ARGS)
{     
    TriggerData* trigger_data;
    Trigger* trigger;
    char const* table_name;
    int id_attnum;
    int timestamp_attnum;
    int table_name_len;
    int i, n_attrs;
    Oid* attr_type_oid;
    imcs_elem_typeid_t* attr_type;
    int* attr_size;
    char** attr_name;
    char** cs_id_prefix;
    int* cs_id_prefix_len;
    int cs_id_max_len = 256;
    char* cs_id = (char*)palloc(cs_id_max_len);
    text* t;
    Datum* values;
    bool* nulls;
    imcs_timeseries_t* ts;
    char* id = NULL;
    int id_len = 0;
    int len;
    char id_buf[32];

    if (!CALLED_AS_TRIGGER(fcinfo)) { 
        imcs_ereport(ERRCODE_TRIGGERED_ACTION_EXCEPTION, "columnar_store_insert_trigger can be called only by trigger"); 
    }
    trigger_data = (TriggerData*)fcinfo->context;
    trigger = trigger_data->tg_trigger;
    table_name = trigger->tgargs[0];
    id_attnum = atoi(trigger->tgargs[1]);
    timestamp_attnum = atoi(trigger->tgargs[2]);
    table_name_len = strlen(table_name);
    n_attrs = trigger->tgnargs/3-1;

    attr_type_oid = (imcs_elem_typeid_t*)palloc(n_attrs*sizeof(imcs_elem_typeid_t));
    attr_type = (Oid*)palloc(n_attrs*sizeof(Oid));
    attr_size = (int*)palloc(n_attrs*sizeof(int));
    attr_name = (char**)palloc(n_attrs*sizeof(char*));
    cs_id_prefix = (char**)palloc(n_attrs*sizeof(char*));
    cs_id_prefix_len = (int*)palloc(n_attrs*sizeof(int));

    values = (Datum*)palloc(sizeof(Datum)*n_attrs);
    nulls = (bool*)palloc(sizeof(bool)*n_attrs);

    for (i = 0; i < n_attrs; i++) {
        attr_name[i] = trigger->tgargs[i*3+3];
        attr_type_oid[i] = atoi(trigger->tgargs[i*3+4]);
        attr_type[i] = imcs_oid_to_typeid(attr_type_oid[i]);
        attr_size[i] = atoi(trigger->tgargs[i*3+5]);
        cs_id_prefix_len[i] = table_name_len + strlen(attr_name[i]) + 1;
        cs_id_prefix[i] = (char*)palloc(cs_id_prefix_len[i]+1);
        sprintf(cs_id_prefix[i], "%s-%s", table_name, attr_name[i]);
    }

    ts = imcs_get_timeseries(cs_id_prefix[timestamp_attnum-1], attr_type[timestamp_attnum-1], true, attr_size[timestamp_attnum-1], true);
    
    if (id_attnum != 0) { /* in case of single timeseries, dummy hash entry to check if timeseries was already initialized is not needed: use entry for timestamp */
        ts->count = 1;
    }
    heap_deform_tuple(trigger_data->tg_trigtuple, trigger_data->tg_relation->rd_att, values, nulls);
    if (id_attnum != 0) { 
        if (nulls[id_attnum-1]) {
            imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "Timseries identifier can not be NULL");
        }
        switch (attr_type[id_attnum-1]) { 
          case TID_int8:
            id_len = sprintf(id_buf, "%d", DatumGetChar(values[id_attnum-1]));
            break;
          case TID_int16:
            id_len = sprintf(id_buf, "%d", DatumGetInt16(values[id_attnum-1]));
            break;
          case TID_int32:
            id_len = sprintf(id_buf, "%d", DatumGetInt32(values[id_attnum-1]));
            break;
          case TID_int64:
            id_len = sprintf(id_buf, "%lld", (long long)DatumGetInt64(values[id_attnum-1]));
            break;             
          case TID_char:
            t = DatumGetTextP(values[id_attnum-1]);
            id = (char*)VARDATA(t);
            id_len = VARSIZE(t) - VARHDRSZ;
            if (attr_type_oid[id_attnum-1] == BPCHAROID) { 
                while (id_len != 0 && id[id_len-1] == ' ') { 
                    id_len -= 1;
                }
            }
            break;
          default:
            imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Unsupported timeseries ID type %d", attr_type_oid[id_attnum-1]); 
        }
    }
    for (i = 0; i < n_attrs; i++) {
        if (nulls[i]) {
            if (imcs_substitute_nulls) { 
                values[i] = 0;
            } else { 
                imcs_ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED, "NULL values are not supported by columnar store");
            }
        }
        if (i+1 != id_attnum) { 
            bool is_timestamp = i+1 == timestamp_attnum;
            char *str;
            if (id_attnum != 0) { 
                int prefix_len = cs_id_prefix_len[i];
                while (cs_id_max_len < prefix_len + id_len + 2) { 
                    cs_id_max_len *= 2;
                    pfree(cs_id);
                    cs_id = (char*)palloc(cs_id_max_len);
                }
                memcpy(cs_id, cs_id_prefix[i], prefix_len);
                cs_id[prefix_len] = '-';
                memcpy(cs_id + prefix_len + 1, id, id_len);
                cs_id[prefix_len + id_len + 1] = '\0';
            } else { 
                cs_id = cs_id_prefix[i];
            }
            ts = imcs_get_timeseries(cs_id, attr_type[i], is_timestamp, attr_size[i], true);
            switch (attr_type[i]) { 
              case TID_int8:
                imcs_append_int8(ts, DatumGetChar(values[i]));
                break;
              case TID_int16:
                imcs_append_int16(ts, DatumGetInt16(values[i]));
                break;
              case TID_int32:
              case TID_date:
                imcs_append_int32(ts, DatumGetInt32(values[i]));
                break;
              case TID_int64:
              case TID_time:
              case TID_timestamp:
              case TID_money:                           
                imcs_append_int64(ts, DatumGetInt64(values[i]));
                break;
              case TID_float:
                imcs_append_float(ts, DatumGetFloat4(values[i]));
                break;
              case TID_double:
                imcs_append_double(ts, DatumGetFloat8(values[i]));
                break;                    
              case TID_char:
                if (nulls[i]) { /* substitute NULL with empty string */
                    imcs_append_char(ts, NULL, 0);
                } else { 
                    t = DatumGetTextP(values[i]);
                    str = (char*)VARDATA(t);
                    len = VARSIZE(t) - VARHDRSZ;
                    if (attr_type_oid[i] == BPCHAROID) { 
                        while (len != 0 && str[len-1] == ' ') { 
                            len -= 1;
                        }
                    }
                    if (len > attr_size[i]) { 
                        imcs_ereport(ERRCODE_STRING_DATA_LENGTH_MISMATCH, "String length %d is larger then element size %d for attribute %s", len, attr_size[i], attr_name[i]);
                    }
                    imcs_append_char(ts, str, len);
                }
                break;
              default:
                Assert(false);
            }
        }
    }
    PG_RETURN_POINTER(NULL);
}    

Datum cs_cut(PG_FUNCTION_ARGS)
{
    bytea* str = PG_GETARG_BYTEA_P(0);
    char* src = (char*)VARDATA(str);
    size_t len = VARSIZE(str) - VARHDRSZ;
    char const* format = PG_GETARG_CSTRING(1);
    char const* fmt = format;
    TupleDesc desc;
    Datum values[MAX_CUT_VALUES];
    bool nulls[MAX_CUT_VALUES];
    int elem_sizes[MAX_CUT_VALUES];
    imcs_elem_typeid_t elem_types[MAX_CUT_VALUES];
    size_t pos = 0;
    int i, n_values;
    imcs_key_t value;
    
    for (i = 0; *fmt != '\0'; i++) { 
        int n;
        char type_letter;
        int elem_size;
        imcs_elem_typeid_t elem_type = TID_char;
        if (sscanf(fmt, "%c%d%n", &type_letter, &elem_size, &n) != 2) { 
            imcs_ereport(ERRCODE_SYNTAX_ERROR, "failed to parse format string '%s'", fmt);
        }
        switch (type_letter) { 
          case 'i':
          case 'I':
            switch (elem_size) { 
              case 1:
                elem_type = TID_int8;
                break;
              case 2:
                elem_type = TID_int16;
                break;
              case 4:
                elem_type = TID_int32;
                break;
              case 8:
                elem_type = TID_int64;
                break;
              default:
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }
            break;
          case 'd':
          case 'D':
            elem_type = TID_date;
            if (elem_size != 4)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 'm':
          case 'M':
            elem_type = TID_money;
            if (elem_size != 8)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 't':
            elem_type = TID_time;
            if (elem_size != 8)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 'T':
            elem_type = TID_timestamp;
            if (elem_size != 8)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 'f':
          case 'F':
            switch (elem_size) { 
              case 4:
                elem_type = TID_float;
                break;
              case 8:
                elem_type = TID_double;
                break;
              default:
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }
            break;
          case 'C':
          case 'c':
            if (elem_size <= 0)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            elem_type = TID_char;
            break;
          default:
            imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type character %c", type_letter);
        }
        if (pos + elem_size > len) { 
            imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "too much values in format string %s", format);
        }
        fmt += n;
        pos += elem_size;
        if (i > MAX_CUT_VALUES) {
            imcs_ereport(ERRCODE_SYNTAX_ERROR, "Too much value in format string %s (limit is %d)", format, MAX_CUT_VALUES);
        }
        elem_types[i] = elem_type;
        elem_sizes[i] = elem_size;
    }
    if (pos != len) { 
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "too few values in format string %s", format);
    }
    n_values = i;
    desc = CreateTemplateTupleDesc(n_values, false);
    for (i = 0; i < n_values; i++) { 
        TupleDescInitEntry(desc, i+1, NULL, imcs_elem_type_to_oid[elem_types[i]], -1, 0);
        nulls[i] = false;
        if (elem_types[i] == TID_char) { 
            values[i] = CStringGetTextDatum(src);
        } else {             
            memcpy(&value, src, elem_sizes[i]);
            switch (elem_types[i]) { 
              case TID_int8:
                values[i] = Int8GetDatum(value.val_int8);
                break;
              case TID_int16:
                values[i] = Int16GetDatum(value.val_int16);
                break;
              case TID_int32:
              case TID_date:
                values[i] = Int32GetDatum(value.val_int32);
                break;
              case TID_int64:
              case TID_time:
              case TID_timestamp:
                values[i] = Int64GetDatum(value.val_int64);
                break;
              case TID_float:
                values[i] = Float4GetDatum(value.val_float);
                break;
              case TID_double:
                values[i] = Float8GetDatum(value.val_double);
                break;
              default:
                Assert(false);
            }
        }
        src += elem_sizes[i];
    }
    TupleDescGetAttInMetadata(desc);
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc, values, nulls)));
}

Datum cs_as(PG_FUNCTION_ARGS)
{
    bytea* str = PG_GETARG_BYTEA_P(0);
    char* src = (char*)VARDATA(str);
    size_t len = VARSIZE(str) - VARHDRSZ;
    char const* type = PG_GETARG_CSTRING(1);
    Oid	typid = TypenameGetTypid(lowerstr(type));
    imcs_key_t value;
    TupleDesc desc;
    int i, n;
    Datum* values;
    bool* nulls;

    if (typid == InvalidOid) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Type %s is not found", type); 
    }
    desc = lookup_rowtype_tupdesc(typid, -1);
    n = desc->natts;
    values = (Datum*)palloc(n*sizeof(Datum));
    nulls = (bool*)palloc(n*sizeof(bool));
    
    for (i = 0; i < n; i++) { 
        Form_pg_attribute attr = desc->attrs[i];
        nulls[i] = false;
        if (attr->atttypid == BPCHAROID) { 
            values[i] = CStringGetTextDatum(src);
            src += attr->atttypmod - VARHDRSZ;
        } else {             
            memcpy(&value, src, attr->attlen);
            switch (attr->atttypid) { 
              case CHAROID:
                values[i] = Int8GetDatum(value.val_int8);
                break;
              case INT2OID:
                values[i] = Int16GetDatum(value.val_int16);
                break;
              case INT4OID:
              case DATEOID:
                values[i] = Int32GetDatum(value.val_int32);
                break;
              case INT8OID:
              case TIMEOID:
              case TIMESTAMPOID:
              case CASHOID:
                values[i] = Int64GetDatum(value.val_int64);
                break;
              case FLOAT4OID:
                values[i] = Float4GetDatum(value.val_float);
                break;
              case FLOAT8OID:
                values[i] = Float8GetDatum(value.val_double);
                break;
              default:
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "Unsupported type %d", attr->atttypid);
            }
            src += attr->attlen;
        }
    }
    ReleaseTupleDesc(desc);
    if (src != (char*)VARDATA(str) + len) { 
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "bytea is not matching target type");
    }
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc, values, nulls)));
}

Datum cs_as_array(PG_FUNCTION_ARGS)
{
    bytea* str = PG_GETARG_BYTEA_P(0);
    char* src = (char*)VARDATA(str);
    size_t len = VARSIZE(str) - VARHDRSZ;
    char const* format = PG_GETARG_CSTRING(1);
    char const* fmt = format;
    Datum values[MAX_CUT_VALUES];
    int elem_sizes[MAX_CUT_VALUES];
    Oid type_out[MAX_CUT_VALUES];
    imcs_elem_typeid_t elem_types[MAX_CUT_VALUES];
    size_t pos = 0;
    int i, n_values;
    imcs_key_t value;
    int16 elmlen;    
    bool elmbyval;
    char elmalign;
    
    for (i = 0; *fmt != '\0'; i++) { 
        int n;
        char type_letter;
        int elem_size;
        imcs_elem_typeid_t elem_type = TID_char;
        if (sscanf(fmt, "%c%d%n", &type_letter, &elem_size, &n) != 2) { 
            imcs_ereport(ERRCODE_SYNTAX_ERROR, "failed to parse format string '%s'", fmt);
        }
        switch (type_letter) { 
          case 'i':
          case 'I':
            switch (elem_size) { 
              case 1:
                elem_type = TID_int8;
                break;
              case 2:
                elem_type = TID_int16;
                break;
              case 4:
                elem_type = TID_int32;
                break;
              case 8:
                elem_type = TID_int64;
                break;
              default:
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }
            break;
          case 'd':
          case 'D':
            elem_type = TID_date;
            if (elem_size != 4)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 'm':
          case 'M':
            elem_type = TID_money;
            if (elem_size != 8)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 't':
            elem_type = TID_time;
            if (elem_size != 8)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 'T':
            elem_type = TID_timestamp;
            if (elem_size != 8)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            break;
          case 'f':
          case 'F':
            switch (elem_size) { 
              case 4:
                elem_type = TID_float;
                break;
              case 8:
                elem_type = TID_double;
                break;
              default:
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }
            break;
          case 'C':
          case 'c':
            if (elem_size <= 0)  { 
                imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type size %d", elem_size);
            }    
            elem_type = TID_char;
            break;
          default:
            imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "invalid type character %c", type_letter);
        }
        if (pos + elem_size > len) { 
            imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "too much values in format string %s", format);
        }
        fmt += n;
        pos += elem_size;
        if (i > MAX_CUT_VALUES) {
            imcs_ereport(ERRCODE_SYNTAX_ERROR, "Too much value in format string %s (limit is %d)", format, MAX_CUT_VALUES);
        }
        elem_types[i] = elem_type;
        elem_sizes[i] = elem_size;
        if (elem_type != TID_char) {
            bool is_varlena; 
            getTypeOutputInfo(imcs_elem_type_to_oid[elem_type], &type_out[i], &is_varlena);
        }
    }
    if (pos != len) { 
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "too few values in format string %s", format);
    }
    n_values = i;
    for (i = 0; i < n_values; i++) { 
        if (elem_types[i] == TID_char) {
            values[i] = CStringGetTextDatum(src);
        } else {             
            memcpy(&value, src, elem_sizes[i]);
            switch (elem_types[i]) { 
              case TID_int8:
                values[i] = Int8GetDatum(value.val_int8);
                break;
              case TID_int16:
                values[i] = Int16GetDatum(value.val_int16);
                break;
              case TID_int32:
              case TID_date:
                values[i] = Int32GetDatum(value.val_int32);
                break;
              case TID_int64:
              case TID_time:
              case TID_timestamp:
                values[i] = Int64GetDatum(value.val_int64);
                break;
              case TID_float:
                values[i] = Float4GetDatum(value.val_float);
                break;
              case TID_double:
                values[i] = Float8GetDatum(value.val_double);
                break;
              default:
                Assert(false);
            }
            values[i] = CStringGetTextDatum(OidOutputFunctionCall(type_out[i], values[i]));
        }
        src += elem_sizes[i];
    }
    get_typlenbyvalalign(TEXTOID, &elmlen, &elmbyval, &elmalign);
    PG_RETURN_ARRAYTYPE_P(construct_array(values, n_values, TEXTOID, elmlen, elmbyval, elmalign));
}

Datum cs_delete_all(PG_FUNCTION_ARGS)
{
    int64 deleted = 0;
    if (imcs_hash != NULL) { 
        HASH_SEQ_STATUS status;
        imcs_hash_entry_t* entry;

        if (imcs_lock != LOCK_EXCLUSIVE) { 
            if (imcs_lock != LOCK_NONE) { 
                LWLockRelease(imcs->lock);
            }
            LWLockAcquire(imcs->lock, LW_EXCLUSIVE);
            imcs_lock = LOCK_EXCLUSIVE;
        }

        hash_seq_init(&status, imcs_hash);
        while ((entry = hash_seq_search(&status)) != NULL) {
            deleted += imcs_delete_all(&entry->value);
        }
        
        LWLockRelease(imcs->lock);
        imcs_lock = LOCK_NONE;
    }
    PG_RETURN_INT64(deleted);
}

Datum columnar_store_truncate(PG_FUNCTION_ARGS)                    
{                                                                       
    if (imcs_hash != NULL) { 
        char const* table_name = PG_GETARG_CSTRING(0);                           
        HASH_SEQ_STATUS status;
        imcs_hash_entry_t* entry;
        size_t table_name_len = strlen(table_name);

        if (imcs_lock != LOCK_EXCLUSIVE) { 
            if (imcs_lock != LOCK_NONE) { 
                LWLockRelease(imcs->lock);
            }
            LWLockAcquire(imcs->lock, LW_EXCLUSIVE);
            imcs_lock = LOCK_EXCLUSIVE;
        }

        hash_seq_init(&status, imcs_hash);
        while ((entry = hash_seq_search(&status)) != NULL) 
        {
            if (strncmp(entry->key.id, table_name, table_name_len) == 0 
                && (entry->key.id[table_name_len] == '-' || entry->key.id[table_name_len] == '\0'))
            {
                imcs_delete_all(&entry->value);
            }
        }
        
        LWLockRelease(imcs->lock);
        imcs_lock = LOCK_NONE;
    }
    PG_RETURN_VOID();
}

Datum cs_used_memory(PG_FUNCTION_ARGS)                    
{                                                                       
    PG_RETURN_INT64(imcs_used_memory());
}

static int32 imcs_date2year(int32 date)
{
    int month, year, mday;
    j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &mday);
    return year;
}

static int32 imcs_date2month(int32 date)
{
    int month, year, mday;
    j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &mday);
    return month;
}

static int32 imcs_date2mday(int32 date)
{
    int month, year, mday;
    j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &mday);
    return mday;
}

static int32 imcs_date2wday(int32 date)
{
    return j2day(date + POSTGRES_EPOCH_JDATE);
}

static int32 imcs_date2quarter(int32 date) 
{
    return (imcs_date2month(date) - 1) / 3 + 1;
}

static int32 imcs_date2week(int32 date) 
{
    return (date - imcs_date2wday(date))/7;
}

static int64 imcs_time2hour(int64 time)
{
    struct pg_tm tm;
	int tz;    
    abstime2tm(time, &tz, &tm, NULL);
    return tm.tm_hour;
}

static int64 imcs_time2minute(int64 time)
{
    struct pg_tm tm;
	int tz;    
    abstime2tm(time, &tz, &tm, NULL);
    return tm.tm_min;
}

static int64 imcs_time2second(int64 time)
{
    struct pg_tm tm;
	int tz;    
    abstime2tm(time, &tz, &tm, NULL);
    return tm.tm_sec;
}

static int64 imcs_timestamp2year(int64 timestamp)
{
    struct pg_tm tm;
    fsec_t fsec;    
    timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL);
    return tm.tm_year;
}

static int64 imcs_timestamp2month(int64 timestamp)
{
    struct pg_tm tm;
    fsec_t fsec;    
    timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL);
    return tm.tm_mon;
}

static int64 imcs_timestamp2mday(int64 timestamp)
{
    struct pg_tm tm;
    fsec_t fsec;    
    timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL);
    return tm.tm_mday;
}

static int64 imcs_timestamp2wday(int64 timestamp)
{
    return imcs_date2wday(imcs_timestamp2date(timestamp));
}

static int64 imcs_timestamp2hour(int64 timestamp)
{
    struct pg_tm tm;
    fsec_t fsec;    
    timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL);
    return tm.tm_hour;
}

static int64 imcs_timestamp2minute(int64 timestamp)
{
    struct pg_tm tm;
    fsec_t fsec;    
    timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL);
    return tm.tm_min;
}

static int64 imcs_timestamp2second(int64 timestamp)
{
    struct pg_tm tm;
    fsec_t fsec;    
    timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL);
    return tm.tm_sec;
}

static int64 imcs_timestamp2quarter(int64 timestamp) 
{
    return (imcs_timestamp2month(timestamp) - 1) / 3 + 1;
}

static int64 imcs_timestamp2week(int64 timestamp) 
{
    return imcs_date2week((int32)imcs_timestamp2date(timestamp));
}


#define IMCS_DATE_FUNC(func)                                            \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_iterator_h result = NULL;                                      \
    IMCS_TRACE(func);                                                   \
    switch (input->elem_type) {                                         \
      case TID_date:                                                    \
        result = imcs_func_int32(input, &imcs_date2##func);             \
        break;                                                          \
      case TID_timestamp:                                               \
        result = imcs_func_int64(input, &imcs_timestamp2##func);        \
        break;                                                          \
      default:                                                          \
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "timeseries of date, time or timestamp type expected"); \
    }                                                                   \
    PG_RETURN_POINTER(result);                                          \
}

#define IMCS_TIME_FUNC(func)                                            \
Datum cs_##func(PG_FUNCTION_ARGS)                                       \
{                                                                       \
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      \
    imcs_iterator_h result = NULL;                                      \
    IMCS_TRACE(func);                                                   \
    switch (input->elem_type) {                                         \
      case TID_time:                                                    \
        result = imcs_func_int64(input, &imcs_time2##func);             \
        break;                                                          \
      case TID_timestamp:                                               \
        result = imcs_func_int64(input, &imcs_timestamp2##func);        \
        break;                                                          \
      default:                                                          \
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "timeseries of time or timestamp type expected"); \
    }                                                                   \
    PG_RETURN_POINTER(result);                                          \
}

IMCS_DATE_FUNC(year);
IMCS_DATE_FUNC(month);
IMCS_DATE_FUNC(mday);
IMCS_DATE_FUNC(wday);
IMCS_DATE_FUNC(week);
IMCS_DATE_FUNC(quarter);
IMCS_TIME_FUNC(hour);
IMCS_TIME_FUNC(minute);
IMCS_TIME_FUNC(second);

Datum cs_call(PG_FUNCTION_ARGS)                                       
{                                                                       
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      
    Oid funcid = PG_GETARG_OID(1);
    Oid ret_typid;
    Oid arg_typid;
    imcs_elem_typeid_t ret_elem_type;
    imcs_elem_typeid_t arg_elem_type;
    imcs_iterator_h result;
    HeapTuple proctup;
    Form_pg_proc procform;
    bool is_bin_function;
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
    {
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "%d is not valid function OID", funcid); 
    }                                                                   
    procform = (Form_pg_proc) GETSTRUCT(proctup);    
    if (procform->pronargs != 1) {
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Only function of one argument can be called");
    }
    ret_typid = procform->prorettype;
    arg_typid = procform->proargtypes.values[0];
    is_bin_function = procform->prolang == INTERNALlanguageId || procform->prolang == ClanguageId;
    ReleaseSysCache(proctup);

    arg_elem_type = imcs_oid_to_typeid(arg_typid);
    ret_elem_type = imcs_oid_to_typeid(ret_typid);
    if (arg_elem_type != input->elem_type) {         
        input = imcs_cast(input, arg_elem_type, get_typmodin(arg_typid) - VARHDRSZ);
    }
    switch (ret_elem_type) {
      case TID_int8:                                       
        IMCS_APPLY(int8_call, arg_elem_type, (input, funcid));
        break;                                              
      case TID_int16:                                       
        IMCS_APPLY(int16_call, arg_elem_type, (input, funcid));
        break;                                              
      case TID_int32:                                       
      case TID_date:                                
        IMCS_APPLY(int32_call, arg_elem_type, (input, funcid));
        break;                                              
      case TID_int64:                                       
      case TID_time:                                
      case TID_timestamp:                           
      case TID_money:                           
        IMCS_APPLY(int64_call, arg_elem_type, (input, funcid));
        break;                                              
      case TID_float:                                       
        IMCS_APPLY(float_call, arg_elem_type, (input, funcid));
        break;                                              
      case TID_double:                                      
        IMCS_APPLY(double_call, arg_elem_type, (input, funcid));
        break;                                              
      default:                                              
        imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "function with character return type are not supported"); 
    }                    
    result->elem_type = ret_elem_type;
    if (!is_bin_function) { /* parallel execution of SQL or PLSQL functions is not possible: SPI code is not reentrant */
        result->flags &= ~FLAG_CONTEXT_FREE;
    }
    PG_RETURN_POINTER(result);                                          
}
    
    
Datum cs_to_array(PG_FUNCTION_ARGS)                                       
{                                                                       
    imcs_iterator_h input = (imcs_iterator_h)PG_GETARG_POINTER(0);      
    size_t size = (size_t)imcs_count(input);
    Datum* body;
    int16 elmlen;    
    bool elmbyval;
    char elmalign;
    int elem_size = input->elem_size;
    size_t i = 0, j, tile_size;
    Oid elmtyp = imcs_elem_type_to_oid[input->elem_type];
    Oid	rettype = get_fn_expr_rettype(fcinfo->flinfo);
    if (get_element_type(rettype) != elmtyp) { 
        imcs_ereport(ERRCODE_DATATYPE_MISMATCH, "Type of sequence element %s doesn't match with function %s return type", imcs_type_mnems[input->elem_type], get_func_name(fcinfo->flinfo->fn_oid)); 
    }
    IMCS_TRACE(to_array); 
    input->reset(input);
    body = palloc(size*sizeof(Datum));

    switch (input->elem_type) { 
      case TID_int8:
        while (input->next(input)) { 
            for (j = 0, tile_size = input->tile_size; j < tile_size; j++, i++) { 
                body[i] = CharGetDatum(input->tile.arr_int8[j]);
            }
        }
        break;
      case TID_int16:
        while (input->next(input)) { 
            for (j = 0, tile_size = input->tile_size; j < tile_size; j++, i++) { 
                body[i] = Int16GetDatum(input->tile.arr_int16[j]);
            }
        }
        break;
      case TID_int32:
      case TID_date:
        while (input->next(input)) { 
            for (j = 0, tile_size = input->tile_size; j < tile_size; j++, i++) { 
                body[i] = Int32GetDatum(input->tile.arr_int32[j]);
            }
        }
        break;
      case TID_time:
      case TID_timestamp:
      case TID_int64:
      case TID_money:                           
        while (input->next(input)) { 
            for (j = 0, tile_size = input->tile_size; j < tile_size; j++, i++) { 
                body[i] = Int64GetDatum(input->tile.arr_int64[j]);
            }
        }
        break;
      case TID_float:
        while (input->next(input)) { 
            for (j = 0, tile_size = input->tile_size; j < tile_size; j++, i++) { 
                body[i] = Float4GetDatum(input->tile.arr_float[j]);
            }
        }
        break;
      case TID_double:
        while (input->next(input)) { 
            for (j = 0, tile_size = input->tile_size; j < tile_size; j++, i++) { 
                body[i] = Float8GetDatum(input->tile.arr_double[j]);
            }
        }
        break;     
      case TID_char:
        while (input->next(input)) { 
            for (j = 0, tile_size = input->tile_size; j < tile_size; j++, i++) { 
                body[i] = PointerGetDatum(cstring_to_text_with_len(input->tile.arr_char + j*elem_size, elem_size));
            }
        }
        break;
      default:
        Assert(false);
    }
    Assert(i == size);
    get_typlenbyvalalign(elmtyp, &elmlen, &elmbyval, &elmalign);
    PG_RETURN_ARRAYTYPE_P(construct_array(body, size, elmtyp, elmlen, elmbyval, elmalign));
}

typedef struct { 
    Datum* body;
    int count;
    int i;
} imcs_from_array_context_t;

#define IMCS_FROM_ARRAY(TYPE, PG_TYPE)                                  \
static bool imcs_from_array_##TYPE##_next(imcs_iterator_h iterator)     \
{                                                                       \
    imcs_from_array_context_t* ctx = (imcs_from_array_context_t*)iterator->context; \
    int j, i = (int)iterator->next_pos;                                 \
    int available = (int)iterator->last_pos - i + 1;                    \
    if (available > imcs_tile_size) {                                   \
        available = imcs_tile_size;                                     \
    }                                                                   \
    for (j = 0; j < available; j++, i++) {                              \
        iterator->tile.arr_##TYPE[j] = DatumGet##PG_TYPE(ctx->body[i]); \
    }                                                                   \
    iterator->next_pos = i;                                             \
    iterator->tile_size = available;                                    \
    return available != 0;                                              \
}

IMCS_FROM_ARRAY(int8, Char);
IMCS_FROM_ARRAY(int16, Int16);
IMCS_FROM_ARRAY(int32, Int32);
IMCS_FROM_ARRAY(int64, Int64);
IMCS_FROM_ARRAY(float, Float4);
IMCS_FROM_ARRAY(double, Float8);

static bool imcs_from_array_char_next(imcs_iterator_h iterator)
{
    imcs_from_array_context_t* ctx = (imcs_from_array_context_t*)iterator->context; 
    int j, i = (int)iterator->next_pos;                                                  
    int available = iterator->last_pos - i + 1;                                     
    size_t elem_size = iterator->elem_size;
    if (available > imcs_tile_size) {                                   
        available = imcs_tile_size;                                     
    }                                                                   
    for (j = 0; j < available; j++, i++) { 
        text* t = (text*)DatumGetPointer(ctx->body[i]);
        size_t len = VARSIZE(t) - VARHDRSZ; 
        memcpy(iterator->tile.arr_char + j*elem_size, VARDATA(t), len);
        memset(iterator->tile.arr_char + j*elem_size + len, '\0', elem_size - len);
    }
    iterator->next_pos = i;
    iterator->tile_size = available;
    return available != 0;
}



static const imcs_iterator_next_t imcs_from_array_next[] = 
{
    imcs_from_array_int8_next, 
    imcs_from_array_int16_next, 
    imcs_from_array_int32_next, 
    imcs_from_array_int32_next, 
    imcs_from_array_int64_next, 
    imcs_from_array_int64_next, 
    imcs_from_array_int64_next, 
    imcs_from_array_int64_next, 
    imcs_from_array_float_next, 
    imcs_from_array_double_next, 
    imcs_from_array_char_next
};

Datum cs_from_array(PG_FUNCTION_ARGS)                                       
{                                                                       
    ArrayType* a = PG_GETARG_ARRAYTYPE_P(0);
    imcs_elem_typeid_t elem_type = imcs_oid_to_typeid(a->elemtype);
    int16 elmlen;    
    bool elmbyval;
    char elmalign;
    MemoryContext old_context;
    imcs_iterator_h iterator;
    imcs_from_array_context_t* ctx;
    int elem_size = elem_type == TID_char ? PG_GETARG_INT32(1) : imcs_type_sizeof[elem_type];
    if (elem_size <= 0) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Element size is not specified"); 
    }    
    IMCS_TRACE(from_array); 
    iterator = imcs_new_iterator(elem_size, sizeof(imcs_from_array_context_t));    
    iterator->elem_type = elem_type;
    iterator->flags |= FLAG_RANDOM_ACCESS;
    ctx = (imcs_from_array_context_t*)iterator->context;

    old_context = MemoryContextSwitchTo(imcs_mem_ctx);    
    get_typlenbyvalalign(a->elemtype, &elmlen, &elmbyval, &elmalign);
    deconstruct_array(a, a->elemtype, elmlen, elmbyval, elmalign, &ctx->body, NULL, &ctx->count);
    MemoryContextSwitchTo(old_context);

    iterator->last_pos = ctx->count-1;
    iterator->next = imcs_from_array_next[elem_type];
    PG_RETURN_POINTER(iterator);
}

typedef struct 
{
    int total;
    int index;
} imcs_profile_context_t;

Datum cs_profile(PG_FUNCTION_ARGS)                                       
{                                                                       
    FuncCallContext* funcctx;
    int  i;
    bool reset = PG_GETARG_BOOL(0);
    imcs_profile_context_t* ctx;

    if (SRF_IS_FIRSTCALL())
    {
        TupleDesc tupdesc;
        MemoryContext oldcontext;
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) { 
            imcs_ereport(ERRCODE_FEATURE_NOT_SUPPORTED, "function returning record called in context that cannot accept type record");
        }
        ctx = (imcs_profile_context_t*)palloc0(sizeof(imcs_profile_context_t));
        funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->user_fctx = ctx;
        MemoryContextSwitchTo(oldcontext);
    }
    funcctx = SRF_PERCALL_SETUP();
    ctx = (imcs_profile_context_t*)funcctx->user_fctx;
    i = ctx->index;
    while (i < imcs_cmd_last_command) { 
        if (imcs_command_profile[i] != 0) {
            char counter[16];
            char* values[2];
            sprintf(counter, "%d", imcs_command_profile[i]);
            values[0] = (char*)imcs_command_mnem[i];
            values[1] = counter;
            ctx->total += imcs_command_profile[i];
            ctx->index = i + 1;
            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(BuildTupleFromCStrings(funcctx->attinmeta, values)));
        }
        i += 1;
    }
    if (i == imcs_cmd_last_command) { 
        char counter[16];
        char* values[2];
        sprintf(counter, "%d", ctx->total);
        values[0] = (char*)"TOTAL";
        values[1] = counter;
        ctx->index = i + 1;
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(BuildTupleFromCStrings(funcctx->attinmeta, values)));
    }        
    if (reset) { 
        memset(imcs_command_profile, 0, sizeof imcs_command_profile);
    }
    SRF_RETURN_DONE(funcctx);
}

Datum cs_str2code(PG_FUNCTION_ARGS)
{
    imcs_dict_key_t key;
    imcs_dict_entry_t* entry;
    VarChar* t = PG_GETARG_VARCHAR_P(0);
    key.val = (char*)VARDATA(t);
    key.len = VARSIZE(t) - VARHDRSZ;
    if (imcs_dict_size == 0) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "IMCS dictionary is disabled"); 
    }
    if (imcs_lock == LOCK_NONE) {         
        LWLockAcquire(imcs->lock, LW_SHARED);  
        imcs_lock = LOCK_SHARED;
    }       
    entry = (imcs_dict_entry_t*)hash_search(imcs_dict, &key, HASH_FIND, NULL);
    if (entry == NULL) { 
        PG_RETURN_INT32(-1);
    } else { 
        PG_RETURN_INT32((int)entry->code);
    }
}

Datum cs_code2str(PG_FUNCTION_ARGS)
{
    uint32 code = PG_GETARG_UINT32(0);
    if (code >= (uint32)imcs_dict_size) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Code %u is out of dictionary range [0..%d)", code, imcs_dict_size); 
    }
	PG_RETURN_TEXT_P(cstring_to_text_with_len(imcs_dict_code_map[code]->key.val, imcs_dict_code_map[code]->key.len));
}

Datum cs_cut_and_code2str(PG_FUNCTION_ARGS)
{
    bytea* str = PG_GETARG_BYTEA_P(0);
    int column_no = PG_GETARG_INT32(1);
    char* src = (char*)VARDATA(str);
    size_t len = VARSIZE(str) - VARHDRSZ;
    uint32 code;
    if (column_no <= 0 || column_no*(imcs_dict_size <= IMCS_SMALL_DICTIONARY ? 2 : 4) > len) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Column with number %d doesn't belong to compound key", column_no);
    }
    code = (imcs_dict_size <= IMCS_SMALL_DICTIONARY) ? *((uint16*)src + column_no - 1) : *((uint32*)src + column_no - 1);
    if (code >= (uint32)imcs_dict_size) { 
        imcs_ereport(ERRCODE_INVALID_PARAMETER_VALUE, "Code %u is out of dictionary range [0..%d)", code, imcs_dict_size); 
    }
	PG_RETURN_TEXT_P(cstring_to_text_with_len(imcs_dict_code_map[code]->key.val, imcs_dict_code_map[code]->key.len));
}

Datum cs_dictionary_size(PG_FUNCTION_ARGS)
{
    int size = imcs_dict ? hash_get_num_entries(imcs_dict) : 0;
    PG_RETURN_INT32(size);
}
