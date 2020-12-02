#ifndef __FUNC_H__
#define __FUNC_H__

#include "imcs.h"

/* Functions defined for all scalar types */
#define IMCS_FUNC_DECL(TYPE)                                        \
    typedef TYPE(*imcs_func_##TYPE##_ptr_t)(TYPE arg);                  \
    typedef TYPE(*imcs_func2_##TYPE##_ptr_t)(TYPE arg1, TYPE arg2);     \
    bool imcs_next_##TYPE(imcs_iterator_h iterator, TYPE* val);         \
    imcs_iterator_h imcs_parse_##TYPE(char const* str, int elem_size);  \
    imcs_iterator_h imcs_adt_parse_##TYPE(char const* str, imcs_adt_parser_t* parser); \
    imcs_iterator_h imcs_const_##TYPE(TYPE val);                        \
    imcs_iterator_h imcs_func_##TYPE(imcs_iterator_h input, imcs_func_##TYPE##_ptr_t func); \
    imcs_iterator_h imcs_func2_##TYPE(imcs_iterator_h left, imcs_iterator_h right, imcs_func2_##TYPE##_ptr_t func); \
    imcs_iterator_h imcs_map_##TYPE(imcs_iterator_h input, imcs_iterator_h positions); \
    imcs_iterator_h imcs_union_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_add_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_sub_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_mul_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_div_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_pow_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_mod_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_maxof_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_minof_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_eq_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_ne_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_gt_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_ge_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_lt_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_le_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_wsum_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_wavg_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_cov_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_corr_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_norm_##TYPE(imcs_iterator_h input);            \
    imcs_iterator_h imcs_thin_##TYPE(imcs_iterator_h input, size_t origin, size_t step); \
    imcs_iterator_h imcs_iif_##TYPE(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter); \
    imcs_iterator_h imcs_if_##TYPE(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter); \
    imcs_iterator_h imcs_filter_##TYPE(imcs_iterator_h cond, imcs_iterator_h input); \
    imcs_iterator_h imcs_unique_##TYPE(imcs_iterator_h input);          \
    imcs_iterator_h imcs_isnan_##TYPE(imcs_iterator_h input);           \
    imcs_iterator_h imcs_abs_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_abs_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_neg_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_reverse_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_diff_##TYPE(imcs_iterator_h input);            \
    imcs_iterator_h imcs_trend_##TYPE(imcs_iterator_h input);           \
    imcs_iterator_h imcs_max_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_min_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_sum_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_any_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_all_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_prd_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_avg_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_var_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_dev_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_median_##TYPE(imcs_iterator_h input);          \
    imcs_iterator_h imcs_group_max_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_min_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_sum_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_any_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_all_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_avg_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_var_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_dev_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_last_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_group_first_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_max_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_min_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_sum_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_any_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_all_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_avg_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_var_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_dev_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_last_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_win_group_first_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_grid_max_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_grid_min_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_grid_sum_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_grid_avg_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_grid_var_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_grid_dev_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_max_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_min_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_sum_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_avg_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_var_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_dev_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_ema_##TYPE(imcs_iterator_h input, size_t interval); \
    imcs_iterator_h imcs_window_atr_##TYPE(imcs_iterator_h input, size_t interval); \
    void imcs_hash_max_##TYPE(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by); \
    void imcs_hash_min_##TYPE(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by); \
    void imcs_hash_sum_##TYPE(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by); \
    void imcs_hash_any_##TYPE(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by); \
    void imcs_hash_all_##TYPE(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by); \
    void imcs_hash_avg_##TYPE(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by); \
    imcs_iterator_h imcs_top_max_##TYPE(imcs_iterator_h input, size_t top); \
    imcs_iterator_h imcs_top_min_##TYPE(imcs_iterator_h input, size_t top); \
    imcs_iterator_h imcs_top_max_pos_##TYPE(imcs_iterator_h input, size_t top); \
    imcs_iterator_h imcs_top_min_pos_##TYPE(imcs_iterator_h input, size_t top); \
    imcs_iterator_h imcs_cum_avg_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_cum_sum_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_cum_min_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_cum_max_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_cum_prd_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_cum_var_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_cum_dev_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_histogram_##TYPE(imcs_iterator_h input, TYPE min_value, TYPE max_value, size_t n_intervals); \
    imcs_iterator_h imcs_cross_##TYPE(imcs_iterator_h input, int first_cross_direction); \
    imcs_iterator_h imcs_extrema_##TYPE(imcs_iterator_h input, int first_extremum); \
    imcs_iterator_h imcs_repeat_##TYPE(imcs_iterator_h input, int n_times); \
    imcs_iterator_h imcs_not_##TYPE(imcs_iterator_h input);             \
    imcs_iterator_h imcs_bit_not_##TYPE(imcs_iterator_h input);         \
    imcs_iterator_h imcs_and_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_or_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_xor_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_asof_join_pos_##TYPE(imcs_iterator_h left, imcs_iterator_h right); \
    imcs_iterator_h imcs_join_pos_##TYPE(imcs_iterator_h left, imcs_iterator_h right);\
    imcs_iterator_h imcs_join_unsorted_##TYPE(imcs_timeseries_t* ts, imcs_iterator_h join_with, int direction); \
    imcs_iterator_h imcs_rank_##TYPE(imcs_iterator_h, imcs_order_t order); \
    imcs_iterator_h imcs_dense_rank_##TYPE(imcs_iterator_h, imcs_order_t order); \
    imcs_iterator_h imcs_sort_##TYPE(imcs_iterator_h, imcs_order_t order); \
    imcs_iterator_h imcs_sort_pos_##TYPE(imcs_iterator_h, imcs_order_t order); \
    imcs_iterator_h imcs_quantile_##TYPE(imcs_iterator_h, size_t q_num) \

    
IMCS_FUNC_DECL(int8);
IMCS_FUNC_DECL(int16);
IMCS_FUNC_DECL(int32);
IMCS_FUNC_DECL(int64);
IMCS_FUNC_DECL(float);
IMCS_FUNC_DECL(double);


#define IMCS_FUNC2_DECL(MNEM, TYPE1, TYPE2)                             \
    imcs_iterator_h imcs_##MNEM##_##TYPE1##_##TYPE2(imcs_iterator_h ts1, imcs_iterator_h ts2, imcs_iterator_h values, TYPE2 filler)

IMCS_FUNC2_DECL(stretch, int32, int8);
IMCS_FUNC2_DECL(stretch, int32, int16);
IMCS_FUNC2_DECL(stretch, int32, int32);
IMCS_FUNC2_DECL(stretch, int32, int64);
IMCS_FUNC2_DECL(stretch, int32, float);
IMCS_FUNC2_DECL(stretch, int32, double);
IMCS_FUNC2_DECL(stretch, int64, int8);
IMCS_FUNC2_DECL(stretch, int64, int16);
IMCS_FUNC2_DECL(stretch, int64, int32);
IMCS_FUNC2_DECL(stretch, int64, int64);
IMCS_FUNC2_DECL(stretch, int64, float);
IMCS_FUNC2_DECL(stretch, int64, double);

IMCS_FUNC2_DECL(stretch0, int32, int8);
IMCS_FUNC2_DECL(stretch0, int32, int16);
IMCS_FUNC2_DECL(stretch0, int32, int32);
IMCS_FUNC2_DECL(stretch0, int32, int64);
IMCS_FUNC2_DECL(stretch0, int32, float);
IMCS_FUNC2_DECL(stretch0, int32, double);
IMCS_FUNC2_DECL(stretch0, int64, int8);
IMCS_FUNC2_DECL(stretch0, int64, int16);
IMCS_FUNC2_DECL(stretch0, int64, int32);
IMCS_FUNC2_DECL(stretch0, int64, int64);
IMCS_FUNC2_DECL(stretch0, int64, float);
IMCS_FUNC2_DECL(stretch0, int64, double);

#define IMCS_JOIN_DECL(JOIN,TYPE1, TYPE2)                               \
    imcs_iterator_h imcs_##JOIN##_##TYPE1##_##TYPE2(imcs_iterator_h ts1, imcs_iterator_h ts2, imcs_iterator_h values)

IMCS_JOIN_DECL(join, int32, int8);
IMCS_JOIN_DECL(join, int32, int16);
IMCS_JOIN_DECL(join, int32, int32);
IMCS_JOIN_DECL(join, int32, int64);
IMCS_JOIN_DECL(join, int32, float);
IMCS_JOIN_DECL(join, int32, double);
IMCS_JOIN_DECL(join, int64, int8);
IMCS_JOIN_DECL(join, int64, int16);
IMCS_JOIN_DECL(join, int64, int32);
IMCS_JOIN_DECL(join, int64, int64);
IMCS_JOIN_DECL(join, int64, float);
IMCS_JOIN_DECL(join, int64, double);

IMCS_JOIN_DECL(asof_join, int32, int8);
IMCS_JOIN_DECL(asof_join, int32, int16);
IMCS_JOIN_DECL(asof_join, int32, int32);
IMCS_JOIN_DECL(asof_join, int32, int64);
IMCS_JOIN_DECL(asof_join, int32, float);
IMCS_JOIN_DECL(asof_join, int32, double);
IMCS_JOIN_DECL(asof_join, int64, int8);
IMCS_JOIN_DECL(asof_join, int64, int16);
IMCS_JOIN_DECL(asof_join, int64, int32);
IMCS_JOIN_DECL(asof_join, int64, int64);
IMCS_JOIN_DECL(asof_join, int64, float);
IMCS_JOIN_DECL(asof_join, int64, double);

/* Functions for sequence of char */
bool imcs_next_char(imcs_iterator_h iterator, void* val);
imcs_iterator_h imcs_parse_char(char const* val, int elem_size);  
imcs_iterator_h imcs_const_char(void const* val, size_t size);  
imcs_iterator_h imcs_iif_char(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter);
imcs_iterator_h imcs_if_char(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter);
imcs_iterator_h imcs_filter_char(imcs_iterator_h cond, imcs_iterator_h input);
imcs_iterator_h imcs_unique_char(imcs_iterator_h input);
imcs_iterator_h imcs_thin_char(imcs_iterator_h input, size_t origin, size_t step);
imcs_iterator_h imcs_repeat_char(imcs_iterator_h input, int count);
imcs_iterator_h imcs_map_char(imcs_iterator_h left, imcs_iterator_h right);
imcs_iterator_h imcs_reverse_char(imcs_iterator_h input);
imcs_iterator_h imcs_add_char(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_eq_char(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_ne_char(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_gt_char(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_ge_char(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_lt_char(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_le_char(imcs_iterator_h left, imcs_iterator_h right); 

/* Polymorphic functions */
imcs_iterator_h imcs_concat(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_cat(imcs_iterator_h left, imcs_iterator_h right); 
imcs_iterator_h imcs_limit(imcs_iterator_h input, imcs_pos_t from, imcs_pos_t till); 
imcs_iterator_h imcs_count_iterator(imcs_iterator_h input); 
imcs_iterator_h imcs_approxdc(imcs_iterator_h input); 
imcs_iterator_h imcs_group_count(imcs_iterator_h group_by);
imcs_iterator_h imcs_group_approxdc(imcs_iterator_h input, imcs_iterator_h group_by);
void imcs_hash_count(imcs_iterator_h result[2], imcs_iterator_h group_by);
void imcs_hash_approxdc(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by);
void imcs_hash_dup_count(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by, size_t min_occurrences);

imcs_iterator_h imcs_like(imcs_iterator_h input, char const* pattern);
imcs_iterator_h imcs_ilike(imcs_iterator_h input, char const* pattern);

imcs_iterator_h imcs_filter_pos(imcs_iterator_h cond);
imcs_iterator_h imcs_filter_first_pos(imcs_iterator_h cond, size_t n);
void imcs_tee(imcs_iterator_h out_iterators[2], imcs_iterator_h in_iterator);

void imcs_from_array(imcs_iterator_h result, void const* buf, size_t buf_size);
void imcs_to_array(imcs_iterator_h iterator, void* buf, size_t buf_size);

imcs_iterator_h imcs_cast_to_char(imcs_iterator_h iterator, int elem_size);

#define IMCS_CAST_DECL(FROM_TYPE, TO_TYPE)                              \
    imcs_iterator_h imcs_##TO_TYPE##_from_##FROM_TYPE(imcs_iterator_h input)

#define IMCS_CASTS_DECL(TYPE)                    \
    IMCS_CAST_DECL(TYPE, int8);                  \
    IMCS_CAST_DECL(TYPE, int16);                 \
    IMCS_CAST_DECL(TYPE, int32);                 \
    IMCS_CAST_DECL(TYPE, int64);                 \
    IMCS_CAST_DECL(TYPE, float);                 \
    IMCS_CAST_DECL(TYPE, double) 

IMCS_CASTS_DECL(int8); 
IMCS_CASTS_DECL(int16); 
IMCS_CASTS_DECL(int32); 
IMCS_CASTS_DECL(int64); 
IMCS_CASTS_DECL(float);
IMCS_CASTS_DECL(double);

#define IMCS_CALL_DECL(RET_TYPE, ARG_TYPE)                                 \
    imcs_iterator_h imcs_##RET_TYPE##_call_##ARG_TYPE(imcs_iterator_h input, Oid funcid)

#define IMCS_CALLS_DECL(TYPE)                    \
    IMCS_CALL_DECL(TYPE, int8);                  \
    IMCS_CALL_DECL(TYPE, int16);                 \
    IMCS_CALL_DECL(TYPE, int32);                 \
    IMCS_CALL_DECL(TYPE, int64);                 \
    IMCS_CALL_DECL(TYPE, float);                 \
    IMCS_CALL_DECL(TYPE, double) 

IMCS_CALLS_DECL(int8); 
IMCS_CALLS_DECL(int16); 
IMCS_CALLS_DECL(int32); 
IMCS_CALLS_DECL(int64); 
IMCS_CALLS_DECL(float);
IMCS_CALLS_DECL(double);

#endif
