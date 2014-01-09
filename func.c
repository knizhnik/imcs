#include "func.h"
#include "btree.h"

#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

double imcs_hash_table_load_factor = 1.0;
size_t imcs_hash_table_init_size = 1361;

#define MAX_PRIME_NUMBERS 29
static const uint32 imcs_prime_numbers[MAX_PRIME_NUMBERS] = 
{
    17,             /* 0 */
    37,             /* 1 */
    79,             /* 2 */
    163,            /* 3 */
    331,            /* 4 */
    673,            /* 5 */
    1361,           /* 6 */
    2729,           /* 7 */
    5471,           /* 8 */
    10949,          /* 9 */
    21911,          /* 10 */
    43853,          /* 11 */
    87719,          /* 12 */
    175447,         /* 13 */
    350899,         /* 14 */
    701819,         /* 15 */
    1403641,        /* 16 */
    2807303,        /* 17 */
    5614657,        /* 18 */
    11229331,       /* 19 */
    22458671,       /* 20 */
    44917381,       /* 21 */
    89834777,       /* 22 */
    179669557,      /* 23 */
    359339171,      /* 24 */
    718678369,      /* 25 */
    1437356741,     /* 26 */
    2147483647,     /* 27 */
    4294967291U     /* 28 */
};

const imcs_elem_typeid_t imcs_underlying_type[] = {TID_int8, TID_int16, TID_int32, TID_int32, TID_int64, TID_int64, TID_int64, TID_int64, TID_float, TID_double, TID_char};

#define IMCS_CHECK_TYPE(arg_type, expected_type)                        \
    if (imcs_underlying_type[arg_type] != expected_type) {              \
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), (errmsg("unexpected timeseries data type %s instead of expected %s", imcs_type_mnems[arg_type], imcs_type_mnems[expected_type])))); \
    }                                                                   


#define IMCS_MIN_int32 ((int32)1 << 31)
#define IMCS_MIN_int64 ((int64)1 << 63)

static uint32 imcs_next_prime_number(uint32 val)
{
    int i; 
    for (i = 0; i < MAX_PRIME_NUMBERS-1 && imcs_prime_numbers[i] <= val; i++);
    return imcs_prime_numbers[i];
}

#define IMCS_NEXT_DEF(TYPE)                                         \
bool imcs_next_##TYPE(imcs_iterator_h iterator, TYPE* val)          \
{                                                                   \
    IMCS_CHECK_TYPE(iterator->elem_type, TID_##TYPE);               \
    if (iterator->tile_offs >= iterator->tile_size) {               \
        if (!iterator->next(iterator)) {                            \
            return false;                                           \
        }                                                           \
        Assert(iterator->tile_size > 0);                            \
        iterator->tile_offs = 0;                                    \
    }                                                               \
    *val = iterator->tile.arr_##TYPE[iterator->tile_offs++];        \
    return true;                                                    \
}

IMCS_NEXT_DEF(int8)
IMCS_NEXT_DEF(int16)
IMCS_NEXT_DEF(int32)
IMCS_NEXT_DEF(int64)
IMCS_NEXT_DEF(float)
IMCS_NEXT_DEF(double)

bool imcs_next_char(imcs_iterator_h iterator, void* val)     
{                                                                       
    IMCS_CHECK_TYPE(iterator->elem_type, TID_char);
    if (iterator->tile_offs >= iterator->tile_size) {                   
        if (!iterator->next(iterator)) { 
            return false;                                                  
        }                                                               
        Assert(iterator->tile_size > 0);               
        iterator->tile_offs = 0;                                        
    }                                                                   
    memcpy(val, iterator->tile.arr_char + iterator->elem_size*iterator->tile_offs++, iterator->elem_size);            
    return true;                                                    
}


#define IMCS_BINARY_DEF(RET_TYPE, TYPE, MNEM, APPLY, OPERATOR)          \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    size_t i, tile_size;                                                \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    if (!iterator->opd[1]->next(iterator->opd[1])) {                    \
        return false;                                                   \
    }                                                                   \
    if (tile_size > iterator->opd[1]->tile_size) {                      \
        tile_size = iterator->opd[1]->tile_size;                        \
    }                                                                   \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##RET_TYPE[i] = APPLY(OPERATOR, iterator->opd[0]->tile.arr_##TYPE[i], iterator->opd[1]->tile.arr_##TYPE[i]); \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h left, imcs_iterator_h right) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(RET_TYPE), 0);    \
    IMCS_CHECK_TYPE(left->elem_type, TID_##TYPE);                       \
    IMCS_CHECK_TYPE(right->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##RET_TYPE;                                 \
    result->opd[0] = imcs_operand(left);                                \
    result->opd[1] = imcs_operand(right);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    return result;                                                      \
}

#define IMCS_BIN_OP(OP, x, y) (x) OP (y)
#define IMCS_BIN_FUNC(OP, x, y) OP(x, y)

IMCS_BINARY_DEF(int8, int8, add, IMCS_BIN_OP, +)
IMCS_BINARY_DEF(int16, int16, add, IMCS_BIN_OP, +)
IMCS_BINARY_DEF(int32, int32, add, IMCS_BIN_OP, +)
IMCS_BINARY_DEF(int64, int64, add, IMCS_BIN_OP, +)
IMCS_BINARY_DEF(float, float, add, IMCS_BIN_OP, +)
IMCS_BINARY_DEF(double, double, add, IMCS_BIN_OP, +)

IMCS_BINARY_DEF(int8, int8, sub, IMCS_BIN_OP, -)
IMCS_BINARY_DEF(int16, int16, sub, IMCS_BIN_OP, -)
IMCS_BINARY_DEF(int32, int32, sub, IMCS_BIN_OP, -)
IMCS_BINARY_DEF(int64, int64, sub, IMCS_BIN_OP, -)
IMCS_BINARY_DEF(float, float, sub, IMCS_BIN_OP, -)
IMCS_BINARY_DEF(double, double, sub, IMCS_BIN_OP, -)

IMCS_BINARY_DEF(int8, int8, mul, IMCS_BIN_OP, *)
IMCS_BINARY_DEF(int16, int16, mul, IMCS_BIN_OP, *)
IMCS_BINARY_DEF(int32, int32, mul, IMCS_BIN_OP, *)
IMCS_BINARY_DEF(int64, int64, mul, IMCS_BIN_OP, *)
IMCS_BINARY_DEF(float, float, mul, IMCS_BIN_OP, *)
IMCS_BINARY_DEF(double, double, mul, IMCS_BIN_OP, *)

IMCS_BINARY_DEF(int8, int8, div, IMCS_BIN_OP, /)
IMCS_BINARY_DEF(int16, int16, div, IMCS_BIN_OP, /)
IMCS_BINARY_DEF(int32, int32, div, IMCS_BIN_OP, /)
IMCS_BINARY_DEF(int64, int64, div, IMCS_BIN_OP, /)
IMCS_BINARY_DEF(float, float, div, IMCS_BIN_OP, /)
IMCS_BINARY_DEF(double, double, div, IMCS_BIN_OP, /)

IMCS_BINARY_DEF(int8, int8, mod, IMCS_BIN_OP, %)
IMCS_BINARY_DEF(int16, int16, mod, IMCS_BIN_OP, %)
IMCS_BINARY_DEF(int32, int32, mod, IMCS_BIN_OP, %)
IMCS_BINARY_DEF(int64, int64, mod, IMCS_BIN_OP, %)
IMCS_BINARY_DEF(float, float, mod, IMCS_BIN_FUNC, fmod)
IMCS_BINARY_DEF(double, double, mod, IMCS_BIN_FUNC, fmod)

IMCS_BINARY_DEF(double, int8, pow, IMCS_BIN_FUNC, pow)
IMCS_BINARY_DEF(double, int16, pow, IMCS_BIN_FUNC, pow)
IMCS_BINARY_DEF(double, int32, pow, IMCS_BIN_FUNC, pow)
IMCS_BINARY_DEF(double, int64, pow, IMCS_BIN_FUNC, pow)
IMCS_BINARY_DEF(double, float, pow, IMCS_BIN_FUNC, pow)
IMCS_BINARY_DEF(double, double, pow, IMCS_BIN_FUNC, pow)

IMCS_BINARY_DEF(int8, int8, eq, IMCS_BIN_OP, ==)
IMCS_BINARY_DEF(int8, int16, eq, IMCS_BIN_OP, ==)
IMCS_BINARY_DEF(int8, int32, eq, IMCS_BIN_OP, ==)
IMCS_BINARY_DEF(int8, int64, eq, IMCS_BIN_OP, ==)
IMCS_BINARY_DEF(int8, float, eq, IMCS_BIN_OP, ==)
IMCS_BINARY_DEF(int8, double, eq, IMCS_BIN_OP, ==)

IMCS_BINARY_DEF(int8, int8, ne, IMCS_BIN_OP, !=)
IMCS_BINARY_DEF(int8, int16, ne, IMCS_BIN_OP, !=)
IMCS_BINARY_DEF(int8, int32, ne, IMCS_BIN_OP, !=)
IMCS_BINARY_DEF(int8, int64, ne, IMCS_BIN_OP, !=)
IMCS_BINARY_DEF(int8, float, ne, IMCS_BIN_OP, !=)
IMCS_BINARY_DEF(int8, double, ne, IMCS_BIN_OP, !=)

IMCS_BINARY_DEF(int8, int8, ge, IMCS_BIN_OP, >=)
IMCS_BINARY_DEF(int8, int16, ge, IMCS_BIN_OP, >=)
IMCS_BINARY_DEF(int8, int32, ge, IMCS_BIN_OP, >=)
IMCS_BINARY_DEF(int8, int64, ge, IMCS_BIN_OP, >=)
IMCS_BINARY_DEF(int8, float, ge, IMCS_BIN_OP, >=)
IMCS_BINARY_DEF(int8, double, ge, IMCS_BIN_OP, >=)

IMCS_BINARY_DEF(int8, int8, le, IMCS_BIN_OP, <=)
IMCS_BINARY_DEF(int8, int16, le, IMCS_BIN_OP, <=)
IMCS_BINARY_DEF(int8, int32, le, IMCS_BIN_OP, <=)
IMCS_BINARY_DEF(int8, int64, le, IMCS_BIN_OP, <=)
IMCS_BINARY_DEF(int8, float, le, IMCS_BIN_OP, <=)
IMCS_BINARY_DEF(int8, double, le, IMCS_BIN_OP, <=)

IMCS_BINARY_DEF(int8, int8, gt, IMCS_BIN_OP, >)
IMCS_BINARY_DEF(int8, int16, gt, IMCS_BIN_OP, >)
IMCS_BINARY_DEF(int8, int32, gt, IMCS_BIN_OP, >)
IMCS_BINARY_DEF(int8, int64, gt, IMCS_BIN_OP, >)
IMCS_BINARY_DEF(int8, float, gt, IMCS_BIN_OP, >)
IMCS_BINARY_DEF(int8, double, gt, IMCS_BIN_OP, >)

IMCS_BINARY_DEF(int8, int8, lt, IMCS_BIN_OP, <)
IMCS_BINARY_DEF(int8, int16, lt, IMCS_BIN_OP, <)
IMCS_BINARY_DEF(int8, int32, lt, IMCS_BIN_OP, <)
IMCS_BINARY_DEF(int8, int64, lt, IMCS_BIN_OP, <)
IMCS_BINARY_DEF(int8, float, lt, IMCS_BIN_OP, <)
IMCS_BINARY_DEF(int8, double, lt, IMCS_BIN_OP, <)

IMCS_BINARY_DEF(int8, int8, and, IMCS_BIN_OP, &)
IMCS_BINARY_DEF(int16, int16, and, IMCS_BIN_OP, &)
IMCS_BINARY_DEF(int32, int32, and, IMCS_BIN_OP, &)
IMCS_BINARY_DEF(int64, int64, and, IMCS_BIN_OP, &)
IMCS_BINARY_DEF(int8, int8, or, IMCS_BIN_OP, |)
IMCS_BINARY_DEF(int16, int16, or, IMCS_BIN_OP, |)
IMCS_BINARY_DEF(int32, int32, or, IMCS_BIN_OP, |)
IMCS_BINARY_DEF(int64, int64, or, IMCS_BIN_OP, |)
IMCS_BINARY_DEF(int8, int8, xor, IMCS_BIN_OP, ^)
IMCS_BINARY_DEF(int16, int16, xor, IMCS_BIN_OP, ^)
IMCS_BINARY_DEF(int32, int32, xor, IMCS_BIN_OP, ^)
IMCS_BINARY_DEF(int64, int64, xor, IMCS_BIN_OP, ^)

#define IMCS_COND(OP, x, y) ((x) OP (y) ? (x) : (y))

IMCS_BINARY_DEF(int8, int8, maxof, IMCS_COND, >)
IMCS_BINARY_DEF(int16, int16, maxof, IMCS_COND, >)
IMCS_BINARY_DEF(int32, int32, maxof, IMCS_COND, >)
IMCS_BINARY_DEF(int64, int64, maxof, IMCS_COND, >)
IMCS_BINARY_DEF(float, float, maxof, IMCS_COND, >)
IMCS_BINARY_DEF(double, double, maxof, IMCS_COND, >)

IMCS_BINARY_DEF(int8, int8, minof, IMCS_COND, <)
IMCS_BINARY_DEF(int16, int16, minof, IMCS_COND, <)
IMCS_BINARY_DEF(int32, int32, minof, IMCS_COND, <)
IMCS_BINARY_DEF(int64, int64, minof, IMCS_COND, <)
IMCS_BINARY_DEF(float, float, minof, IMCS_COND, <)
IMCS_BINARY_DEF(double, double, minof, IMCS_COND, <)


#define IMCS_UNARY_DEF(RES_TYPE, TYPE, MNEM, OPERATION)                 \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    size_t i, tile_size;                                                \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##RES_TYPE[i] = OPERATION(iterator->opd[0]->tile.arr_##TYPE[i]); \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input)             \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(RES_TYPE), 0);    \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##RES_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    return result;                                                      \
}

#define IMCS_ABS(x) (x < 0 ? -x : x)
IMCS_UNARY_DEF(int8, int8, abs, IMCS_ABS)
IMCS_UNARY_DEF(int16, int16, abs, IMCS_ABS)
IMCS_UNARY_DEF(int32, int32, abs, IMCS_ABS)
IMCS_UNARY_DEF(int64, int64, abs, IMCS_ABS)
IMCS_UNARY_DEF(float, float, abs, IMCS_ABS)
IMCS_UNARY_DEF(double, double, abs, IMCS_ABS)

#define IMCS_NEG(x) (-x)
IMCS_UNARY_DEF(int8, int8, neg, IMCS_NEG)
IMCS_UNARY_DEF(int16, int16, neg, IMCS_NEG)
IMCS_UNARY_DEF(int32, int32, neg, IMCS_NEG)
IMCS_UNARY_DEF(int64, int64, neg, IMCS_NEG)
IMCS_UNARY_DEF(float, float, neg, IMCS_NEG)
IMCS_UNARY_DEF(double, double, neg, IMCS_NEG)


#define IMCS_LOGICAL_NOT(x) !(x)
IMCS_UNARY_DEF(int8, int8, not, IMCS_LOGICAL_NOT)
IMCS_UNARY_DEF(int8, int16, not, IMCS_LOGICAL_NOT)
IMCS_UNARY_DEF(int8, int32, not, IMCS_LOGICAL_NOT)
IMCS_UNARY_DEF(int8, int64, not, IMCS_LOGICAL_NOT)

#define IMCS_BIT_NOT(x) ~(x)
IMCS_UNARY_DEF(int8, int8, bit_not, IMCS_BIT_NOT)
IMCS_UNARY_DEF(int16, int16, bit_not, IMCS_BIT_NOT)
IMCS_UNARY_DEF(int32, int32, bit_not, IMCS_BIT_NOT)
IMCS_UNARY_DEF(int64, int64, bit_not, IMCS_BIT_NOT)

#define IMCS_ISNAN(x) isnan(x)
IMCS_UNARY_DEF(int8, float, isnan, IMCS_ISNAN)
IMCS_UNARY_DEF(int8, double, isnan, IMCS_ISNAN)

static bool imcs_add_char_next(imcs_iterator_h iterator)       
{                                                                       
    size_t i, tile_size;                                                
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    
        return false;                                                   
    }                                                                   
    tile_size = iterator->opd[0]->tile_size;                            
    if (!iterator->opd[1]->next(iterator->opd[1])) { 
        return false;                                                      
    }                                                                   
    if (tile_size > iterator->opd[1]->tile_size) {                      
        tile_size = iterator->opd[1]->tile_size;                        
    }                                                                   
    for (i = 0; i < tile_size; i++) {                                   
        char* dst = iterator->tile.arr_char + i*iterator->elem_size;
        char* end = dst + iterator->elem_size;
        size_t n = iterator->opd[0]->elem_size;
        char const* src = iterator->opd[0]->tile.arr_char + i*n;
        while (n-- != 0 && *src != '\0') { 
            *dst++ = *src++;
        }
        n = iterator->opd[1]->elem_size;
        src = iterator->opd[1]->tile.arr_char + i*n;
        while (n-- != 0 && *src != '\0') { 
            *dst++ = *src++;
        }
        while (dst < end) { 
            *dst++ = '\0';
        }
    }                                                                   
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                    
    return true;                                                        
}                                                                       
                                                                        
imcs_iterator_h imcs_add_char(imcs_iterator_h left, imcs_iterator_h right)  
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(left->elem_size + right->elem_size, 0); 
    IMCS_CHECK_TYPE(left->elem_type, TID_char);                       
    IMCS_CHECK_TYPE(right->elem_type, TID_char);                      
    result->elem_type = TID_char;                                 
    result->opd[0] = imcs_operand(left);                                              
    result->opd[1] = imcs_operand(right);                                             
    result->next = imcs_add_char_next;                         
    result->flags = FLAG_CONTEXT_FREE;                                 
    return result;                                                      
}

static bool imcs_cat_next(imcs_iterator_h iterator)       
{                                                                       
    size_t i, tile_size;                                                
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    
        return false;                                                   
    }                                                                   
    tile_size = iterator->opd[0]->tile_size;                            
    if (!iterator->opd[1]->next(iterator->opd[1])) { 
        return false;                                                      
    }                                                                   
    if (tile_size > iterator->opd[1]->tile_size) {                      
        tile_size = iterator->opd[1]->tile_size;                        
    }                                                                   
    for (i = 0; i < tile_size; i++) {                                   
        memcpy(iterator->tile.arr_char + i*iterator->elem_size, iterator->opd[0]->tile.arr_char + i*iterator->opd[0]->elem_size, iterator->opd[0]->elem_size);
        memcpy(iterator->tile.arr_char + i*iterator->elem_size + iterator->opd[0]->elem_size, iterator->opd[1]->tile.arr_char + i*iterator->opd[1]->elem_size, iterator->opd[1]->elem_size);
    }                                                                   
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                    
    return true;                                                        
}                                                                       
                                                                        
imcs_iterator_h imcs_cat(imcs_iterator_h left, imcs_iterator_h right)  
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(left->elem_size + right->elem_size, 0); 
    result->elem_type = TID_char;                                 
    result->opd[0] = imcs_operand(left);                                              
    result->opd[1] = imcs_operand(right);                                             
    result->next = imcs_cat_next;                         
    result->flags = FLAG_CONTEXT_FREE;                                 
    return result;                                                      
}


typedef struct imcs_parse_context_t_ { 
    char const* cur;
    char const* beg;
} imcs_parse_context_t;


static void imcs_reset_parse_iterator(imcs_iterator_h iterator)
{
    imcs_parse_context_t* ctx = (imcs_parse_context_t*)iterator->context; 
    ctx->cur = ctx->beg;
    imcs_reset_iterator(iterator);
}

#define IMCS_PARSE_DEF(TYPE, SCAN_TYPE, SCAN_FORMAT)                    \
static bool imcs_parse_##TYPE##_next(imcs_iterator_h iterator)          \
{                                                                       \
    size_t i;                                                           \
    size_t this_tile_size = imcs_tile_size;                             \
    imcs_parse_context_t* ctx = (imcs_parse_context_t*)iterator->context; \
    char const* ptr = ctx->cur;                                         \
    for (i = 0; i < this_tile_size && *ptr != '}' && *ptr != '\0'; i++) { \
        SCAN_TYPE value;                                                \
        int n;                                                          \
        int rc = sscanf(ptr, SCAN_FORMAT "%n", &value, &n);             \
        if (rc != 1) {                                                  \
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), (errmsg("invalid input format of timeseries literal %s", ptr)))); \
        }                                                               \
        iterator->tile.arr_##TYPE[i] = (TYPE)value;                     \
        ptr += n;                                                       \
        if (*ptr == ',') ptr += 1;                                      \
    }                                                                   \
    ctx->cur = ptr;                                                     \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return i != 0;                                                      \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_parse_##TYPE(char const* input, int elem_size)     \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_parse_context_t)); \
    imcs_parse_context_t* ctx = (imcs_parse_context_t*)result->context; \
    Assert(elem_size == sizeof(TYPE));                                  \
    result->elem_type = TID_##TYPE;                                     \
    result->next = imcs_parse_##TYPE##_next;                            \
    result->reset = imcs_reset_parse_iterator;                          \
    if (*input == '{') input += 1;                                      \
    ctx->cur = ctx->beg = input;                                        \
    return result;                                                      \
}

IMCS_PARSE_DEF(int8, int32, "%i")
IMCS_PARSE_DEF(int16, int32, "%i")
IMCS_PARSE_DEF(int32, int32, "%i")
IMCS_PARSE_DEF(int64, long long, "%lli")
IMCS_PARSE_DEF(float, double, "%lf")
IMCS_PARSE_DEF(double, double, "%lf")

static bool imcs_parse_char_next(imcs_iterator_h iterator) 
{                                                                       
    size_t i;                                                       
    size_t this_tile_size = imcs_tile_size;                         
    size_t elem_size  = iterator->elem_size;
    imcs_parse_context_t* ctx = (imcs_parse_context_t*)iterator->context; 
    char const* ptr = ctx->cur;                                         
    for (i = 0; i < this_tile_size && *ptr != '}' && *ptr != '\0'; i++) { 
        size_t n = 0; 
        char* dst = &iterator->tile.arr_char[i*elem_size];
        if (*ptr == '\'' || *ptr == '"') { 
            char quote = *ptr++;
            while (*ptr != quote) { 
                if (*ptr == '\0') { 
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), (errmsg("unterminated string literal %s", ctx->cur)))); 
                }
                if (n == elem_size) { 
                    ereport(ERROR, (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH), (errmsg("CHAR literal too long")))); 
                }
                *dst++ = *ptr++;
                n += 1;
            }
            ptr += 1;
        } else { 
            while (*ptr != ',' && *ptr != '}' && *ptr != '\0') {
                if (n == elem_size) { 
                    ereport(ERROR, (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH), (errmsg("CHAR literal too long")))); 
                }
                *dst++ = *ptr++;
                n += 1;
            }
        }
        memset(dst, 0, elem_size - n);
        if (*ptr == ',') ptr += 1;                                      
    }                                                                   
    ctx->cur = ptr;                                                     
    iterator->tile_size = i;                                            
    iterator->next_pos += i;                                         
    return i != 0;
}                                                                       
                                                                        
imcs_iterator_h imcs_parse_char(char const* input, int elem_size) 
{ 
    imcs_iterator_h result = imcs_new_iterator(elem_size, sizeof(imcs_parse_context_t));                                                                
    imcs_parse_context_t* ctx = (imcs_parse_context_t*)result->context; 
    result->elem_type = TID_char;                       
    result->next = imcs_parse_char_next;                         
    result->reset = imcs_reset_parse_iterator;                       
    if (*input == '{') input += 1;                                      
    ctx->cur = ctx->beg = input;                                        
    return result;                                                    
}

typedef struct imcs_adt_parse_context_t { 
    char* cur;
    char* beg;
    imcs_adt_parser_t* parser;
} imcs_adt_parse_context_t;


static void imcs_reset_adt_parse_iterator(imcs_iterator_h iterator)
{
    imcs_adt_parse_context_t* ctx = (imcs_adt_parse_context_t*)iterator->context; 
    ctx->cur = ctx->beg;
    imcs_reset_iterator(iterator);
}

#define IMCS_ADT_PARSE_DEF(TYPE, POSIX_TYPE)                            \
static bool imcs_adt_parse_##TYPE##_next(imcs_iterator_h iterator)      \
{                                                                       \
    size_t i;                                                           \
    size_t this_tile_size = imcs_tile_size;                             \
    imcs_adt_parse_context_t* ctx = (imcs_adt_parse_context_t*)iterator->context; \
    char* ptr = ctx->cur;                                               \
    char ch = *ptr;                                                     \
    for (i = 0; i < this_tile_size && ch != '}' && ch != '\0'; ptr += (ch != '\0'), i++) { \
        Datum datum;                                                    \
        if (*ptr == '\'' || *ptr == '"') {                              \
            char quote = *ptr++;                                        \
            char* end = strchr(ptr, quote);                             \
            if (end == NULL) {                                          \
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), (errmsg("unterminated string literal %s", ptr-1)))); \
            }                                                           \
            *end = '\0';                                                \
            datum = ctx->parser->parse(ctx->parser, ptr);               \
            *end++ = quote;   /* needed for reset */                    \
            ptr = end;                                                  \
            ch = *ptr;                                                  \
        } else {                                                        \
            char* sep = strchr(ptr, ',');                               \
            if (sep == NULL) {                                          \
                sep = strchr(ptr, '}');                                 \
                if (sep == NULL) {                                      \
                    sep = ptr + strlen(ptr);                            \
                }                                                       \
            }                                                           \
            ch = *sep;                                                  \
            *sep = '\0';                                                \
            datum = ctx->parser->parse(ctx->parser, ptr);               \
            *sep = ch; /* needed for reset */                           \
            ptr = sep;                                                  \
        }                                                               \
        iterator->tile.arr_##TYPE[i] = DatumGet##POSIX_TYPE(datum);     \
    }                                                                   \
    ctx->cur = ptr;                                                     \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return i != 0;                                                      \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_adt_parse_##TYPE(char const* input, imcs_adt_parser_t* parser) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_adt_parse_context_t)); \
    imcs_adt_parse_context_t* ctx = (imcs_adt_parse_context_t*)result->context; \
    result->elem_type = TID_##TYPE;                                     \
    result->next = imcs_adt_parse_##TYPE##_next;                        \
    result->reset = imcs_reset_adt_parse_iterator;                      \
    if (*input == '{') input += 1;                                      \
    ctx->cur = ctx->beg = (char*)input;                                 \
    ctx->parser = parser;                                               \
    return result;                                                      \
}

IMCS_ADT_PARSE_DEF(int32, Int32);
IMCS_ADT_PARSE_DEF(int64, Int64);

typedef struct imcs_norm_context_t_ { 
    double norm;
} imcs_norm_context_t;

static void imcs_norm_merge(imcs_iterator_h dst, imcs_iterator_h src)  
{
    imcs_norm_context_t* ctx = (imcs_norm_context_t*)dst->context; 
    dst->tile.arr_double[0] += src->tile.arr_double[0];
    ctx->norm = sqrt(dst->tile.arr_double[0]);  
}

#define IMCS_NORM_DEF(TYPE)                                             \
static bool imcs_norm_##TYPE##_prepare(imcs_iterator_h iterator)        \
{                                                                       \
    size_t i, tile_size;                                                \
    double norm = 0.0;                                                  \
    while (iterator->opd[0]->next(iterator->opd[0])) {                  \
        tile_size = iterator->opd[0]->tile_size;                        \
        for (i = 0; i < tile_size; i++) {                               \
            double val = (double)iterator->opd[0]->tile.arr_##TYPE[i];  \
            norm += val*val;                                            \
        }                                                               \
    }                                                                   \
    iterator->tile.arr_double[0] = norm;                                \
    return true;                                                        \
}                                                                       \
static bool imcs_norm_##TYPE##_next(imcs_iterator_h iterator)           \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_norm_context_t* ctx = (imcs_norm_context_t*)iterator->context; \
    double norm = ctx->norm;                                            \
    if (iterator->next_pos == 0) {                                      \
        if (!(iterator->flags & FLAG_PREPARED)) {                       \
            norm = 0;                                                   \
            while (iterator->opd[0]->next(iterator->opd[0])) {          \
                tile_size = iterator->opd[0]->tile_size;                \
                for (i = 0; i < tile_size; i++) {                       \
                    double val = (double)iterator->opd[0]->tile.arr_##TYPE[i]; \
                    norm += val*val;                                    \
                }                                                       \
            }                                                           \
            ctx->norm = norm = sqrt(norm);                              \
        }                                                               \
        iterator->opd[0]->reset(iterator->opd[0]);                      \
    }                                                                   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_double[i] = (double)iterator->opd[0]->tile.arr_##TYPE[i] / norm; \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_norm_##TYPE(imcs_iterator_h input)                 \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(double), sizeof(imcs_norm_context_t)); \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_double;                                     \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_norm_##TYPE##_next;                             \
    result->prepare = imcs_norm_##TYPE##_prepare;                       \
    result->merge = imcs_norm_merge;                                    \
    return result;                                                      \
}


IMCS_NORM_DEF(int8)
IMCS_NORM_DEF(int16)
IMCS_NORM_DEF(int32)
IMCS_NORM_DEF(int64)
IMCS_NORM_DEF(float)
IMCS_NORM_DEF(double)

typedef struct imcs_thin_context_t_ {
    size_t origin;
    size_t step;
} imcs_thin_context_t; 
#define IMCS_THIN_DEF(TYPE)                                             \
static bool imcs_thin_##TYPE##_next(imcs_iterator_h iterator)           \
{                                                                       \
    size_t i, tile_size = iterator->opd[0]->tile_size;                  \
    size_t this_tile_size = imcs_tile_size;                             \
    imcs_thin_context_t* ctx = (imcs_thin_context_t*)iterator->context; \
    size_t step = ctx->step;                                            \
    imcs_pos_t pos = ctx->origin + iterator->next_pos*step;             \
    imcs_pos_t last = iterator->opd[0]->next_pos;                       \
    for (i = 0; i < this_tile_size; i++) {                              \
        while (pos >= last) {                                           \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                if (i != 0) {                                           \
                    iterator->tile_size = i;                            \
                    iterator->next_pos += i;                            \
                    return true;                                        \
                }                                                       \
                return false;                                           \
            }                                                           \
            last = iterator->opd[0]->next_pos;                          \
            tile_size = iterator->opd[0]->tile_size;                    \
        }                                                               \
        iterator->tile.arr_##TYPE[i] = iterator->opd[0]->tile.arr_##TYPE[(size_t)(pos - last + tile_size)]; \
        pos += step;                                                    \
    }                                                                   \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_thin_##TYPE(imcs_iterator_h input, size_t origin, size_t step) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_thin_context_t)); \
    imcs_thin_context_t* ctx = (imcs_thin_context_t*)result->context;   \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = input->elem_type;                               \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_thin_##TYPE##_next;                             \
    ctx->origin = origin;                                               \
    ctx->step = step;                                                   \
    return result;                                                      \
}

IMCS_THIN_DEF(int8)
IMCS_THIN_DEF(int16)
IMCS_THIN_DEF(int32)
IMCS_THIN_DEF(int64)
IMCS_THIN_DEF(float)
IMCS_THIN_DEF(double)

static bool imcs_thin_char_next(imcs_iterator_h iterator)  
{                                                                       
    size_t i, tile_size = iterator->opd[0]->tile_size;                
    size_t this_tile_size = imcs_tile_size;                         
    imcs_thin_context_t* ctx = (imcs_thin_context_t*)iterator->context; 
    size_t step = ctx->step;                                        
    imcs_pos_t pos = ctx->origin + iterator->next_pos*step;          
    imcs_pos_t last = iterator->opd[0]->next_pos; 
    size_t elem_size = iterator->elem_size;
    for (i = 0; i < this_tile_size; i++) {                           
        while (pos >= last) {                                           
            if (!iterator->opd[0]->next(iterator->opd[0])) { 
                if (i != 0) {                 
                    iterator->tile_size = i;                            
                    iterator->next_pos += i;                         
                    return true;                                      
                }                                                       
                return false;                                              
            }                                                           
            last = iterator->opd[0]->next_pos;                         
            tile_size = iterator->opd[0]->tile_size;                      
        }                                                               
        memcpy(iterator->tile.arr_char + i*elem_size, iterator->opd[0]->tile.arr_char + elem_size*(size_t)(pos - last + tile_size), elem_size); 
        pos += step;                                                    
    }                                                                   
    iterator->tile_size = i;                                            
    iterator->next_pos += i;                                         
    return true;                                                    
}                                                                       
                                                                        
imcs_iterator_h imcs_thin_char(imcs_iterator_h input, size_t origin, size_t step) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(input->elem_size, sizeof(imcs_thin_context_t)); 
    imcs_thin_context_t* ctx = (imcs_thin_context_t*)result->context; 
    IMCS_CHECK_TYPE(input->elem_type, TID_char);                
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(input);                                               
    result->next = imcs_thin_char_next;                          
    ctx->origin = origin;                                               
    ctx->step = step;                                                   
    return result;                                                    
}

typedef struct imcs_repeat_context_t_ {
    size_t n_times;
    size_t offs;
} imcs_repeat_context_t; 

static void imcs_reset_repeat_iterator(imcs_iterator_h iterator)
{
    imcs_repeat_context_t* ctx = (imcs_repeat_context_t*)iterator->context; 
    ctx->offs = 0;   
    imcs_reset_iterator(iterator);
}

#define IMCS_REPEAT_DEF(TYPE)                                           \
static bool imcs_repeat_##TYPE##_next(imcs_iterator_h iterator)         \
{                                                                       \
    imcs_repeat_context_t* ctx = (imcs_repeat_context_t*)iterator->context; \
    size_t this_tile_size = imcs_tile_size;                             \
    size_t n_times = ctx->n_times;                                      \
    size_t i, tile_size = iterator->opd[0]->tile_size*n_times;          \
    size_t offs = ctx->offs;                                            \
    for (i = 0; i < this_tile_size; i++, offs++) {                      \
        if (offs >= tile_size) {                                        \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                if (i != 0) {                                           \
                    iterator->tile_size = i;                            \
                    iterator->next_pos += i;                            \
                    ctx->offs = offs;                                   \
                    return true;                                        \
                }                                                       \
                return false;                                           \
            }                                                           \
            tile_size = iterator->opd[0]->tile_size*n_times;            \
            offs = 0;                                                   \
        }                                                               \
        iterator->tile.arr_##TYPE[i] = iterator->opd[0]->tile.arr_##TYPE[offs/n_times]; \
    }                                                                   \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    ctx->offs = offs;                                                   \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_repeat_##TYPE(imcs_iterator_h input, int n_times)  \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_repeat_context_t)); \
    imcs_repeat_context_t* ctx = (imcs_repeat_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = input->elem_type;                               \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_repeat_##TYPE##_next;                           \
    result->reset = imcs_reset_repeat_iterator;                         \
    ctx->offs = 0;                                                      \
    ctx->n_times = n_times;                                             \
    return result;                                                      \
}

IMCS_REPEAT_DEF(int8)
IMCS_REPEAT_DEF(int16)
IMCS_REPEAT_DEF(int32)
IMCS_REPEAT_DEF(int64)
IMCS_REPEAT_DEF(float)
IMCS_REPEAT_DEF(double)

static bool imcs_repeat_char_next(imcs_iterator_h iterator)  
{                                                                       
    imcs_repeat_context_t* ctx = (imcs_repeat_context_t*)iterator->context; 
    size_t this_tile_size = imcs_tile_size;                         
    size_t n_times = ctx->n_times;                                  
    size_t i, tile_size = iterator->opd[0]->tile_size*n_times;        
    size_t offs = ctx->offs;                                        
    size_t elem_size = iterator->elem_size;
    for (i = 0; i < this_tile_size; i++, offs++) {                           
        if (offs >= tile_size) {                                        
            if (!iterator->opd[0]->next(iterator->opd[0])) { 
                if (i != 0) {                 
                    iterator->tile_size = i;                            
                    iterator->next_pos += i;                         
                    ctx->offs = offs;                                   
                    return true;                                      
                }                                                       
                return false;                                              
            }                                                           
            tile_size = iterator->opd[0]->tile_size*n_times;              
            offs = 0;                                                   
        }                                                               
        memcpy(iterator->tile.arr_char + i*elem_size, iterator->opd[0]->tile.arr_char + elem_size*(offs/n_times), elem_size);  
    }                                                                   
    iterator->tile_size = i;                                            
    iterator->next_pos += i;                                         
    ctx->offs = offs;                                                   
    return true;      
}                                                                       
                                                                        
imcs_iterator_h imcs_repeat_char(imcs_iterator_h input, int n_times) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(input->elem_size, sizeof(imcs_repeat_context_t)); 
    imcs_repeat_context_t* ctx = (imcs_repeat_context_t*)result->context; 
    IMCS_CHECK_TYPE(input->elem_type, TID_char);
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(input);                                               
    result->next = imcs_repeat_char_next;                          
    result->reset = imcs_reset_repeat_iterator;                   
    ctx->offs = 0;                                                      
    ctx->n_times = n_times;                                             
    return result;                                                    
}

#define IMCS_UNARY_FUNC_DEF(RET_TYPE, ARG_TYPE)                         \
typedef struct imcs_func_##ARG_TYPE##_context_t_ {                      \
    imcs_func_##ARG_TYPE##_ptr_t func;                                  \
} imcs_func_##ARG_TYPE##_context_t;                                     \
static bool imcs_func_##ARG_TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_func_##ARG_TYPE##_context_t* ctx = (imcs_func_##ARG_TYPE##_context_t*)iterator->context; \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##RET_TYPE[i] = ctx->func(iterator->opd[0]->tile.arr_##ARG_TYPE[i]); \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_func_##ARG_TYPE(imcs_iterator_h input, imcs_func_##ARG_TYPE##_ptr_t func) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(RET_TYPE), sizeof(imcs_func_##ARG_TYPE##_context_t)); \
    imcs_func_##ARG_TYPE##_context_t* ctx = (imcs_func_##ARG_TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##ARG_TYPE);                  \
    result->elem_type = TID_##RET_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_func_##ARG_TYPE##_next;                         \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    ctx->func = func;                                                   \
    return result;                                                      \
}

IMCS_UNARY_FUNC_DEF(int8, int8)
IMCS_UNARY_FUNC_DEF(int16, int16)
IMCS_UNARY_FUNC_DEF(int32, int32)
IMCS_UNARY_FUNC_DEF(int64, int64)
IMCS_UNARY_FUNC_DEF(float, float)
IMCS_UNARY_FUNC_DEF(double, double)


#define IMCS_BINARY_FUNC_DEF(RET_TYPE, ARG_TYPE)                        \
typedef struct imcs_func2_##ARG_TYPE##_context_t_ {                     \
    imcs_func2_##ARG_TYPE##_ptr_t func;                                 \
} imcs_func2_##ARG_TYPE##_context_t;                                    \
static bool imcs_func2_##ARG_TYPE##_next(imcs_iterator_h iterator)      \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_func2_##ARG_TYPE##_context_t* ctx = (imcs_func2_##ARG_TYPE##_context_t*)iterator->context; \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    if (!iterator->opd[1]->next(iterator->opd[1])) {                    \
        return false;                                                   \
    }                                                                   \
    if (tile_size > iterator->opd[1]->tile_size) {                      \
        tile_size = iterator->opd[1]->tile_size;                        \
    }                                                                   \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##RET_TYPE[i] = ctx->func(iterator->opd[0]->tile.arr_##ARG_TYPE[i], iterator->opd[1]->tile.arr_##ARG_TYPE[i]); \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_func2_##ARG_TYPE(imcs_iterator_h left, imcs_iterator_h right, imcs_func2_##ARG_TYPE##_ptr_t func) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(RET_TYPE), sizeof(imcs_func2_##ARG_TYPE##_context_t)); \
    imcs_func2_##ARG_TYPE##_context_t* ctx = (imcs_func2_##ARG_TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(left->elem_type, TID_##ARG_TYPE);                   \
    IMCS_CHECK_TYPE(right->elem_type, TID_##ARG_TYPE);                  \
    result->elem_type = TID_##RET_TYPE;                                 \
    result->opd[0] = imcs_operand(left);                                \
    result->opd[1] = imcs_operand(right);                               \
    result->next = imcs_func2_##ARG_TYPE##_next;                        \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    ctx->func = func;                                                   \
    return result;                                                      \
}


IMCS_BINARY_FUNC_DEF(int8, int8)
IMCS_BINARY_FUNC_DEF(int16, int16)
IMCS_BINARY_FUNC_DEF(int32, int32)
IMCS_BINARY_FUNC_DEF(int64, int64)
IMCS_BINARY_FUNC_DEF(float, float)
IMCS_BINARY_FUNC_DEF(double, double)

#define IMCS_COMPARE_CHAR_DEF(MNEM, OP)                                 \
static bool imcs_##MNEM##_char_next(imcs_iterator_h iterator)           \
{                                                                       \
    size_t i, tile_size;                                                \
    size_t elem_size = iterator->opd[0]->elem_size;                     \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    if (!iterator->opd[1]->next(iterator->opd[1])) {                    \
        return false;                                                   \
    }                                                                   \
    if (tile_size > iterator->opd[1]->tile_size) {                      \
        tile_size = iterator->opd[1]->tile_size;                        \
    }                                                                   \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_int8[i] = memcmp(iterator->opd[0]->tile.arr_char + i*elem_size, iterator->opd[1]->tile.arr_char + i*elem_size, elem_size) OP 0; \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_char(imcs_iterator_h left, imcs_iterator_h right) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(int8), 0);        \
    IMCS_CHECK_TYPE(left->elem_type, TID_char);                         \
    IMCS_CHECK_TYPE(right->elem_type, TID_char);                        \
    if (left->elem_size != right->elem_size) {                          \
        ereport(ERROR, (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH), (errmsg("timeseries of CHAR have different element size")))); \
    }                                                                   \
    result->elem_type = TID_int8;                                       \
    result->opd[0] = imcs_operand(left);                                \
    result->opd[1] = imcs_operand(right);                               \
    result->next = imcs_##MNEM##_char_next;                             \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    return result;                                                      \
}


IMCS_COMPARE_CHAR_DEF(eq, ==)
IMCS_COMPARE_CHAR_DEF(ne, !=)
IMCS_COMPARE_CHAR_DEF(lt, <)
IMCS_COMPARE_CHAR_DEF(gt, >)
IMCS_COMPARE_CHAR_DEF(le, <=)
IMCS_COMPARE_CHAR_DEF(ge, >=)

#define IMCS_CAST_DEF(FROM_TYPE, TO_TYPE)                               \
static bool imcs_##TO_TYPE##_from_##FROM_TYPE##_next(imcs_iterator_h iterator) \
{                                                                       \
    size_t i, tile_size;                                                \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##TO_TYPE[i] = (TO_TYPE)iterator->opd[0]->tile.arr_##FROM_TYPE[i]; \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##TO_TYPE##_from_##FROM_TYPE(imcs_iterator_h input)\
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TO_TYPE), 0);     \
    IMCS_CHECK_TYPE(input->elem_type, TID_##FROM_TYPE);                 \
    result->elem_type = TID_##TO_TYPE;                                  \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##TO_TYPE##_from_##FROM_TYPE##_next;            \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    return result;                                                      \
}                                                                       \

#define IMCS_CASTS_DEF(TYPE)    \
    IMCS_CAST_DEF(TYPE, int8)   \
    IMCS_CAST_DEF(TYPE, int16)  \
    IMCS_CAST_DEF(TYPE, int32)  \
    IMCS_CAST_DEF(TYPE, int64)  \
    IMCS_CAST_DEF(TYPE, float)  \
    IMCS_CAST_DEF(TYPE, double) 

IMCS_CASTS_DEF(int8) 
IMCS_CASTS_DEF(int16) 
IMCS_CASTS_DEF(int32) 
IMCS_CASTS_DEF(int64) 
IMCS_CASTS_DEF(float)
IMCS_CASTS_DEF(double)

typedef struct { 
    void* arr;
} imcs_array_context_t;

static bool imcs_from_array_next(imcs_iterator_h iterator)    
{              
    imcs_array_context_t* ctx = (imcs_array_context_t*)iterator->context;
    size_t i = (size_t)iterator->next_pos;                           
    size_t tile_size = i + imcs_tile_size - 1 < iterator->last_pos ? imcs_tile_size : (size_t)iterator->last_pos - i + 1; 
    if (tile_size == 0) {                                               
        return false;                                                   
    }                                                                   
    memcpy(iterator->tile.arr_char, (char*)ctx->arr + i*iterator->elem_size, tile_size*iterator->elem_size); 
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                 
    return true;                                                    
}                                                                       
                                                                        
void imcs_from_array(imcs_iterator_h result, void const* buf, size_t buf_size) 
{         
    imcs_array_context_t* ctx = (imcs_array_context_t*)result->context;
    ctx->arr = (void*)buf;                                           
    result->last_pos = buf_size-1;                                   
    result->next = imcs_from_array_next;                         
}


void imcs_to_array(imcs_iterator_h input, void* buf, size_t buf_size) 
{                                                                       
    size_t count = 0;                                               
    size_t available; 
    size_t elem_size = input->elem_size;
    input->reset(input);
    while (buf_size != 0) {                                              
        if (input->tile_offs >= input->tile_size) {                     
            if (!input->next(input)) {                
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), (errmsg("failed to extract array at position %ld", count)))); \
                break;                                                  
            } else {                                                    
                Assert(input->tile_size > 0);          
                input->tile_offs = 0;                                   
            }                                                           
        }                                                               
        available = input->tile_size - input->tile_offs;                
        if (available > buf_size) {                                     
            available = buf_size;                                       
        }                                                               
        memcpy((char*)buf + count*elem_size, input->tile.arr_char + input->tile_offs*elem_size, available*elem_size); 
        buf_size -= available;                                          
        count += available;                                             
        input->tile_offs += available;                                  
    }                                                                   
}

#define IMCS_REVERSE_DEF(TYPE)                                          \
static bool imcs_reverse_##TYPE##_next(imcs_iterator_h iterator)        \
{                                                                       \
    size_t i, tile_size;                                                \
    if (iterator->next_pos != 0) {                                      \
        return false;                                                   \
    }                                                                   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    if (iterator->opd[0]->next(iterator->opd[0])) {                     \
        size_t size;                                                    \
        TYPE* buf;                                                      \
        iterator->opd[0]->reset(iterator->opd[0]);                      \
        size = (size_t)imcs_count(iterator->opd[0]);                    \
        buf = (TYPE*)imcs_alloc(size*sizeof(TYPE));                     \
        iterator->opd[0]->reset(iterator->opd[0]);                      \
        for (i = 0; i < size/2; i++) {                                  \
            TYPE tmp = buf[i];                                          \
            buf[i] = buf[size-i-1];                                     \
            buf[size-i-1] = tmp;                                        \
        }                                                               \
        imcs_from_array(iterator, buf, size);                           \
        return iterator->next(iterator);                                \
    } else {                                                            \
        size_t from = (size_t)iterator->first_pos;                      \
        size_t till = iterator->last_pos >= tile_size ? tile_size-1 : (size_t)iterator->last_pos; \
        for (i = from; i <= till; i++) {                               \
            iterator->tile.arr_##TYPE[i-from] = iterator->opd[0]->tile.arr_##TYPE[tile_size-i-1]; \
        }                                                               \
        iterator->tile_size = till-from+1;                              \
        iterator->next_pos = till;                                      \
        return true;                                                    \
    }                                                                   \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_reverse_##TYPE(imcs_iterator_h input)              \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_array_context_t)); \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = input->elem_type;                               \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_reverse_##TYPE##_next;                          \
    result->flags |= FLAG_RANDOM_ACCESS;                                \
    return result;                                                      \
}

IMCS_REVERSE_DEF(int8)
IMCS_REVERSE_DEF(int16)
IMCS_REVERSE_DEF(int32)
IMCS_REVERSE_DEF(int64)
IMCS_REVERSE_DEF(float)
IMCS_REVERSE_DEF(double)

static bool imcs_reverse_char_next(imcs_iterator_h iterator)       
{                                                                       
    size_t i, tile_size;                                            
    size_t elem_size = iterator->elem_size;
    if (iterator->next_pos != 0) {                                   
        return false;
    }                                                                   
    if (!iterator->opd[0]->next(iterator->opd[0])) { 
        return false;                                                      
    }                                                                   
    tile_size = iterator->opd[0]->tile_size;                              
    if (iterator->opd[0]->next(iterator->opd[0])) {     
        size_t size;                                      
        char* buf;                                                      
        char* tmp = (char*)palloc(elem_size);
        size = imcs_count(iterator->opd[0]);
        buf = imcs_alloc(size*elem_size);                        
        imcs_to_array(iterator->opd[0], buf, size);        
        for (i = 0; i < size/2; i++) {                                  
            memcpy(tmp, buf + i*elem_size, elem_size);
            memcpy(buf + i*elem_size, buf + (size-i-1)*elem_size, elem_size);
            memcpy(buf + (size-i-1)*elem_size, tmp, elem_size);
        }                   
        pfree(tmp);
        imcs_from_array(iterator, buf, size);               
        return iterator->next(iterator);                                
    } else {                                                                   
        size_t from = (size_t)iterator->first_pos;                      
        size_t till = iterator->last_pos >= tile_size ? tile_size-1 : (size_t)iterator->last_pos; 
        for (i = from; i <= till; i++) {                               
            memcpy(iterator->tile.arr_char + elem_size*(i-from), iterator->opd[0]->tile.arr_char + elem_size*(tile_size-i-1), elem_size); 
        }                                                               
        iterator->tile_size = till-from+1;                              
        iterator->next_pos = till;                                      
        return true;                                                    
    }
}                                                                       
                                                                        
imcs_iterator_h imcs_reverse_char(imcs_iterator_h input) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(input->elem_size, sizeof(imcs_array_context_t)); 
    IMCS_CHECK_TYPE(input->elem_type, TID_char);
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(input);                                               
    result->next = imcs_reverse_char_next;                               
    result->flags |= FLAG_RANDOM_ACCESS;
    return result;                                                    
}


static bool imcs_const_next(imcs_iterator_h iterator) 
{                                                                       
    iterator->tile_size = imcs_tile_size;                            
    iterator->next_pos += imcs_tile_size;                         
    return true;                                                    
}                                                                       
                                                                        

#define IMCS_CONST_DEF(TYPE)                                            \
imcs_iterator_h imcs_const_##TYPE(TYPE val)                             \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), 0);        \
    size_t i;                                                           \
    size_t this_tile_size = imcs_tile_size;                             \
    for (i = 0; i < this_tile_size; i++) {                              \
        result->tile.arr_##TYPE[i] = val;                               \
    }                                                                   \
    result->elem_type = TID_##TYPE;                                     \
    result->next = imcs_const_next;                                     \
    result->flags = FLAG_CONSTANT|FLAG_CONTEXT_FREE;                    \
    return result;                                                      \
}


IMCS_CONST_DEF(int8)
IMCS_CONST_DEF(int16)
IMCS_CONST_DEF(int32)
IMCS_CONST_DEF(int64)
IMCS_CONST_DEF(float)
IMCS_CONST_DEF(double)

imcs_iterator_h imcs_const_char(void const* val, size_t elem_size)       
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(elem_size, 0); 
    size_t i;                                                       
    size_t this_tile_size = imcs_tile_size;                         
    for (i = 0; i < this_tile_size; i++) {                           
        memcpy(result->tile.arr_char + i*elem_size, val, elem_size);                               
    }                                                                   
    result->elem_type = TID_char;                       
    result->next = imcs_const_next;                         
    result->flags = FLAG_CONTEXT_FREE;                                 
    return result;                                                    
}

#define IMCS_AGG_DEF(TYPE, AGG_TYPE, MNEM, INIT, ACCUMULATE, RESULT)    \
typedef struct {                                                        \
    AGG_TYPE agg;                                                       \
    double norm;                                                        \
    imcs_count_t count;                                                 \
} imcs_##MNEM##_##TYPE##_context_t;                                     \
static void imcs_##MNEM##_##TYPE##_merge(imcs_iterator_h dst, imcs_iterator_h src) \
{                                                                       \
    imcs_##MNEM##_##TYPE##_context_t* src_ctx = (imcs_##MNEM##_##TYPE##_context_t*)src->context; \
    imcs_##MNEM##_##TYPE##_context_t* dst_ctx = (imcs_##MNEM##_##TYPE##_context_t*)dst->context; \
    double norm = 0;                                                    \
    dst_ctx->agg = ACCUMULATE(dst_ctx->agg, src_ctx->agg);              \
    dst_ctx->count += src_ctx->count;                                   \
    norm = dst_ctx->norm + src_ctx->norm;                               \
    dst_ctx->norm = norm;                                               \
    dst->tile.arr_##AGG_TYPE[0] = RESULT(dst_ctx->agg, dst_ctx->count); \
}                                                                       \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    imcs_##MNEM##_##TYPE##_context_t* ctx = (imcs_##MNEM##_##TYPE##_context_t*)iterator->context; \
    size_t i, tile_size;                                                \
    imcs_count_t count = 0;                                             \
    AGG_TYPE agg = 0;                                                   \
    double norm = 0;                                                    \
    if (iterator->flags & FLAG_PREPARED) {                              \
        return iterator->tile_size != 0;                                \
    }                                                                   \
    if (iterator->next_pos != 0) {                                      \
        return false;                                                   \
    }                                                                   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    agg = (AGG_TYPE)INIT(iterator->opd[0]->tile.arr_##TYPE[0]);         \
    i = 1;                                                              \
    do {                                                                \
        tile_size = iterator->opd[0]->tile_size;                        \
        count += tile_size;                                             \
        for (; i < tile_size; i++) {                                    \
            agg = ACCUMULATE(agg, iterator->opd[0]->tile.arr_##TYPE[i]); \
        }                                                               \
        i = 0;                                                          \
    } while (iterator->opd[0]->next(iterator->opd[0]));                 \
    Assert(count != 0);                                                 \
    iterator->next_pos = 1;                                             \
    iterator->tile_size = 1;                                            \
    iterator->tile.arr_##AGG_TYPE[0] = RESULT(agg, count);              \
    ctx->agg = agg;                                                     \
    ctx->norm = norm;                                                   \
    ctx->count = count;                                                 \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input)             \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(AGG_TYPE), sizeof(imcs_##MNEM##_##TYPE##_context_t)); \
    imcs_##MNEM##_##TYPE##_context_t* ctx = (imcs_##MNEM##_##TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##AGG_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->prepare = imcs_##MNEM##_##TYPE##_next;                      \
    result->merge = imcs_##MNEM##_##TYPE##_merge;                       \
    ctx->count = 0;                                                     \
    return result;                                                      \
}

#define IMCS_AGG_INIT(val) (val)
#define IMCS_MAX_ACCUMULATE(agg, val) (agg < val ? val : agg)
#define IMCS_AGG_RESULT(agg, count) (agg)
IMCS_AGG_DEF(int8, int8, max, IMCS_AGG_INIT, IMCS_MAX_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int16, int16, max, IMCS_AGG_INIT, IMCS_MAX_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int32, int32, max, IMCS_AGG_INIT, IMCS_MAX_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int64, int64, max, IMCS_AGG_INIT, IMCS_MAX_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(float, float, max, IMCS_AGG_INIT, IMCS_MAX_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(double, double, max, IMCS_AGG_INIT, IMCS_MAX_ACCUMULATE, IMCS_AGG_RESULT)

#define IMCS_MIN_ACCUMULATE(agg, val) (agg > val ? val : agg)
IMCS_AGG_DEF(int8, int8, min, IMCS_AGG_INIT, IMCS_MIN_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int16, int16, min, IMCS_AGG_INIT, IMCS_MIN_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int32, int32, min, IMCS_AGG_INIT, IMCS_MIN_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int64, int64, min, IMCS_AGG_INIT, IMCS_MIN_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(float, float, min, IMCS_AGG_INIT, IMCS_MIN_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(double, double, min, IMCS_AGG_INIT, IMCS_MIN_ACCUMULATE, IMCS_AGG_RESULT)

#define IMCS_SUM_ACCUMULATE(agg, val) (agg + val)
IMCS_AGG_DEF(int8, int64, sum, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int16, int64, sum, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int32, int64, sum, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int64, int64, sum, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(float, double, sum, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(double, double, sum, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AGG_RESULT)

#define IMCS_ALL_ACCUMULATE(agg, val) (agg & val)
IMCS_AGG_DEF(int8, int8, all, IMCS_AGG_INIT, IMCS_ALL_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int16, int16, all, IMCS_AGG_INIT, IMCS_ALL_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int32, int32, all, IMCS_AGG_INIT, IMCS_ALL_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int64, int64, all, IMCS_AGG_INIT, IMCS_ALL_ACCUMULATE, IMCS_AGG_RESULT)

#define IMCS_ANY_ACCUMULATE(agg, val) (agg | val)
IMCS_AGG_DEF(int8, int8, any, IMCS_AGG_INIT, IMCS_ANY_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int16, int16, any, IMCS_AGG_INIT, IMCS_ANY_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int32, int32, any, IMCS_AGG_INIT, IMCS_ANY_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int64, int64, any, IMCS_AGG_INIT, IMCS_ANY_ACCUMULATE, IMCS_AGG_RESULT)

#define IMCS_PRD_ACCUMULATE(agg, val) (agg * val)
IMCS_AGG_DEF(int8, int64, prd, IMCS_AGG_INIT, IMCS_PRD_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int16, int64, prd, IMCS_AGG_INIT, IMCS_PRD_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int32, int64, prd, IMCS_AGG_INIT, IMCS_PRD_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(int64, int64, prd, IMCS_AGG_INIT, IMCS_PRD_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(float, double, prd, IMCS_AGG_INIT, IMCS_PRD_ACCUMULATE, IMCS_AGG_RESULT)
IMCS_AGG_DEF(double, double, prd, IMCS_AGG_INIT, IMCS_PRD_ACCUMULATE, IMCS_AGG_RESULT)


#define IMCS_AVG_RESULT(agg, count) (agg/count)
IMCS_AGG_DEF(int8, double, avg, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AVG_RESULT)
IMCS_AGG_DEF(int16, double, avg, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AVG_RESULT)
IMCS_AGG_DEF(int32, double, avg, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AVG_RESULT)
IMCS_AGG_DEF(int64, double, avg, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AVG_RESULT)
IMCS_AGG_DEF(float, double, avg, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AVG_RESULT)
IMCS_AGG_DEF(double, double, avg, IMCS_AGG_INIT, IMCS_SUM_ACCUMULATE, IMCS_AVG_RESULT)

#define IMCS_VAR_INIT(val) (norm = (double)val*val, val)
#define IMCS_VAR_ACCUMULATE(agg, val) (norm += (double)val*val, agg + val)
#define IMCS_VAR_RESULT(agg, count) ((norm - agg*agg/count)/count)
IMCS_AGG_DEF(int8, double, var, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_VAR_RESULT)
IMCS_AGG_DEF(int16, double, var, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_VAR_RESULT)
IMCS_AGG_DEF(int32, double, var, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_VAR_RESULT)
IMCS_AGG_DEF(int64, double, var, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_VAR_RESULT)
IMCS_AGG_DEF(float, double, var, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_VAR_RESULT)
IMCS_AGG_DEF(double, double, var, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_VAR_RESULT)

#define IMCS_DEV_RESULT(agg, count) sqrt((norm - agg*agg/count)/count)
IMCS_AGG_DEF(int8, double, dev, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_DEV_RESULT)
IMCS_AGG_DEF(int16, double, dev, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_DEV_RESULT)
IMCS_AGG_DEF(int32, double, dev, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_DEV_RESULT)
IMCS_AGG_DEF(int64, double, dev, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_DEV_RESULT)
IMCS_AGG_DEF(float, double, dev, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_DEV_RESULT)
IMCS_AGG_DEF(double, double, dev, IMCS_VAR_INIT, IMCS_VAR_ACCUMULATE, IMCS_DEV_RESULT)

static void imcs_count_merge(imcs_iterator_h dst, imcs_iterator_h src) 
{                                                                       
    dst->tile.arr_int64[0] += src->tile.arr_int64[0];
}                                                                       
static bool imcs_count_next(imcs_iterator_h iterator)       
{                                                                       
    imcs_count_t count = 0;                                             
    if (iterator->flags & FLAG_PREPARED) {                              
        return iterator->tile_size != 0;                                
    }
    if (iterator->next_pos != 0) {                                      
        return false;                                                   
    }                                                                   
    while (iterator->opd[0]->next(iterator->opd[0])) {                    
        count += iterator->opd[0]->tile_size;
    }                                                                   
    iterator->next_pos = 1;                                             
    iterator->tile_size = 1;                                            
    iterator->tile.arr_int64[0] = count;              
    return true;                                                        
}                                                                       
                                                                        
imcs_iterator_h imcs_count_iterator(imcs_iterator_h input)             
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), 0); 
    result->elem_type = TID_int64;                                 
    result->opd[0] = imcs_operand(input);                               
    result->next = imcs_count_next;                         
    result->prepare = imcs_count_next;                      
    result->merge = imcs_count_merge;                       
    return result;                                                      \
}

typedef struct {                                                        
    double sx;
    double sy;
    double sxx;
    double sxy;
    double syy;
    imcs_count_t count;
} imcs_bin_agg_context_t;                                  

#define IMCS_BIN_AGG_DEF(TYPE, AGG_TYPE, MNEM, ACCUMULATE, RESULT) \
static void imcs_##MNEM##_##TYPE##_merge(imcs_iterator_h dst, imcs_iterator_h src) \
{                                                                       \
    imcs_bin_agg_context_t* dst_ctx = (imcs_bin_agg_context_t*)dst->context; \
    imcs_bin_agg_context_t* src_ctx = (imcs_bin_agg_context_t*)src->context; \
    double sx = dst_ctx->sx + src_ctx->sx;                              \
    double sy = dst_ctx->sy + src_ctx->sy;                              \
    double sxx = dst_ctx->sxx + src_ctx->sxx;                           \
    double sxy = dst_ctx->sxy + src_ctx->sxy;                           \
    double syy = dst_ctx->syy + src_ctx->syy;                           \
    imcs_count_t count = dst_ctx->count + src_ctx->count;               \
    dst->tile.arr_##AGG_TYPE[0] = RESULT(count);                        \
    dst_ctx->sx = sx;                                                   \
    dst_ctx->sy = sy;                                                   \
    dst_ctx->sxy = sxy;                                                 \
    dst_ctx->sxx = sxx;                                                 \
    dst_ctx->syy = syy;                                                 \
    dst_ctx->count = count;                                             \
}                                                                       \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_count_t count = 0;                                             \
    imcs_bin_agg_context_t* ctx = (imcs_bin_agg_context_t*)iterator->context; \
    double sx = 0, sy = 0, sxx = 0, sxy = 0, syy = 0;                   \
    if (iterator->flags & FLAG_PREPARED) {                              \
        return iterator->tile_size != 0;                                \
    }                                                                   \
    if (iterator->next_pos != 0) {                                      \
        return false;                                                   \
    }                                                                   \
    while (iterator->opd[0]->next(iterator->opd[0])                     \
           && iterator->opd[1]->next(iterator->opd[1]))                 \
    {                                                                   \
        tile_size = iterator->opd[0]->tile_size;                        \
        if (tile_size > iterator->opd[1]->tile_size) {                  \
            tile_size = iterator->opd[1]->tile_size;                    \
        }                                                               \
        for (i = 0; i < tile_size; i++) {                               \
            ACCUMULATE(iterator->opd[0]->tile.arr_##TYPE[i], iterator->opd[1]->tile.arr_##TYPE[i], count); \
            count += 1;                                                 \
        }                                                               \
    }                                                                   \
    if (count == 0) {                                                   \
        return false;                                                   \
    }                                                                   \
    iterator->next_pos = 1;                                             \
    iterator->tile_size = 1;                                            \
    iterator->tile.arr_##AGG_TYPE[0] = RESULT(count);                   \
    ctx->sx = sx;                                                       \
    ctx->sy = sy;                                                       \
    ctx->sxx = sxx;                                                     \
    ctx->sxy = sxy;                                                     \
    ctx->syy = syy;                                                     \
    ctx->count = count;                                                 \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h left, imcs_iterator_h right) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(AGG_TYPE), sizeof(imcs_bin_agg_context_t)); \
    imcs_bin_agg_context_t* ctx = (imcs_bin_agg_context_t*)result->context; \
    IMCS_CHECK_TYPE(left->elem_type, TID_##TYPE);                       \
    IMCS_CHECK_TYPE(right->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##AGG_TYPE;                                 \
    result->opd[0] = imcs_operand(left);                                \
    result->opd[1] = imcs_operand(right);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->prepare = imcs_##MNEM##_##TYPE##_next;                      \
    result->merge = imcs_##MNEM##_##TYPE##_merge;                       \
    ctx->count = 0;                                                     \
    return result;                                                      \
}

#define IMCS_COV_ACCUMULATE(x, y, count) (sx += x, sy += y, sxy += (double)x*y)
#define IMCS_COV_RESULT(count) ((sxy - sx*sy/count)/count)
IMCS_BIN_AGG_DEF(int8, double, cov, IMCS_COV_ACCUMULATE, IMCS_COV_RESULT)
IMCS_BIN_AGG_DEF(int16, double, cov, IMCS_COV_ACCUMULATE, IMCS_COV_RESULT)
IMCS_BIN_AGG_DEF(int32, double, cov, IMCS_COV_ACCUMULATE, IMCS_COV_RESULT)
IMCS_BIN_AGG_DEF(int64, double, cov, IMCS_COV_ACCUMULATE, IMCS_COV_RESULT)
IMCS_BIN_AGG_DEF(float, double, cov, IMCS_COV_ACCUMULATE, IMCS_COV_RESULT)
IMCS_BIN_AGG_DEF(double, double, cov, IMCS_COV_ACCUMULATE, IMCS_COV_RESULT)

#define IMCS_CORR_ACCUMULATE(x, y, count) (sx += x, sxx += (double)x*x, sy += y, syy += (double)y*y, sxy += (double)x*y)
#define IMCS_CORR_RESULT(count) (sxy - sx*sy/count) / sqrt((sxx - sx*sx/count) * (syy - sy*sy/count))
IMCS_BIN_AGG_DEF(int8, double, corr, IMCS_CORR_ACCUMULATE, IMCS_CORR_RESULT)
IMCS_BIN_AGG_DEF(int16, double, corr, IMCS_CORR_ACCUMULATE, IMCS_CORR_RESULT)
IMCS_BIN_AGG_DEF(int32, double, corr, IMCS_CORR_ACCUMULATE, IMCS_CORR_RESULT)
IMCS_BIN_AGG_DEF(int64, double, corr, IMCS_CORR_ACCUMULATE, IMCS_CORR_RESULT)
IMCS_BIN_AGG_DEF(float, double, corr, IMCS_CORR_ACCUMULATE, IMCS_CORR_RESULT)
IMCS_BIN_AGG_DEF(double, double, corr, IMCS_CORR_ACCUMULATE, IMCS_CORR_RESULT)

#define IMCS_WSUM_ACCUMULATE(x, y, count) (sxy += (double)x*y)
#define IMCS_WSUM_RESULT(count) (sxy)
IMCS_BIN_AGG_DEF(int8, double, wsum, IMCS_WSUM_ACCUMULATE, IMCS_WSUM_RESULT)
IMCS_BIN_AGG_DEF(int16, double, wsum, IMCS_WSUM_ACCUMULATE, IMCS_WSUM_RESULT)
IMCS_BIN_AGG_DEF(int32, double, wsum, IMCS_WSUM_ACCUMULATE, IMCS_WSUM_RESULT)
IMCS_BIN_AGG_DEF(int64, double, wsum, IMCS_WSUM_ACCUMULATE, IMCS_WSUM_RESULT)
IMCS_BIN_AGG_DEF(float, double, wsum, IMCS_WSUM_ACCUMULATE, IMCS_WSUM_RESULT)
IMCS_BIN_AGG_DEF(double, double, wsum, IMCS_WSUM_ACCUMULATE, IMCS_WSUM_RESULT)

#define IMCS_WAVG_ACCUMULATE(x, y, count) (sxy += x*y, sx += x)
#define IMCS_WAVG_RESULT(count) (sxy/sx)
IMCS_BIN_AGG_DEF(int8, double, wavg, IMCS_WAVG_ACCUMULATE, IMCS_WAVG_RESULT)
IMCS_BIN_AGG_DEF(int16, double, wavg, IMCS_WAVG_ACCUMULATE, IMCS_WAVG_RESULT)
IMCS_BIN_AGG_DEF(int32, double, wavg, IMCS_WAVG_ACCUMULATE, IMCS_WAVG_RESULT)
IMCS_BIN_AGG_DEF(int64, double, wavg, IMCS_WAVG_ACCUMULATE, IMCS_WAVG_RESULT)
IMCS_BIN_AGG_DEF(float, double, wavg, IMCS_WAVG_ACCUMULATE, IMCS_WAVG_RESULT)
IMCS_BIN_AGG_DEF(double, double, wavg, IMCS_WAVG_ACCUMULATE, IMCS_WAVG_RESULT)

typedef struct imcs_agg_context_t_ 
{
    size_t interval;
    size_t count;
    size_t offset;
    imcs_key_t accumulator;
    double norm;
    union { 
        char arr_char[1];
        int8 arr_int8[1];
        int16 arr_int16[1];
        int32 arr_int32[1];
        int64 arr_int64[1];
        float arr_float[1];
        double arr_double[1];
    } history;
} imcs_agg_context_t;


#define IMCS_WINDOW_AGG_DEF(TYPE, AGG_TYPE, MNEM, NEXT, INIT)           \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
size_t i, tile_size;                                                    \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        NEXT(TYPE, AGG_TYPE, iterator->tile.arr_##AGG_TYPE[i], ctx->accumulator.val_##AGG_TYPE, ctx->history.arr_##TYPE[iterator->next_pos % ctx->interval], iterator->opd[0]->tile.arr_##TYPE[i]); \
        iterator->next_pos += 1;                                        \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    return true;                                                        \
}                                                                       \
static void imcs_##MNEM##_##TYPE##_reset(imcs_iterator_h iterator)      \
{                                                                       \
    size_t i;                                                           \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    ctx->accumulator.val_##AGG_TYPE = INIT;                             \
    ctx->norm = 0;                                                      \
    for (i = 0; i < ctx->interval; i++) {                               \
        ctx->history.arr_##TYPE[i] = INIT;                              \
    }                                                                   \
    imcs_reset_iterator(iterator);                                      \
}                                                                       \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input, size_t interval) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(AGG_TYPE), sizeof(imcs_agg_context_t) + sizeof(TYPE)*interval); \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context;     \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##AGG_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    ctx->interval = interval;                                           \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->reset = imcs_##MNEM##_##TYPE##_reset;                       \
    imcs_##MNEM##_##TYPE##_reset(result);                               \
    return result;                                                      \
}


#define IMCS_WINDOW_SUM_NEXT(TYPE, AGG_TYPE, result, acc, hist, val) (acc -= hist, result = acc += hist = val)

IMCS_WINDOW_AGG_DEF(int8, int64, window_sum, IMCS_WINDOW_SUM_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, int64, window_sum, IMCS_WINDOW_SUM_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, int64, window_sum, IMCS_WINDOW_SUM_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, int64, window_sum, IMCS_WINDOW_SUM_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, double, window_sum, IMCS_WINDOW_SUM_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_sum, IMCS_WINDOW_SUM_NEXT, 0)

#define IMCS_WINDOW_AVG_NEXT(TYPE, AGG_TYPE, result, acc, hist, val) (acc -= hist, result = (acc += hist = val) / ctx->interval)

IMCS_WINDOW_AGG_DEF(int8, double, window_avg, IMCS_WINDOW_AVG_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, double, window_avg, IMCS_WINDOW_AVG_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, double, window_avg, IMCS_WINDOW_AVG_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, double, window_avg, IMCS_WINDOW_AVG_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, double, window_avg, IMCS_WINDOW_AVG_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_avg, IMCS_WINDOW_AVG_NEXT, 0)

#define IMCS_WINDOW_MIN_NEXT(TYPE, AGG_TYPE, result, acc, hist, val) {  \
    TYPE old = hist;                                                    \
    hist = val;                                                         \
    if (val < acc) {                                                    \
        acc = val;                                                      \
    } else if (old == acc) {                                            \
        size_t j, n;                                                    \
        TYPE min;                                                       \
        min = ctx->history.arr_##TYPE[0];                               \
        for (j = 1, n = ctx->interval; j < n; j++) {                    \
            if (ctx->history.arr_##TYPE[j] < min) {                     \
                min = ctx->history.arr_##TYPE[j];                       \
            }                                                           \
        }                                                               \
        acc = min;                                                      \
    }                                                                   \
    result = acc;                                                       \
} 

IMCS_WINDOW_AGG_DEF(int8, int8, window_min, IMCS_WINDOW_MIN_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, int16, window_min, IMCS_WINDOW_MIN_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, int32, window_min, IMCS_WINDOW_MIN_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, int64, window_min, IMCS_WINDOW_MIN_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, float, window_min, IMCS_WINDOW_MIN_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_min, IMCS_WINDOW_MIN_NEXT, 0)

#define IMCS_WINDOW_MAX_NEXT(TYPE, AGG_TYPE, result, acc, hist, val) {  \
    TYPE old = hist;                                                    \
    hist = val;                                                         \
    if (val > acc) {                                                    \
        acc = val;                                                      \
    } else if (old == acc) {                                            \
        size_t j, n;                                                    \
        TYPE max;                                                       \
        max = ctx->history.arr_##TYPE[0];                               \
        for (j = 1, n = ctx->interval; j < n; j++) {                    \
            if (ctx->history.arr_##TYPE[j] > max) {                     \
                max = ctx->history.arr_##TYPE[j];                       \
            }                                                           \
        }                                                               \
        acc = max;                                                      \
    }                                                                   \
    result = acc;                                                       \
} 

IMCS_WINDOW_AGG_DEF(int8, int8, window_max, IMCS_WINDOW_MAX_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, int16, window_max, IMCS_WINDOW_MAX_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, int32, window_max, IMCS_WINDOW_MAX_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, int64, window_max, IMCS_WINDOW_MAX_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, float, window_max, IMCS_WINDOW_MAX_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_max, IMCS_WINDOW_MAX_NEXT, 0)


#define IMCS_WINDOW_VAR_NEXT(TYPE, AGG_TYPE, result, acc, hist, val) (acc -= hist, ctx->norm -= (AGG_TYPE)hist*hist, hist = val, acc += val, ctx->norm += (AGG_TYPE)val*val, result = (ctx->norm - acc*acc/ctx->interval)/ctx->interval)

IMCS_WINDOW_AGG_DEF(int8, double, window_var, IMCS_WINDOW_VAR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, double, window_var, IMCS_WINDOW_VAR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, double, window_var, IMCS_WINDOW_VAR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, double, window_var, IMCS_WINDOW_VAR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, double, window_var, IMCS_WINDOW_VAR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_var, IMCS_WINDOW_VAR_NEXT, 0)

#define IMCS_WINDOW_DEV_NEXT(TYPE, AGG_TYPE, result, acc, hist, val) (acc -= hist, ctx->norm -= (AGG_TYPE)hist*hist, hist = val, acc += val, ctx->norm += (AGG_TYPE)val*val, result = sqrt((ctx->norm - acc*acc/ctx->interval)/ctx->interval))

IMCS_WINDOW_AGG_DEF(int8, double, window_dev, IMCS_WINDOW_DEV_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, double, window_dev, IMCS_WINDOW_DEV_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, double, window_dev, IMCS_WINDOW_DEV_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, double, window_dev, IMCS_WINDOW_DEV_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, double, window_dev, IMCS_WINDOW_DEV_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_dev, IMCS_WINDOW_DEV_NEXT, 0)


#define IMCS_WINDOW_EMA_NEXT(TYPE, AGG_TYPE, result, acc, hist, val)    \
    if (iterator->next_pos == 0) {                                      \
        result = acc = val;                                             \
    } else {                                                            \
        double p = 2.0 / (ctx->interval + 1);                           \
        result = acc = val*p + acc * (1 - p);                           \
    } 

IMCS_WINDOW_AGG_DEF(int8, double, window_ema, IMCS_WINDOW_EMA_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, double, window_ema, IMCS_WINDOW_EMA_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, double, window_ema, IMCS_WINDOW_EMA_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, double, window_ema, IMCS_WINDOW_EMA_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, double, window_ema, IMCS_WINDOW_EMA_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_ema, IMCS_WINDOW_EMA_NEXT, 0)

#define IMCS_WINDOW_ATR_NEXT(TYPE, AGG_TYPE, result, acc, hist, val)   \
   size_t n = iterator->next_pos < ctx->interval ? iterator->next_pos+1 : ctx->interval; \
   result = acc = (acc * (n-1) + val) / n

IMCS_WINDOW_AGG_DEF(int8, double, window_atr, IMCS_WINDOW_ATR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int16, double, window_atr, IMCS_WINDOW_ATR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int32, double, window_atr, IMCS_WINDOW_ATR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(int64, double, window_atr, IMCS_WINDOW_ATR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(float, double, window_atr, IMCS_WINDOW_ATR_NEXT, 0)
IMCS_WINDOW_AGG_DEF(double, double, window_atr, IMCS_WINDOW_ATR_NEXT, 0)


#define IMCS_CUMULATIVE_AGG_DEF(TYPE, AGG_TYPE, MNEM, INIT, NEXT)       \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    size_t i = 0, tile_size;                                            \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    if (iterator->next_pos == 0) {                                      \
        iterator->next_pos += 1;                                        \
        iterator->tile.arr_##AGG_TYPE[i] = INIT(ctx->accumulator.val_##AGG_TYPE, iterator->opd[0]->tile.arr_##TYPE[0]); \
        i += 1;                                                         \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (; i < tile_size; i++) {                                        \
        iterator->next_pos += 1;                                        \
        iterator->tile.arr_##AGG_TYPE[i] = NEXT(ctx->accumulator.val_##AGG_TYPE, iterator->opd[0]->tile.arr_##TYPE[i]); \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input)             \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(AGG_TYPE), sizeof(imcs_agg_context_t)); \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##AGG_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    return result;                                                      \
}


#define IMCS_CUMULATIVE_AGG_INIT(acc, val) (acc = val)
#define IMCS_CUMULATIVE_SUM_NEXT(acc, val) (acc += val)

IMCS_CUMULATIVE_AGG_DEF(int8, int64, cum_sum, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_SUM_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int16, int64, cum_sum, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_SUM_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int32, int64, cum_sum, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_SUM_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int64, int64, cum_sum, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_SUM_NEXT)
IMCS_CUMULATIVE_AGG_DEF(float, double, cum_sum, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_SUM_NEXT)
IMCS_CUMULATIVE_AGG_DEF(double, double, cum_sum, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_SUM_NEXT)

#define IMCS_CUMULATIVE_AVG_NEXT(acc, val) (acc = (val + (iterator->next_pos-1)*acc)/iterator->next_pos)

IMCS_CUMULATIVE_AGG_DEF(int8, double, cum_avg, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_AVG_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int16, double, cum_avg, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_AVG_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int32, double, cum_avg, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_AVG_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int64, double, cum_avg, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_AVG_NEXT)
IMCS_CUMULATIVE_AGG_DEF(float, double, cum_avg, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_AVG_NEXT)
IMCS_CUMULATIVE_AGG_DEF(double, double, cum_avg, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_AVG_NEXT)

#define IMCS_CUMULATIVE_VAR_INIT(acc, val) (acc = val, ctx->norm = val*val, 0)
#define IMCS_CUMULATIVE_VAR_NEXT(acc, val) (acc += val, ctx->norm += val*val, (ctx->norm - acc*acc/iterator->next_pos)/iterator->next_pos)

IMCS_CUMULATIVE_AGG_DEF(int8, int64, cum_var, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_VAR_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int16, int64, cum_var, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_VAR_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int32, int64, cum_var, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_VAR_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int64, int64, cum_var, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_VAR_NEXT)
IMCS_CUMULATIVE_AGG_DEF(float, double, cum_var, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_VAR_NEXT)
IMCS_CUMULATIVE_AGG_DEF(double, double, cum_var, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_VAR_NEXT)

#define IMCS_CUMULATIVE_DEV_NEXT(acc, val) (acc += val, ctx->norm += val*val, sqrt((ctx->norm - acc*acc/iterator->next_pos)/iterator->next_pos))

IMCS_CUMULATIVE_AGG_DEF(int8, int64, cum_dev, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_DEV_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int16, int64, cum_dev, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_DEV_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int32, int64, cum_dev, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_DEV_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int64, int64, cum_dev, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_DEV_NEXT)
IMCS_CUMULATIVE_AGG_DEF(float, double, cum_dev, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_DEV_NEXT)
IMCS_CUMULATIVE_AGG_DEF(double, double, cum_dev, IMCS_CUMULATIVE_VAR_INIT, IMCS_CUMULATIVE_DEV_NEXT)

#define IMCS_CUMULATIVE_PRD_NEXT(acc, val) (acc *= val)

IMCS_CUMULATIVE_AGG_DEF(int8, int64, cum_prd, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_PRD_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int16, int64, cum_prd, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_PRD_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int32, int64, cum_prd, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_PRD_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int64, int64, cum_prd, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_PRD_NEXT)
IMCS_CUMULATIVE_AGG_DEF(float, double, cum_prd, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_PRD_NEXT)
IMCS_CUMULATIVE_AGG_DEF(double, double, cum_prd, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_PRD_NEXT)

#define IMCS_CUMULATIVE_MAX_NEXT(acc, val) (acc = val > acc ? val : acc)

IMCS_CUMULATIVE_AGG_DEF(int8, int8, cum_max, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MAX_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int16, int16, cum_max, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MAX_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int32, int32, cum_max, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MAX_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int64, int64, cum_max, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MAX_NEXT)
IMCS_CUMULATIVE_AGG_DEF(float, float, cum_max, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MAX_NEXT)
IMCS_CUMULATIVE_AGG_DEF(double, double, cum_max, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MAX_NEXT)

#define IMCS_CUMULATIVE_MIN_NEXT(acc, val) (acc = val < acc ? val : acc)

IMCS_CUMULATIVE_AGG_DEF(int8, int8, cum_min, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MIN_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int16, int16, cum_min, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MIN_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int32, int32, cum_min, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MIN_NEXT)
IMCS_CUMULATIVE_AGG_DEF(int64, int64, cum_min, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MIN_NEXT)
IMCS_CUMULATIVE_AGG_DEF(float, float, cum_min, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MIN_NEXT)
IMCS_CUMULATIVE_AGG_DEF(double, double, cum_min, IMCS_CUMULATIVE_AGG_INIT, IMCS_CUMULATIVE_MIN_NEXT)


static void imcs_reset_binary_agg_iterator(imcs_iterator_h iterator)
{
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context; 
    ctx->offset = ctx->count = 0;  
    imcs_reset_iterator(iterator);
}


#define IMCS_GROUP_AGG_DEF(TYPE, AGG_TYPE, MNEM, INIT, ACCUMULATE, RESULT) \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    size_t i, j = 0, tile_size;                                         \
    size_t this_tile_size = imcs_tile_size;                             \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    size_t elem_size = iterator->opd[1]->elem_size;                     \
    while (true) {                                                      \
        if (ctx->offset >= iterator->opd[1]->tile_size) {               \
            if (!iterator->opd[1]->next(iterator->opd[1])) {            \
                if (j + ctx->count != 0) {                              \
                    if (ctx->count != 0) {                              \
                        iterator->tile.arr_##AGG_TYPE[j++] = RESULT(ctx->accumulator.val_##AGG_TYPE, ctx->count); \
                        iterator->next_pos += 1;                        \
                        ctx->count = 0;                                 \
                    }                                                   \
                    iterator->tile_size = j;                            \
                    return true;                                        \
                }                                                       \
                return false;                                           \
            } else {                                                    \
                Assert(ctx->offset == iterator->opd[0]->tile_size);     \
                if (!iterator->opd[0]->next(iterator->opd[0])) {        \
                    return false;                                       \
                }                                                       \
                Assert(iterator->opd[1]->tile_size <= iterator->opd[0]->tile_size); \
            }                                                           \
            ctx->offset = 0;                                            \
        }                                                               \
        tile_size = iterator->opd[1]->tile_size;                        \
        for (i = ctx->offset; i < tile_size; i++) {                     \
            bool same;                                                  \
            switch (elem_size) {                                        \
              case 1:                                                   \
                same = *ctx->history.arr_int8 == iterator->opd[1]->tile.arr_int8[i]; \
                *ctx->history.arr_int8 = iterator->opd[1]->tile.arr_int8[i]; \
                break;                                                  \
              case 2:                                                   \
                same = *ctx->history.arr_int16 == iterator->opd[1]->tile.arr_int16[i]; \
                *ctx->history.arr_int16 = iterator->opd[1]->tile.arr_int16[i]; \
                break;                                                  \
              case 4:                                                   \
                same = *ctx->history.arr_int32 == iterator->opd[1]->tile.arr_int32[i]; \
                *ctx->history.arr_int32 = iterator->opd[1]->tile.arr_int32[i]; \
                break;                                                  \
              case 8:                                                  \
                same = *ctx->history.arr_int64 == iterator->opd[1]->tile.arr_int64[i]; \
                *ctx->history.arr_int64 = iterator->opd[1]->tile.arr_int64[i]; \
                break;                                                  \
              default:                                                  \
                same = memcmp(ctx->history.arr_char, &iterator->opd[1]->tile.arr_char[i*elem_size], elem_size) == 0; \
                memcpy(ctx->history.arr_char, &iterator->opd[1]->tile.arr_char[i*elem_size], elem_size); \
            }                                                           \
            if (ctx->count != 0 && !same) {                             \
                iterator->tile.arr_##AGG_TYPE[j] = RESULT(ctx->accumulator.val_##AGG_TYPE, ctx->count); \
                iterator->next_pos += 1;                                \
                ctx->count = 0;                                         \
                j += 1;                                                 \
            }                                                           \
            ctx->accumulator.val_##AGG_TYPE = (ctx->count == 0)         \
                ? INIT(iterator->opd[0]->tile.arr_##TYPE[i])            \
                : ACCUMULATE(ctx->accumulator.val_##AGG_TYPE, iterator->opd[0]->tile.arr_##TYPE[i], ctx->count); \
            ctx->count += 1;                                            \
            if (j == this_tile_size) {                                  \
                ctx->offset = i + 1;                                    \
                iterator->tile_size = this_tile_size;                   \
                return true;                                            \
            }                                                           \
        }                                                               \
        ctx->offset = tile_size;                                        \
    }                                                                   \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input, imcs_iterator_h group_by) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(AGG_TYPE), sizeof(imcs_agg_context_t) + group_by->elem_size); \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context;     \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##AGG_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    result->opd[1] = imcs_operand(group_by);                            \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->reset = imcs_reset_binary_agg_iterator;                     \
    ctx->offset = ctx->count = 0;                                       \
    return result;                                                      \
}

#define IMCS_GROUP_AGG_INIT(val) (val)
#define IMCS_GROUP_MAX_ACCUMULATE(agg, val, count) (agg < val ? val : agg)
#define IMCS_GROUP_AGG_RESULT(agg, count) (agg)
IMCS_GROUP_AGG_DEF(int8, int8, group_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int16, int16, group_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int32, int32, group_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int64, int64, group_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(float, float, group_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

#define IMCS_GROUP_MIN_ACCUMULATE(agg, val, count) (agg > val ? val : agg)
IMCS_GROUP_AGG_DEF(int8, int8, group_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int16, int16, group_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int32, int32, group_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int64, int64, group_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(float, float, group_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

#define IMCS_GROUP_FIRST_ACCUMULATE(agg, val, count) (agg)
IMCS_GROUP_AGG_DEF(int8, int8, group_first, IMCS_GROUP_AGG_INIT, IMCS_GROUP_FIRST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int16, int16, group_first, IMCS_GROUP_AGG_INIT, IMCS_GROUP_FIRST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int32, int32, group_first, IMCS_GROUP_AGG_INIT, IMCS_GROUP_FIRST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int64, int64, group_first, IMCS_GROUP_AGG_INIT, IMCS_GROUP_FIRST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(float, float, group_first, IMCS_GROUP_AGG_INIT, IMCS_GROUP_FIRST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_first, IMCS_GROUP_AGG_INIT, IMCS_GROUP_FIRST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

#define IMCS_GROUP_LAST_ACCUMULATE(agg, val, count) (val)
IMCS_GROUP_AGG_DEF(int8, int8, group_last, IMCS_GROUP_AGG_INIT, IMCS_GROUP_LAST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int16, int16, group_last, IMCS_GROUP_AGG_INIT, IMCS_GROUP_LAST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int32, int32, group_last, IMCS_GROUP_AGG_INIT, IMCS_GROUP_LAST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int64, int64, group_last, IMCS_GROUP_AGG_INIT, IMCS_GROUP_LAST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(float, float, group_last, IMCS_GROUP_AGG_INIT, IMCS_GROUP_LAST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_last, IMCS_GROUP_AGG_INIT, IMCS_GROUP_LAST_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

#define IMCS_GROUP_SUM_ACCUMULATE(agg, val, count) (agg + val)
IMCS_GROUP_AGG_DEF(int8, int64, group_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int16, int64, group_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int32, int64, group_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int64, int64, group_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(float, double, group_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

#define IMCS_GROUP_ALL_ACCUMULATE(agg, val, count) (agg & val)
IMCS_GROUP_AGG_DEF(int8, int8, group_all, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ALL_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int16, int16, group_all, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ALL_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int32, int32, group_all, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ALL_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int64, int64, group_all, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ALL_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

imcs_iterator_h imcs_group_all_float(imcs_iterator_h iterator, imcs_iterator_h group_by)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_GROUP_ALL is supported only for integer types")))); 
    return NULL;
}
imcs_iterator_h imcs_group_all_double(imcs_iterator_h iterator, imcs_iterator_h group_by)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_GROUP_ALL is supported only for integer types")))); 
    return NULL;
}

#define IMCS_GROUP_ANY_ACCUMULATE(agg, val, count) (agg | val)
IMCS_GROUP_AGG_DEF(int8, int8, group_any, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ANY_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int16, int16, group_any, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ANY_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int32, int32, group_any, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ANY_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GROUP_AGG_DEF(int64, int64, group_any, IMCS_GROUP_AGG_INIT, IMCS_GROUP_ANY_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

imcs_iterator_h imcs_group_any_float(imcs_iterator_h iterator, imcs_iterator_h group_by)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_GROUP_ANY is supported only for integer types")))); 
    return NULL;
}
imcs_iterator_h imcs_group_any_double(imcs_iterator_h iterator, imcs_iterator_h group_by)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_GROUP_ANY is supported only for integer types")))); 
    return NULL;
}

#define IMCS_GROUP_AVG_RESULT(agg, count) (agg/count)
IMCS_GROUP_AGG_DEF(int8, double, group_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GROUP_AGG_DEF(int16, double, group_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GROUP_AGG_DEF(int32, double, group_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GROUP_AGG_DEF(int64, double, group_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GROUP_AGG_DEF(float, double, group_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)

#define IMCS_GROUP_VAR_INIT(val) (ctx->norm = (double)val*val, val)
#define IMCS_GROUP_VAR_ACCUMULATE(agg, val, count) (ctx->norm += (double)val*val, agg + val)
#define IMCS_GROUP_VAR_RESULT(agg, count) ((ctx->norm - agg*agg/count)/count)
IMCS_GROUP_AGG_DEF(int8, double, group_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GROUP_AGG_DEF(int16, double, group_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GROUP_AGG_DEF(int32, double, group_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GROUP_AGG_DEF(int64, double, group_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GROUP_AGG_DEF(float, double, group_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)

#define IMCS_GROUP_DEV_RESULT(agg, count) sqrt((ctx->norm - agg*agg/count)/count)
IMCS_GROUP_AGG_DEF(int8, double, group_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GROUP_AGG_DEF(int16, double, group_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GROUP_AGG_DEF(int32, double, group_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GROUP_AGG_DEF(int64, double, group_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GROUP_AGG_DEF(float, double, group_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GROUP_AGG_DEF(double, double, group_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)

static void imcs_reset_unary_agg_iterator(imcs_iterator_h iterator)
{
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context; 
    ctx->offset = ctx->count = 0;  
    imcs_reset_iterator(iterator);
}

static bool imcs_group_agg_count_next(imcs_iterator_h iterator) 
{                                                                       
    size_t i, j = 0, tile_size;                                     
    size_t this_tile_size = imcs_tile_size;                         
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context; 
    size_t elem_size = iterator->opd[0]->elem_size; 
    while (true) {                                                         
        if (ctx->offset >= iterator->opd[0]->tile_size) {                 
            if (!iterator->opd[0]->next(iterator->opd[0])) { 
                if (j + ctx->count != 0) {      
                    if (ctx->count != 0) {                              
                        iterator->tile.arr_int64[j++] = ctx->count;     
                        iterator->next_pos += 1;                     
                        ctx->count = 0;                                 
                    }                                                   
                    iterator->tile_size = j;                            
                    return true;                                      
                }                                                       
                return false;                                                  
            }                                                           
            ctx->offset = 0;                                            
        }                                                               
        tile_size = iterator->opd[0]->tile_size;                          
        for (i = ctx->offset; i < tile_size; i++) {                     
            bool same;                                              
            switch (elem_size) {                                        
              case 1:                                                   
                same = ctx->accumulator.val_int8 == iterator->opd[0]->tile.arr_int8[i]; 
                ctx->accumulator.val_int8 = iterator->opd[0]->tile.arr_int8[i]; 
                break;                                                  
              case 2:                                                   
                same = ctx->accumulator.val_int16 == iterator->opd[0]->tile.arr_int16[i]; 
                ctx->accumulator.val_int16 = iterator->opd[0]->tile.arr_int16[i];       
                break;                                                  
              case 4:                                                   
                same = ctx->accumulator.val_int32 == iterator->opd[0]->tile.arr_int32[i]; 
                ctx->accumulator.val_int32 = iterator->opd[0]->tile.arr_int32[i];       
                break;                                                  
              case 8:                                                  
                same = ctx->accumulator.val_int64 == iterator->opd[0]->tile.arr_int64[i]; 
                ctx->accumulator.val_int64 = iterator->opd[0]->tile.arr_int64[i]; 
                break;                                                  
              default:                                                  
                same = memcmp(ctx->history.arr_char, &iterator->opd[1]->tile.arr_char[i*elem_size], elem_size) == 0; 
                memcpy(ctx->history.arr_char, &iterator->opd[1]->tile.arr_char[i*elem_size], elem_size);              
            }                                                           
            if (ctx->count != 0 && !same) {                             
                iterator->tile.arr_int64[j] = ctx->count;               
                iterator->next_pos += 1;                             
                ctx->count = 1;                                         
                if (++j == this_tile_size) {                         
                    ctx->offset = i + 1;                                
                    iterator->tile_size = this_tile_size;            
                    return true;                                    
                }                                                       
            } else {                                                    
                ctx->count += 1;                                        
            }                                                           
        }                                                               
        ctx->offset = tile_size;                                        
    }                                                                   
}                                                                       
                                                                        
imcs_iterator_h imcs_group_count(imcs_iterator_h group_by) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(sizeof(imcs_count_t), sizeof(imcs_agg_context_t)); 
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context; 
    result->elem_type = TID_int64;                   
    result->opd[0] = imcs_operand(group_by);                                            
    result->next = imcs_group_agg_count_next;                        
    result->reset = imcs_reset_unary_agg_iterator;                   
    ctx->offset = ctx->count = 0;                                       
    return result;                                                    
}


#define IMCS_GRID_AGG_DEF(TYPE, AGG_TYPE, MNEM, INIT, ACCUMULATE, RESULT) \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    size_t i, j = 0, tile_size;                                         \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    size_t this_tile_size = imcs_tile_size;                             \
    while (true) {                                                      \
        if (ctx->offset >= iterator->opd[0]->tile_size) {               \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                if (j + ctx->count != 0) {                              \
                    if (ctx->count != 0) {                              \
                        iterator->tile.arr_##AGG_TYPE[j++] = RESULT(ctx->accumulator.val_##AGG_TYPE, ctx->count); \
                        iterator->next_pos += 1;                        \
                        ctx->count = 0;                                 \
                    }                                                   \
                    iterator->tile_size = j;                            \
                    return true;                                        \
                }                                                       \
                return false;                                           \
            }                                                           \
            ctx->offset = 0;                                            \
        }                                                               \
        tile_size = iterator->opd[0]->tile_size;                        \
        for (i = ctx->offset; i < tile_size; i++) {                     \
            ctx->accumulator.val_##AGG_TYPE = (ctx->count == 0)         \
                ? INIT(iterator->opd[0]->tile.arr_##TYPE[i])            \
                : ACCUMULATE(ctx->accumulator.val_##AGG_TYPE, iterator->opd[0]->tile.arr_##TYPE[i], ctx->count); \
            if (++ctx->count == ctx->interval) {                        \
                iterator->tile.arr_##AGG_TYPE[j] = RESULT(ctx->accumulator.val_##AGG_TYPE, ctx->count); \
                iterator->next_pos += 1;                                \
                ctx->count = 0;                                         \
                if (++j == this_tile_size) {                            \
                    ctx->offset = i + 1;                                \
                    iterator->tile_size = this_tile_size;               \
                    return true;                                        \
                }                                                       \
            }                                                           \
        }                                                               \
        ctx->offset = tile_size;                                        \
    }                                                                   \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input, size_t interval) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(AGG_TYPE), sizeof(imcs_agg_context_t)); \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context;     \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##AGG_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->reset = imcs_reset_unary_agg_iterator;                      \
    ctx->interval = interval;                                           \
    ctx->offset = ctx->count = 0;                                       \
    return result;                                                      \
}

IMCS_GRID_AGG_DEF(int8, int8, grid_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int16, int16, grid_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int32, int32, grid_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int64, int64, grid_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(float, float, grid_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(double, double, grid_max, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MAX_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

IMCS_GRID_AGG_DEF(int8, int8, grid_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int16, int16, grid_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int32, int32, grid_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int64, int64, grid_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(float, float, grid_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(double, double, grid_min, IMCS_GROUP_AGG_INIT, IMCS_GROUP_MIN_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

IMCS_GRID_AGG_DEF(int8, int64, grid_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int16, int64, grid_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int32, int64, grid_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(int64, int64, grid_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(float, double, grid_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)
IMCS_GRID_AGG_DEF(double, double, grid_sum, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AGG_RESULT)

IMCS_GRID_AGG_DEF(int8, double, grid_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GRID_AGG_DEF(int16, double, grid_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GRID_AGG_DEF(int32, double, grid_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GRID_AGG_DEF(int64, double, grid_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GRID_AGG_DEF(float, double, grid_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)
IMCS_GRID_AGG_DEF(double, double, grid_avg, IMCS_GROUP_AGG_INIT, IMCS_GROUP_SUM_ACCUMULATE, IMCS_GROUP_AVG_RESULT)

IMCS_GRID_AGG_DEF(int8, double, grid_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GRID_AGG_DEF(int16, double, grid_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GRID_AGG_DEF(int32, double, grid_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GRID_AGG_DEF(int64, double, grid_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GRID_AGG_DEF(float, double, grid_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)
IMCS_GRID_AGG_DEF(double, double, grid_var, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_VAR_RESULT)

IMCS_GRID_AGG_DEF(int8, double, grid_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GRID_AGG_DEF(int16, double, grid_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GRID_AGG_DEF(int32, double, grid_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GRID_AGG_DEF(int64, double, grid_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GRID_AGG_DEF(float, double, grid_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)
IMCS_GRID_AGG_DEF(double, double, grid_dev, IMCS_GROUP_VAR_INIT, IMCS_GROUP_VAR_ACCUMULATE, IMCS_GROUP_DEV_RESULT)


#define IMCS_DIFF_DEF(TYPE)                                             \
static bool imcs_diff_##TYPE##_next(imcs_iterator_h iterator)           \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##TYPE[i] = iterator->opd[0]->tile.arr_##TYPE[i] - ctx->accumulator.val_##TYPE; \
        ctx->accumulator.val_##TYPE = iterator->opd[0]->tile.arr_##TYPE[i]; \
    }                                                                   \
    iterator->next_pos += tile_size;                                    \
    iterator->tile_size = tile_size;                                    \
    return true;                                                        \
}                                                                       \
static void imcs_diff_##TYPE##_reset(imcs_iterator_h iterator)          \
{                                                                       \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    ctx->accumulator.val_##TYPE = 0;                                    \
    imcs_reset_iterator(iterator);                                      \
}                                                                       \
imcs_iterator_h imcs_diff_##TYPE(imcs_iterator_h input)                 \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_agg_context_t)); \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context;     \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##TYPE;                                     \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_diff_##TYPE##_next;                             \
    result->reset = imcs_diff_##TYPE##_reset;                           \
    ctx->accumulator.val_##TYPE = 0;                                    \
    return result;                                                      \
}

IMCS_DIFF_DEF(int8)
IMCS_DIFF_DEF(int16)
IMCS_DIFF_DEF(int32)
IMCS_DIFF_DEF(int64)
IMCS_DIFF_DEF(float)
IMCS_DIFF_DEF(double)

#define IMCS_DIFF0_DEF(TYPE)                                            \
static bool imcs_diff0_##TYPE##_next(imcs_iterator_h iterator)          \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    if (iterator->next_pos == 0) {                                      \
        ctx->accumulator.val_##TYPE = iterator->opd[0]->tile.arr_##TYPE[0]; \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##TYPE[i] = iterator->opd[0]->tile.arr_##TYPE[i] - ctx->accumulator.val_##TYPE; \
        ctx->accumulator.val_##TYPE = iterator->opd[0]->tile.arr_##TYPE[i]; \
    }                                                                   \
    iterator->next_pos += tile_size;                                    \
    iterator->tile_size = tile_size;                                    \
    return true;                                                        \
}                                                                       \
imcs_iterator_h imcs_diff0_##TYPE(imcs_iterator_h input)                \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_agg_context_t)); \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_##TYPE;                                     \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_diff0_##TYPE##_next;                            \
    return result;                                                      \
}

IMCS_DIFF0_DEF(int8)
IMCS_DIFF0_DEF(int16)
IMCS_DIFF0_DEF(int32)
IMCS_DIFF0_DEF(int64)
IMCS_DIFF0_DEF(float)
IMCS_DIFF0_DEF(double)

typedef struct imcs_concat_context_t_ {
    size_t left_offs;
    size_t right_offs;
    bool   left_array;
    imcs_iterator_h opd[2];
} imcs_concat_context_t;

static bool imcs_concat_next(imcs_iterator_h iterator)         
{                                                                       
    imcs_concat_context_t* ctx = (imcs_concat_context_t*)iterator->context; 
    size_t tile_size = 0; 
    size_t elem_size = iterator->elem_size;
    size_t this_tile_size = imcs_tile_size; 
    size_t available;

    while (true) { 
        if (ctx->left_array) {            
            if (ctx->left_offs != 0) { 
                available = iterator->opd[0]->tile_size - ctx->left_offs;
                if (available > this_tile_size - tile_size) { 
                    available = this_tile_size - tile_size;
                }
                memcpy(iterator->tile.arr_char + tile_size*elem_size, iterator->opd[0]->tile.arr_char + ctx->left_offs*elem_size, available*elem_size); 
                tile_size += available;
                ctx->left_offs += available;
                if (tile_size == this_tile_size) { 
                    break;
                }
            }
            if (!iterator->opd[0]->next(iterator->opd[0])) { 
                if (iterator->opd[1]->next == imcs_concat_next) { 
                    iterator->opd[0] = iterator->opd[1]->opd[0];
                    iterator->opd[1] = iterator->opd[1]->opd[1];
                    ctx->left_offs = 0;
                    continue;
                }
                ctx->left_array = false;
            } else {                                    
                available = iterator->opd[0]->tile_size;
                if (available > this_tile_size - tile_size) { 
                    ctx->left_offs = available = this_tile_size - tile_size;
                } else { 
                    ctx->left_offs = 0;
                }
                memcpy(iterator->tile.arr_char + tile_size*elem_size, iterator->opd[0]->tile.arr_char, available*elem_size); 
                tile_size += available;
                if (tile_size == this_tile_size) { 
                    break;
                } 
                continue;
            }
        } else if (ctx->right_offs != 0) { 
            available = iterator->opd[1]->tile_size - ctx->right_offs;       
            if (available > this_tile_size - tile_size) { 
                available = this_tile_size - tile_size;
            }
            memcpy(iterator->tile.arr_char + tile_size*elem_size, iterator->opd[1]->tile.arr_char + ctx->right_offs*elem_size, available*elem_size); 
            tile_size += available;
            ctx->right_offs += available;
            if (tile_size == this_tile_size) { 
                break;
            }
        }                                                                   
        ctx->right_offs = 0;
        if (!iterator->opd[1]->next(iterator->opd[1])) { 
            if (tile_size == 0) {
                return false;                                                      
            }
        } else {            
            available = iterator->opd[1]->tile_size;
            if (available > this_tile_size - tile_size) { 
                ctx->right_offs = available = this_tile_size - tile_size;
            }
            memcpy(iterator->tile.arr_char + tile_size*elem_size, iterator->opd[1]->tile.arr_char, available*elem_size); 
            tile_size += available;
        }
        break;
    }
    iterator->tile_size = tile_size;                            
    iterator->next_pos += tile_size;                         
    return true;                                                    
}                                                                       

static void imcs_concat_reset(imcs_iterator_h iterator)         
{    
    imcs_concat_context_t* ctx = (imcs_concat_context_t*)iterator->context; 
    ctx->left_array = true;                                          
    ctx->left_offs = ctx->right_offs = 0;                                                
    iterator->opd[0] = ctx->opd[0];
    iterator->opd[1] = ctx->opd[1];
    imcs_reset_iterator(iterator);
}

imcs_iterator_h imcs_concat(imcs_iterator_h left, imcs_iterator_h right) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(left->elem_size, sizeof(imcs_concat_context_t)); 
    imcs_concat_context_t* ctx = (imcs_concat_context_t*)result->context; 
    IMCS_CHECK_TYPE(left->elem_type, right->elem_type);
    if (left->elem_size != right->elem_size) {                          
        ereport(ERROR, (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH), (errmsg("timeseries of CHAR have different element size")))); 
    }                                                                   
    result->elem_type = left->elem_type;
    ctx->opd[0] = result->opd[0] = imcs_operand(left);                                                
    ctx->opd[1] = result->opd[1] = imcs_operand(right);                                              
    result->next = imcs_concat_next;                                 
    result->reset = imcs_concat_reset;                               
    ctx->left_array = true;                                          
    ctx->left_offs = ctx->right_offs = 0;                                                
    return result;                                                    
}

#define IMCS_IIF_DEF(TYPE)                                              \
static bool imcs_iif_##TYPE##_next(imcs_iterator_h iterator)            \
{                                                                       \
    size_t i, tile_size;                                                \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    if (!iterator->opd[1]->next(iterator->opd[1])) {                    \
        return false;                                                   \
    }                                                                   \
    if (tile_size > iterator->opd[1]->tile_size) {                      \
        tile_size = iterator->opd[1]->tile_size;                        \
    }                                                                   \
    if (!iterator->opd[2]->next(iterator->opd[2])) {                    \
        return false;                                                   \
    }                                                                   \
    if (tile_size > iterator->opd[2]->tile_size) {                      \
        tile_size = iterator->opd[2]->tile_size;                        \
    }                                                                   \
    for (i = 0; i < tile_size; i++) {                                   \
        iterator->tile.arr_##TYPE[i] = iterator->opd[0]->tile.arr_int8[i] ? iterator->opd[1]->tile.arr_##TYPE[i] : iterator->opd[2]->tile.arr_##TYPE[i]; \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_iif_##TYPE(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), 0);        \
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);                         \
    IMCS_CHECK_TYPE(then_iter->elem_type, TID_##TYPE);                  \
    IMCS_CHECK_TYPE(else_iter->elem_type, TID_##TYPE);                  \
    result->elem_type = then_iter->elem_type;                           \
    result->opd[0] = imcs_operand(cond);                                \
    result->opd[1] = imcs_operand(then_iter);                           \
    result->opd[2] = imcs_operand(else_iter);                           \
    result->next = imcs_iif_##TYPE##_next;                              \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    return result;                                                      \
}

IMCS_IIF_DEF(int8)
IMCS_IIF_DEF(int16)
IMCS_IIF_DEF(int32)
IMCS_IIF_DEF(int64)
IMCS_IIF_DEF(float)
IMCS_IIF_DEF(double)

static bool imcs_iif_char_next(imcs_iterator_h iterator)   
{                                                                       
    size_t i, tile_size;                                            
    size_t elem_size = iterator->elem_size;
    if (!iterator->opd[0]->next(iterator->opd[0])) { 
        return false;                                                      
    }                                                                   
    tile_size = iterator->opd[0]->tile_size;                              
    if (!iterator->opd[1]->next(iterator->opd[1])) { 
        return false;                                                      
    }                                                                   
    if (tile_size > iterator->opd[1]->tile_size) {                        
        tile_size = iterator->opd[1]->tile_size;                          
    }                                                                   
    if (!iterator->opd[2]->next(iterator->opd[2])) { 
        return false;                                                      
    }                                                                   
    if (tile_size > iterator->opd[2]->tile_size) {                        
        tile_size = iterator->opd[2]->tile_size;                          
    }                                                                   
    for (i = 0; i < tile_size; i++) {                                   
        memcpy(iterator->tile.arr_char + i*elem_size, (iterator->opd[0]->tile.arr_int8[i] ? iterator->opd[1]->tile.arr_char : iterator->opd[2]->tile.arr_char) + i*elem_size, elem_size); 
    }                                                                   
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                 
    return true;                                                    
}                                                                       
                                                                        
imcs_iterator_h imcs_iif_char(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(then_iter->elem_size, 0);   
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);
    IMCS_CHECK_TYPE(then_iter->elem_type, TID_char);
    IMCS_CHECK_TYPE(else_iter->elem_type, TID_char);
    if (then_iter->elem_size != else_iter->elem_size) {                
        ereport(ERROR, (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH), (errmsg("timeseries of CHAR have different element size")))); 
    }                                                                   
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(cond);                                                
    result->opd[1] = imcs_operand(then_iter);                                         
    result->opd[2] = imcs_operand(else_iter);                                         
    result->next = imcs_iif_char_next;                           
    result->flags = FLAG_CONTEXT_FREE;                                  
    return result;                                                    
}

typedef struct imcs_if_context_t_ { 
    size_t then_offs;
    size_t else_offs;
} imcs_if_context_t;


#define IMCS_IF_DEF(TYPE)                                               \
static bool imcs_if_##TYPE##_next(imcs_iterator_h iterator)             \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_if_context_t* ctx = (imcs_if_context_t*)iterator->context;     \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        if (iterator->opd[0]->tile.arr_int8[i]) {                       \
            if (ctx->then_offs >= iterator->opd[1]->tile_size) {        \
                if (!iterator->opd[1]->next(iterator->opd[1])) {        \
                    return false;                                       \
                }                                                       \
                Assert(iterator->opd[1]->tile_size > 0);                \
                ctx->then_offs = 0;                                     \
            }                                                           \
            iterator->tile.arr_##TYPE[i] = iterator->opd[1]->tile.arr_##TYPE[ctx->then_offs++] ; \
        } else {                                                        \
            if (ctx->else_offs >= iterator->opd[2]->tile_size) {        \
                if (!iterator->opd[2]->next(iterator->opd[2])) {        \
                    return false;                                       \
                }                                                       \
                Assert(iterator->opd[2]->tile_size > 0);                \
                ctx->else_offs = 0;                                     \
            }                                                           \
            iterator->tile.arr_##TYPE[i] = iterator->opd[2]->tile.arr_##TYPE[ctx->else_offs++] ; \
        }                                                               \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_if_##TYPE(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_if_context_t)); \
    imcs_if_context_t* ctx = (imcs_if_context_t*)result->context;       \
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);                         \
    IMCS_CHECK_TYPE(then_iter->elem_type, TID_##TYPE);                  \
    IMCS_CHECK_TYPE(else_iter->elem_type, TID_##TYPE);                  \
    result->elem_type = then_iter->elem_type;                           \
    result->opd[0] = imcs_operand(cond);                                \
    result->opd[1] = imcs_operand(then_iter);                           \
    result->opd[2] = imcs_operand(else_iter);                           \
    ctx->then_offs = ctx->else_offs = 0;                                \
    result->next = imcs_if_##TYPE##_next;                               \
    return result;                                                      \
}

IMCS_IF_DEF(int8)
IMCS_IF_DEF(int16)
IMCS_IF_DEF(int32)
IMCS_IF_DEF(int64)
IMCS_IF_DEF(float)
IMCS_IF_DEF(double)

static bool imcs_if_char_next(imcs_iterator_h iterator)   
{                                                                       
    size_t i, tile_size;                                            
    size_t elem_size = iterator->elem_size;
    imcs_if_context_t* ctx = (imcs_if_context_t*)iterator->context; 
    if (!iterator->opd[0]->next(iterator->opd[0])) { 
        return false;                                                      
    }                                                                   
    tile_size = iterator->opd[0]->tile_size;                              
    for (i = 0; i < tile_size; i++) {                                   
        if (iterator->opd[0]->tile.arr_int8[i]) {                 
            if (ctx->then_offs >= iterator->opd[1]->tile_size) {          
                if (!iterator->opd[1]->next(iterator->opd[1])) {               
                    return false;                                          
                }                                                       
                Assert(iterator->opd[1]->tile_size > 0); 
                ctx->then_offs = 0;                                     
            }                                                           
            memcpy(iterator->tile.arr_char + i*elem_size, iterator->opd[1]->tile.arr_char + elem_size*ctx->then_offs++, elem_size); 
        } else {                                                        
            if (ctx->else_offs >= iterator->opd[2]->tile_size) {          
                if (!iterator->opd[2]->next(iterator->opd[2])) {               
                    return false;                                          
                }                                                       
                Assert(iterator->opd[2]->tile_size > 0); 
                ctx->else_offs = 0;                                     
            }                                                           
            memcpy(iterator->tile.arr_char + i*elem_size, iterator->opd[2]->tile.arr_char + elem_size*ctx->else_offs++, elem_size); 
        }                                                               
    }                                                                   
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                 
    return true;                                                    
}                                                                       
                                                                        
imcs_iterator_h imcs_if_char(imcs_iterator_h cond, imcs_iterator_h then_iter, imcs_iterator_h else_iter) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(then_iter->elem_size, sizeof(imcs_if_context_t)); 
    imcs_if_context_t* ctx = (imcs_if_context_t*)result->context; 
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);
    IMCS_CHECK_TYPE(then_iter->elem_type, TID_char);
    IMCS_CHECK_TYPE(else_iter->elem_type, TID_char);
    if (then_iter->elem_size != else_iter->elem_size) {                
        ereport(ERROR, (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH), (errmsg("timeseries of CHAR have different element size")))); 
    }                                                                   
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(cond);                                                
    result->opd[1] = imcs_operand(then_iter);                                         
    result->opd[2] = imcs_operand(else_iter);                                         
    ctx->then_offs = ctx->else_offs = 0;                                
    result->next = imcs_if_char_next;                            
    return result;                                                    
}

typedef struct imcs_filter_context_t_ { 
    size_t left_offs;
    size_t right_offs;
} imcs_filter_context_t;

static void imcs_filter_reset(imcs_iterator_h iterator)   
{
    imcs_filter_context_t* ctx = (imcs_filter_context_t*)iterator->context; 
    ctx->left_offs = ctx->right_offs = 0;                               
    imcs_reset_iterator(iterator);
}
#define IMCS_FILTER_DEF(TYPE)                                           \
static bool imcs_filter_##TYPE##_next(imcs_iterator_h iterator)         \
{                                                                       \
    size_t i, n, tile_size = 0;                                         \
    imcs_filter_context_t* ctx = (imcs_filter_context_t*)iterator->context; \
    size_t this_tile_size = imcs_tile_size;                             \
    do {                                                                \
        if (ctx->left_offs >= iterator->opd[0]->tile_size) {            \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                if (tile_size != 0) {                                   \
                    iterator->tile_size = tile_size;                    \
                    iterator->next_pos += tile_size;                    \
                    return true;                                        \
                }                                                       \
                return false;                                           \
            }                                                           \
            ctx->left_offs = 0;                                         \
        }                                                               \
        if (ctx->right_offs >= iterator->opd[1]->tile_size) {           \
            if (!iterator->opd[1]->next(iterator->opd[1])) {            \
                if (tile_size != 0) {                                   \
                    iterator->tile_size = tile_size;                    \
                    iterator->next_pos += tile_size;                    \
                    return true;                                        \
                }                                                       \
                return false;                                           \
            }                                                           \
            ctx->right_offs = 0;                                        \
        }                                                               \
        n = iterator->opd[0]->tile_size - ctx->left_offs;               \
        if (n > iterator->opd[1]->tile_size - ctx->right_offs) {        \
            n = iterator->opd[1]->tile_size - ctx->right_offs;          \
        }                                                               \
        for (i = 0; i < n; i++) {                                       \
            if (iterator->opd[0]->tile.arr_int8[ctx->left_offs + i]) { \
                iterator->tile.arr_##TYPE[tile_size] = iterator->opd[1]->tile.arr_##TYPE[ctx->right_offs + i];\
                if (++tile_size == this_tile_size) {                    \
                    i += 1;                                             \
                    break;                                              \
                }                                                       \
            }                                                           \
        }                                                               \
        ctx->left_offs += i;                                            \
        ctx->right_offs += i;                                           \
    } while (tile_size < this_tile_size);                               \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_filter_##TYPE(imcs_iterator_h cond, imcs_iterator_h input) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_filter_context_t)); \
    imcs_filter_context_t* ctx = (imcs_filter_context_t*)result->context; \
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);                         \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = input->elem_type;                               \
    result->opd[0] = imcs_operand(cond);                                \
    result->opd[1] = imcs_operand(input);                               \
    result->next = imcs_filter_##TYPE##_next;                           \
    result->reset = imcs_filter_reset;                                  \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    ctx->left_offs = ctx->right_offs = 0;                               \
    return result;                                                      \
}

IMCS_FILTER_DEF(int8)
IMCS_FILTER_DEF(int16)
IMCS_FILTER_DEF(int32)
IMCS_FILTER_DEF(int64)
IMCS_FILTER_DEF(float)
IMCS_FILTER_DEF(double)

static bool imcs_filter_char_next(imcs_iterator_h iterator) 
{                                                                       
    size_t i, n, tile_size = 0;                                     
    size_t this_tile_size = imcs_tile_size;                         
    size_t elem_size = iterator->elem_size;
    imcs_filter_context_t* ctx = (imcs_filter_context_t*)iterator->context; 
    do {                                                                
        if (ctx->left_offs >= iterator->opd[0]->tile_size) {              
            if (!iterator->opd[0]->next(iterator->opd[0])) { 
                if (tile_size != 0) {               
                    iterator->tile_size = tile_size;                        
                    iterator->next_pos += tile_size;                     
                    return true;                                        
                }                                                           
                return false;                                               
            }                                                           
            ctx->left_offs = 0;                                         
        }                                                               
        if (ctx->right_offs >= iterator->opd[1]->tile_size) {            
            if (!iterator->opd[1]->next(iterator->opd[1])) { 
                if (tile_size != 0) {               
                    iterator->tile_size = tile_size;                        
                    iterator->next_pos += tile_size;                     
                    return true;                                        
                }                                                           
                return false;                                               
            }                                                           
            ctx->right_offs = 0;                                        
        }                                                               
        n = iterator->opd[0]->tile_size - ctx->left_offs;                 
        if (n > iterator->opd[1]->tile_size - ctx->right_offs) {         
            n = iterator->opd[1]->tile_size - ctx->right_offs;           
        }                                                               
        for (i = 0; i < n; i++) {                                       
            if (iterator->opd[0]->tile.arr_int8[ctx->left_offs + i]) { 
                memcpy(iterator->tile.arr_char + elem_size*tile_size, iterator->opd[1]->tile.arr_char + elem_size*(ctx->right_offs + i), elem_size);
                if (++tile_size == this_tile_size) {                 
                    i += 1;                                             
                    break;                                              
                }                                                       
            }                                                           
        }                                                               
        ctx->left_offs += i;                                            
        ctx->right_offs += i;                                           
    } while (tile_size < this_tile_size);                            
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                 
    return true;                                                    
}                                                                       
                                                                        
imcs_iterator_h imcs_filter_char(imcs_iterator_h cond, imcs_iterator_h input) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(input->elem_size, sizeof(imcs_filter_context_t)); 
    imcs_filter_context_t* ctx = (imcs_filter_context_t*)result->context; 
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);
    IMCS_CHECK_TYPE(input->elem_type, TID_char);
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(cond);                                                
    result->opd[1] = imcs_operand(input);                                              
    result->next = imcs_filter_char_next;                        
    result->reset = imcs_filter_reset;   
    result->flags = FLAG_CONTEXT_FREE;                            
    ctx->left_offs = ctx->right_offs = 0;                               
    return result;                                                    
}


typedef struct imcs_filter_pos_context_t_ { 
    size_t     offs;
    imcs_pos_t origin;
} imcs_filter_pos_context_t;


static void imcs_filter_pos_reset(imcs_iterator_h iterator)   
{
    imcs_filter_pos_context_t* ctx = (imcs_filter_pos_context_t*)iterator->context; 
    ctx->offs = 0;
    imcs_reset_iterator(iterator);
}
static bool imcs_filter_pos_next(imcs_iterator_h iterator) 
{                                                                       
    size_t i, n, tile_size = 0;                                     
    size_t this_tile_size = imcs_tile_size;                         
    imcs_filter_pos_context_t* ctx = (imcs_filter_pos_context_t*)iterator->context; 
    int64 pos;
    do {                                                                
        if (ctx->offs >= iterator->opd[0]->tile_size) {              
            if (!iterator->opd[0]->next(iterator->opd[0])) { 
                if (tile_size != 0) {             
                    iterator->tile_size = tile_size;                        
                    iterator->next_pos += tile_size;                     
                    return true;                                        
                }                                                           
                return false;                                               
            }                                                           
            ctx->offs = 0;                                         
        }                                                               
        n = iterator->opd[0]->tile_size - ctx->offs;                 
        pos = iterator->opd[0]->next_pos - n - ctx->origin;
        for (i = 0; i < n; i++) {                                       
            if (iterator->opd[0]->tile.arr_int8[ctx->offs + i]) { 
                iterator->tile.arr_int64[tile_size] = pos + i; 
                if (++tile_size == this_tile_size) {                 
                    i += 1;                                             
                    break;                                              
                }                                                       
            }                                                           
        }                                                               
        ctx->offs += i;                                            
    } while (tile_size < this_tile_size);                            
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                 
    return true;                                                    
}                                                                       
                                                                        
imcs_iterator_h imcs_filter_pos(imcs_iterator_h cond) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(sizeof(imcs_pos_t), sizeof(imcs_filter_pos_context_t)); 
    imcs_filter_pos_context_t* ctx = (imcs_filter_pos_context_t*)result->context; 
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);
    result->elem_type = TID_int64;                        
    result->opd[0] = imcs_operand(cond);                                                    
    result->next = imcs_filter_pos_next;                    
    result->reset = imcs_filter_pos_reset;                           
    result->flags = FLAG_CONTEXT_FREE;                            
    ctx->offs = 0;
    ctx->origin = cond->first_pos;
    return result;                                                    
}


static int imcs_get_first_pos(imcs_iterator_h iterator)
{
    imcs_pos_t max_first_pos = iterator->first_pos;
    int i;
    for (i = 0; i < 3; i++) { 
        if (iterator->opd[i]) { 
            imcs_pos_t first_pos = imcs_get_first_pos(iterator->opd[i]);
            if (first_pos > max_first_pos) { 
                max_first_pos = first_pos;
            }
        }
    }
    return max_first_pos;
}

static void imcs_filter_first_pos_merge(imcs_iterator_h dst, imcs_iterator_h src)
{
    size_t n = dst->last_pos;
    size_t tile_size = dst->tile_size;
    size_t l = 0, r = tile_size;
    imcs_pos_t pos = src->tile.arr_int64[0];
    size_t ins = src->tile_size;
    do { 
        size_t m = (l + r) >> 1;
        if (dst->tile.arr_int64[m] < pos) { 
            l = m+1;
        } else { 
            r = m;
        }
    } while (l < r);

    if (l + ins > n) { 
        ins = n - l;
    }
    if (ins > 0) { 
        size_t shift = tile_size - l;
        if (l + ins + shift > n) { 
            shift = n - l - ins;
        }
        memmove(&dst->tile.arr_int64[l+ins], &dst->tile.arr_int64[l], shift*sizeof(imcs_pos_t));
        memcpy(&dst->tile.arr_int64[l], src->tile.arr_int64, ins*sizeof(imcs_pos_t));
        dst->next_pos = dst->tile_size = l + ins + shift;
    }
}

static bool imcs_filter_first_pos_next(imcs_iterator_h iterator)
{
    size_t n = iterator->last_pos;
    size_t this_tile_size = 0;
    imcs_pos_t pos;
    if (iterator->flags & FLAG_PREPARED) {                              
        return iterator->tile_size != 0;                                
    }                                                                   
    if (iterator->next_pos != 0) {                                      
        return false;                                                   
    }                   
    pos = imcs_get_first_pos(iterator);
    while (this_tile_size < n && iterator->opd[0]->next(iterator->opd[0])) {                  
        size_t i, tile_size = iterator->opd[0]->tile_size;              
        for (i = 0; i < tile_size; i++, pos++) {                               
            if (iterator->opd[0]->tile.arr_int8[i]) { 
                iterator->tile.arr_int64[this_tile_size] = pos;
                if (++this_tile_size >= n) { 
                    break;
                }
            }
        }
    }
    iterator->tile_size = this_tile_size;                                    
    iterator->next_pos = this_tile_size;                                 
    return this_tile_size != 0;                                                    
}                                                                       
                
    
imcs_iterator_h imcs_filter_first_pos(imcs_iterator_h cond, size_t n) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(sizeof(imcs_pos_t), 0); 
    IMCS_CHECK_TYPE(cond->elem_type, TID_int8);
    if (n > imcs_tile_size) {                                         
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("N should not be larger than tile size")))); 
    }                                                                   
    result->elem_type = TID_int64;                        
    result->opd[0] = imcs_operand(cond);                                                    
    result->next = imcs_filter_first_pos_next;                    
    result->prepare = imcs_filter_first_pos_next;                    
    result->merge = imcs_filter_first_pos_merge;                           
    result->last_pos = n;
    return result;                                                    
}


typedef struct imcs_top_context_t_ {               
    size_t top;
} imcs_top_context_t;                              

#define IMCS_TOP_DEF(TYPE, MNEM, CMP)                                   \
static void imcs_##MNEM##_##TYPE##_merge(imcs_iterator_h dst, imcs_iterator_h src)  \
{                                                                       \
    imcs_top_context_t* ctx = (imcs_top_context_t*)dst->context;        \
    size_t top = ctx->top;                                              \
    size_t dst_pos = 0;                                                 \
    size_t src_pos = 0;                                                 \
    size_t dst_tile_size = dst->tile_size;                              \
    size_t src_tile_size = src->tile_size;                              \
    Assert(dst_pos < dst_tile_size && src_pos < src_tile_size);         \
    while (true) {                                                      \
        if (!(dst->tile.arr_##TYPE[dst_pos] CMP src->tile.arr_##TYPE[src_pos])) { \
            if (dst_tile_size < top) {                                  \
                dst_tile_size += 1;                                     \
            }                                                           \
            memmove(&dst->tile.arr_##TYPE[dst_pos+1], &dst->tile.arr_##TYPE[dst_pos], (dst_tile_size - dst_pos - 1)*sizeof(TYPE)); \
            dst->tile.arr_##TYPE[dst_pos++] = src->tile.arr_##TYPE[src_pos++];  \
            if (dst_pos == dst_tile_size || src_pos == src_tile_size) { \
                /* either there are no more items in src, either dst_pos == top */  \
                break;                                                  \
            }                                                           \
        } else {                                                        \
            if (dst->tile.arr_##TYPE[dst_pos] == src->tile.arr_##TYPE[src_pos] && ++src_pos == src_tile_size) { \
                break;                                                  \
            }                                                           \
            if (++dst_pos == dst_tile_size) {                           \
                if (dst_pos < top) {                                    \
                    dst_tile_size += top - dst_pos < src_tile_size - src_pos ? top - dst_pos : src_tile_size - src_pos; \
                    memcpy(&dst->tile.arr_##TYPE[dst_pos], &src->tile.arr_##TYPE[src_pos], (dst_tile_size - dst_pos)*sizeof(TYPE)); \
                }                                                       \
                break;                                                  \
            }                                                           \
        }                                                               \
    }                                                                   \
    dst->next_pos = dst->tile_size = dst_tile_size;                     \
}                                                                       \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    imcs_top_context_t* ctx = (imcs_top_context_t*)iterator->context;   \
    size_t n = 0, top = ctx->top;                                       \
    if (iterator->flags & FLAG_PREPARED) {                              \
        return iterator->tile_size != 0;                                \
    }                                                                   \
    if (iterator->next_pos != 0) {                                      \
        return false;                                                   \
    }                                                                   \
    while (iterator->opd[0]->next(iterator->opd[0])) {                  \
        size_t i, tile_size = iterator->opd[0]->tile_size;              \
        for (i = 0; i < tile_size; i++) {                               \
            TYPE val = iterator->opd[0]->tile.arr_##TYPE[i];            \
            if (n < top || !(iterator->tile.arr_##TYPE[top-1] CMP val)) { \
                size_t l = 0, r = n;                                    \
                while (l < r) {                                         \
                    size_t m = (l + r) >> 1;                            \
                    if (iterator->tile.arr_##TYPE[m] CMP val) {         \
                        l = m + 1;                                      \
                    } else {                                            \
                        r = m;                                          \
                    }                                                   \
                }                                                       \
                if (n < top) {                                          \
                    n += 1;                                             \
                }                                                       \
                if (n > r) {                                            \
                    memmove(&iterator->tile.arr_##TYPE[r+1], &iterator->tile.arr_##TYPE[r], (n-r-1)*sizeof(TYPE)); \
                }                                                       \
                iterator->tile.arr_##TYPE[r] = val;                     \
            }                                                           \
        }                                                               \
    }                                                                   \
    iterator->next_pos = n;                                             \
    iterator->tile_size = n;                                            \
    return n != 0;                                                      \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input, size_t top) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_top_context_t)); \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    if (top > imcs_tile_size) {                                         \
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("TOP value should not be larger than tile size")))); \
    }                                                                   \
    result->elem_type = input->elem_type;                               \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->prepare = imcs_##MNEM##_##TYPE##_next;                      \
    result->merge = imcs_##MNEM##_##TYPE##_merge;                       \
    ((imcs_top_context_t*)result->context)->top = top;                  \
    return result;                                                      \
}

IMCS_TOP_DEF(int8, top_max, >=)
IMCS_TOP_DEF(int16, top_max, >=) 
IMCS_TOP_DEF(int32, top_max, >=)
IMCS_TOP_DEF(int64, top_max, >=) 
IMCS_TOP_DEF(float, top_max, >=)
IMCS_TOP_DEF(double, top_max, >=) 


IMCS_TOP_DEF(int8, top_min, <=)
IMCS_TOP_DEF(int16, top_min, <=) 
IMCS_TOP_DEF(int32, top_min, <=)
IMCS_TOP_DEF(int64, top_min, <=) 
IMCS_TOP_DEF(float, top_min, <=)
IMCS_TOP_DEF(double, top_min, <=) 


#define IMCS_TOP_POS_DEF(TYPE, MNEM, CMP)                               \
typedef struct imcs_top_pos_##MNEM##_##TYPE##_context_t_ {              \
    size_t top;                                                         \
    imcs_pos_t origin;                                                  \
    TYPE   values[1];                                                   \
} imcs_top_pos_##MNEM##_##TYPE##_context_t;                             \
static void imcs_##MNEM##_##TYPE##_merge(imcs_iterator_h dst, imcs_iterator_h src)  \
{                                                                       \
    imcs_top_pos_##MNEM##_##TYPE##_context_t* dst_ctx = (imcs_top_pos_##MNEM##_##TYPE##_context_t*)dst->context; \
    imcs_top_pos_##MNEM##_##TYPE##_context_t* src_ctx = (imcs_top_pos_##MNEM##_##TYPE##_context_t*)src->context; \
    size_t top = dst_ctx->top;                                          \
    size_t dst_pos = 0;                                                 \
    size_t src_pos = 0;                                                 \
    size_t dst_tile_size = dst->tile_size;                              \
    size_t src_tile_size = src->tile_size;                              \
    Assert(dst_pos < dst_tile_size && src_pos < src_tile_size);         \
    while (true) {                                                      \
        if (!(dst_ctx->values[dst_pos] CMP src_ctx->values[src_pos])) { \
            if (dst_tile_size < top) {                                  \
                dst_tile_size += 1;                                     \
            }                                                           \
            memmove(&dst->tile.arr_int64[dst_pos+1], &dst->tile.arr_int64[dst_pos], (dst_tile_size - dst_pos - 1)*sizeof(int64)); \
            dst->tile.arr_int64[dst_pos] = src->tile.arr_int64[src_pos]; \
            memmove(&dst_ctx->values[dst_pos+1], &dst_ctx->values[dst_pos], (dst_tile_size - dst_pos - 1)*sizeof(TYPE)); \
            dst_ctx->values[dst_pos++] = src_ctx->values[src_pos++];    \
            if (dst_pos == dst_tile_size || src_pos == src_tile_size) { \
                /* either there are no more items in src, either dst_pos == top */  \
                break;                                                  \
            }                                                           \
        } else {                                                        \
            if (dst_ctx->values[dst_pos] == src_ctx->values[src_pos] && ++src_pos == src_tile_size) { \
                break;                                                  \
            }                                                           \
            if (++dst_pos == dst_tile_size) {                           \
                if (dst_pos < top) {                                    \
                    dst_tile_size += top - dst_pos < src_tile_size - src_pos ? top - dst_pos : src_tile_size - src_pos; \
                    memcpy(&dst->tile.arr_int64[dst_pos], &src->tile.arr_int64[src_pos], (dst_tile_size - dst_pos)*sizeof(int64)); \
                    memcpy(&dst_ctx->values[dst_pos], &src_ctx->values[src_pos], (dst_tile_size - dst_pos)*sizeof(TYPE)); \
                }                                                       \
                break;                                                  \
            }                                                           \
        }                                                               \
    }                                                                   \
    dst->next_pos = dst->tile_size = dst_tile_size;                     \
}                                                                       \
static bool imcs_##MNEM##_##TYPE##_next(imcs_iterator_h iterator)       \
{                                                                       \
    imcs_top_pos_##MNEM##_##TYPE##_context_t* ctx = (imcs_top_pos_##MNEM##_##TYPE##_context_t*)iterator->context; \
    TYPE* buf = ctx->values;                                            \
    size_t n = 0, top = ctx->top;                                       \
    imcs_pos_t pos = iterator->opd[0]->next_pos - ctx->origin;          \
    if (iterator->flags & FLAG_PREPARED) {                              \
        return iterator->tile_size != 0;                                \
    }                                                                   \
    if (iterator->next_pos != 0) {                                      \
        return false;                                                   \
    }                                                                   \
    while (iterator->opd[0]->next(iterator->opd[0])) {                  \
        size_t i, tile_size = iterator->opd[0]->tile_size;              \
        for (i = 0; i < tile_size; i++) {                               \
            TYPE val = iterator->opd[0]->tile.arr_##TYPE[i];            \
            if (n < top || !(buf[top-1] CMP val)) {                     \
                size_t l = 0, r = n;                                    \
                while (l < r) {                                         \
                    size_t m = (l + r) >> 1;                            \
                    if (buf[m] CMP val) {                               \
                        l = m + 1;                                      \
                    } else {                                            \
                        r = m;                                          \
                    }                                                   \
                }                                                       \
                if (n < top) {                                          \
                    n += 1;                                             \
                }                                                       \
                if (n > r) {                                            \
                    memmove(&iterator->tile.arr_int64[r+1], &iterator->tile.arr_int64[r], (n-r-1)*sizeof(int64)); \
                    memmove(&buf[r+1], &buf[r], (n-r-1)*sizeof(TYPE));  \
                }                                                       \
                buf[r] = val;                                           \
                iterator->tile.arr_int64[r] = pos + i;                  \
            }                                                           \
        }                                                               \
        pos = iterator->opd[0]->next_pos - ctx->origin;                 \
    }                                                                   \
    iterator->next_pos = n;                                             \
    iterator->tile_size = n;                                            \
    return n != 0;                                                      \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##MNEM##_##TYPE(imcs_iterator_h input, size_t top) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(imcs_pos_t), sizeof(imcs_top_pos_##MNEM##_##TYPE##_context_t) + (top-1)*sizeof(TYPE)); \
    imcs_top_pos_##MNEM##_##TYPE##_context_t* ctx = (imcs_top_pos_##MNEM##_##TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    if (top > imcs_tile_size) {                                         \
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("TOP value should not be larger than tile size")))); \
    }                                                                   \
    result->elem_type = TID_int64;                                      \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##MNEM##_##TYPE##_next;                         \
    result->prepare = imcs_##MNEM##_##TYPE##_next;                      \
    result->merge = imcs_##MNEM##_##TYPE##_merge;                       \
    ctx->top = top;                                                     \
    ctx->origin = input->first_pos;                                     \
    return result;                                                      \
}

IMCS_TOP_POS_DEF(int8, top_max_pos, >=)
IMCS_TOP_POS_DEF(int16, top_max_pos, >=) 
IMCS_TOP_POS_DEF(int32, top_max_pos, >=)
IMCS_TOP_POS_DEF(int64, top_max_pos, >=) 
IMCS_TOP_POS_DEF(float, top_max_pos, >=)
IMCS_TOP_POS_DEF(double, top_max_pos, >=) 


IMCS_TOP_POS_DEF(int8, top_min_pos, <=)
IMCS_TOP_POS_DEF(int16, top_min_pos, <=) 
IMCS_TOP_POS_DEF(int32, top_min_pos, <=)
IMCS_TOP_POS_DEF(int64, top_min_pos, <=) 
IMCS_TOP_POS_DEF(float, top_min_pos, <=)
IMCS_TOP_POS_DEF(double, top_min_pos, <=) 


#define SWAP(x, y) ((void)(temp = *(x), *(x) = *(y), *(y) = temp))

typedef struct { 
    imcs_order_t order;
} imcs_sort_context_t;

#define IMCS_SORT_DEF(TYPE)                                             \
    static void insertion_sort_##TYPE(TYPE const* data, imcs_pos_t* permutation, size_t n_elems) \
    {                                                                   \
        imcs_pos_t temp,                                                \
            *last,                                                      \
            *first,                                                     \
            *middle;                                                    \
        if (n_elems > 1) {                                              \
            first = middle = 1 + permutation;                           \
            last = n_elems - 1 + permutation;                           \
            while (first != last) {                                     \
                ++first;                                                \
                if (data[*middle] > data[*first]) {                     \
                    middle = first;                                     \
                }                                                       \
            }                                                           \
            if (data[*permutation] > data[*middle]) {                   \
                SWAP(permutation, middle);                              \
            }                                                           \
            ++permutation;                                              \
            while (permutation != last) {                               \
                first = permutation++;                                  \
                if (data[*first] > data[*permutation]) {                \
                    middle = permutation;                               \
                    temp = *middle;                                     \
                    do {                                                \
                        *middle-- = *first--;                           \
                    } while (data[*first] > data[temp]);                \
                    *middle = temp;                                     \
                }                                                       \
            }                                                           \
        }                                                               \
    }                                                                   \
                                                                        \
    static bool sorted_##TYPE(TYPE const* data, size_t n_elems)         \
    {                                                                   \
        for (--n_elems; n_elems; --n_elems) {                           \
            if (data[0] > data[1]) {                                    \
                return false;                                           \
            }                                                           \
            data += 1;                                                  \
        }                                                               \
        return true;                                                    \
    }                                                                   \
                                                                        \
    static bool rev_sorted_##TYPE(TYPE const* data, size_t n_elems)     \
    {                                                                   \
        for (--n_elems; n_elems; --n_elems) {                           \
            if (data[1] > data[0]) {                                    \
                return false;                                           \
            }                                                           \
            data += 1;                                                  \
        }                                                               \
        return true;                                                    \
    }                                                                   \
                                                                        \
    static void median_estimate_##TYPE(TYPE const* data, imcs_pos_t* permutation, size_t n) \
    {                                                                   \
        imcs_pos_t temp;                                                \
        size_t lu_seed = 123456789LU;                                   \
        const size_t k = ((lu_seed) = 69069 * (lu_seed) + 362437) % --n; \
        SWAP(permutation, permutation + k);                             \
        if (data[permutation[1]] > data[permutation[0]]) {              \
            temp = permutation[1];                                      \
            if (data[permutation[n]] > data[permutation[0]]) {          \
                permutation[1] = permutation[0];                        \
                if (data[temp] > data[permutation[n]]) {                \
                    *permutation = permutation[n];                      \
                    permutation[n] = temp;                              \
                } else {                                                \
                    *permutation = temp;                                \
                }                                                       \
            } else {                                                    \
                permutation[1] = permutation[n];                        \
                permutation[n] = temp;                                  \
            }                                                           \
        } else {                                                        \
            if (data[permutation[0]] > data[permutation[n]]) {          \
                if (data[permutation[1]] > data[permutation[n]]) {      \
                    temp = permutation[1];                              \
                    permutation[1] = permutation[n];                    \
                    permutation[n] = *permutation;                      \
                    *permutation = temp;                                \
                } else {                                                \
                    SWAP(permutation, permutation + n);                 \
                }                                                       \
            }                                                           \
        }                                                               \
    }                                                                   \
                                                                        \
    static void heap_sort_##TYPE(TYPE const* data, imcs_pos_t* permutation, size_t n_elems) \
    {                                                                   \
        size_t i, child, parent;                                        \
        imcs_pos_t temp;                                                \
        if (n_elems > 1) {                                              \
            i = --n_elems / 2;                                          \
            do {                                                        \
                {                                                       \
                    parent = i;                                         \
                    temp = permutation[parent];                         \
                    child = parent * 2;                                 \
                    while (n_elems > child) {                           \
                        if (data[permutation[child + 1]] > data[permutation[child]]) { \
                            child += 1;                                 \
                        }                                               \
                        if (data[permutation[child]] > data[temp]) { \
                            permutation[parent] = permutation[child];   \
                            parent = child;                             \
                            child *= 2;                                 \
                        } else {                                        \
                            child -= 1;                                 \
                            break;                                      \
                        }                                               \
                    }                                                   \
                    if (n_elems == child && data[permutation[child]] > data[temp]) { \
                        permutation[parent] = permutation[child];       \
                        parent = child;                                 \
                    }                                                   \
                    permutation[parent] = temp;                         \
                }                                                       \
            } while (i--);                                              \
            SWAP(permutation, permutation + n_elems);                   \
            for (--n_elems; n_elems; --n_elems) {                       \
                parent = 0;                                             \
                temp = permutation[parent];                             \
                child = parent * 2;                                     \
                while (n_elems > child) {                               \
                    if (data[permutation[child + 1]] > data[permutation[child]]) { \
                        child += 1;                                     \
                    }                                                   \
                    if (data[permutation[child]] > data[temp]) {        \
                        permutation[parent] = permutation[child];       \
                        parent = child;                                 \
                        child *= 2;                                     \
                    } else {                                            \
                        child -= 1;                                     \
                        break;                                          \
                    }                                                   \
                }                                                       \
                if (n_elems == child && data[permutation[child]] > data[temp]) { \
                    permutation[parent] = permutation[child];           \
                    parent = child;                                     \
                }                                                       \
                permutation[parent] = temp;                             \
                SWAP(permutation, permutation + n_elems);               \
            }                                                           \
        }                                                               \
    }                                                                   \
                                                                        \
    static void qloop_##TYPE(TYPE const* data, imcs_pos_t* permutation, size_t n_elems, size_t d) \
    {                                                                   \
        imcs_pos_t temp, *first, *last;                                 \
        while (n_elems > 50) {                                          \
            if (sorted_##TYPE(data, n_elems)) {                         \
                return;                                                 \
            }                                                           \
            if (!d--) {                                                 \
                heap_sort_##TYPE(data, permutation, n_elems);           \
                return;                                                 \
            }                                                           \
            median_estimate_##TYPE(data, permutation, n_elems);         \
            first = 1 + permutation;                                    \
            last = n_elems - 1 + permutation;                           \
            do {                                                        \
                ++first;                                                \
            } while (data[*permutation] > data[*first]);                \
            do {                                                        \
                --last;                                                 \
            } while (data[*last] > data[*permutation]);                 \
            while (last > first) {                                      \
                SWAP(last, first);                                      \
                do {                                                    \
                    ++first;                                            \
                } while (data[*permutation] > data[*first]);            \
                do {                                                    \
                    --last;                                             \
                } while (data[*last] > data[*permutation]);             \
            }                                                           \
            SWAP(permutation, last);                                    \
            qloop_##TYPE(data, last + 1, n_elems - 1 + permutation - last, d); \
            n_elems = last - permutation;                               \
        }                                                               \
        insertion_sort_##TYPE(data, permutation, n_elems);              \
    }                                                                   \
                                                                        \
    static void init_permutation_##TYPE(imcs_pos_t* permutation, size_t n_elems, imcs_order_t order) \
    {                                                                   \
        size_t i;                                                       \
        if (order == IMCS_DESC_ORDER) {                                 \
            for (i = 0; i < n_elems; i++) {                             \
                permutation[i] = n_elems - i - 1;                       \
            }                                                           \
        } else {                                                        \
            for (i = 0; i < n_elems; i++) {                             \
                permutation[i] = i;                                     \
            }                                                           \
        }                                                               \
    }                                                                   \
    static void imcs_sort_array_##TYPE(TYPE const* data, imcs_pos_t* permutation, size_t n_elems, imcs_order_t order) \
    {                                                                   \
        size_t i, j, d, n;                                              \
        if (n_elems > 1 && !sorted_##TYPE(data, n_elems)) {             \
            if (!rev_sorted_##TYPE(data, n_elems)) {                    \
                n = n_elems / 4;                                        \
                d = 2;                                                  \
                while (n) {                                             \
                    ++d;                                                \
                    n /= 2;                                             \
                }                                                       \
                init_permutation_##TYPE(permutation, n_elems, IMCS_ASC_ORDER); \
                qloop_##TYPE(data, permutation, n_elems, 2 * d);        \
                if (order == IMCS_DESC_ORDER) {                         \
                    for (i = 0, j = n_elems-1; i < j; i++, j--) {       \
                        imcs_pos_t tmp = permutation[i];                \
                        permutation[i] = permutation[j];                \
                        permutation[j] = tmp;                           \
                    }                                                   \
                }                                                       \
            } else {                                                    \
                init_permutation_##TYPE(permutation, n_elems, (imcs_order_t)(IMCS_DESC_ORDER - order)); \
            }                                                           \
        } else {                                                        \
            init_permutation_##TYPE(permutation, n_elems, order);       \
        }                                                               \
    }                                                                   \
    static bool imcs_sort_pos_##TYPE##_next(imcs_iterator_h iterator)   \
    {                                                                   \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)iterator->context; \
        size_t count = (size_t)imcs_count(iterator->opd[0]);            \
        TYPE* arr = (TYPE*)palloc(count*sizeof(TYPE));                  \
        imcs_pos_t* permutation = (imcs_pos_t*)imcs_alloc(count*sizeof(imcs_pos_t)); \
        imcs_to_array(iterator->opd[0], arr, count);                    \
        imcs_sort_array_##TYPE(arr, permutation, count, ctx->order);    \
        pfree(arr);                                                     \
        imcs_from_array(iterator, permutation, count);                  \
        return iterator->next(iterator);                                \
    }                                                                   \
    imcs_iterator_h imcs_sort_pos_##TYPE(imcs_iterator_h input, imcs_order_t order) \
    {                                                                   \
        imcs_iterator_h result = imcs_new_iterator(sizeof(imcs_pos_t), sizeof(imcs_array_context_t)); \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)result->context; \
        IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                  \
        result->elem_type = TID_int64;                                  \
        result->opd[0] = imcs_operand(input);                           \
        result->next = imcs_sort_pos_##TYPE##_next;                     \
        result->flags = FLAG_RANDOM_ACCESS;                             \
        ctx->order = order;                                             \
        return result;                                                  \
    }                                                                   \
    static bool imcs_sort_##TYPE##_next(imcs_iterator_h iterator)       \
    {                                                                   \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)iterator->context; \
        size_t i, count = (size_t)imcs_count(iterator->opd[0]);         \
        TYPE* src = (TYPE*)palloc(count*sizeof(TYPE));                  \
        imcs_pos_t* permutation = (imcs_pos_t*)imcs_alloc(count*sizeof(imcs_pos_t)); \
        TYPE* dst = (TYPE*)permutation; /* sizeof(imcs_pos_t) = 8 >= sizeof(TYPE) */ \
        imcs_to_array(iterator->opd[0], src, count);                    \
        imcs_sort_array_##TYPE(src, permutation, count, ctx->order);    \
        for (i = 0; i < count; i++) {                                   \
            dst[i] = src[permutation[i]];                               \
        }                                                               \
        pfree(src);                                                     \
        imcs_from_array(iterator, dst, count);                          \
        return iterator->next(iterator);                                \
    }                                                                   \
    imcs_iterator_h imcs_sort_##TYPE(imcs_iterator_h input, imcs_order_t order) \
    {                                                                   \
        imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_array_context_t)); \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)result->context; \
        IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                  \
        result->elem_type = input->elem_type;                           \
        result->opd[0] = imcs_operand(input);                           \
        result->next = imcs_sort_##TYPE##_next;                         \
        result->flags = FLAG_RANDOM_ACCESS;                             \
        ctx->order = order;                                             \
        return result;                                                  \
    }                                                                   \
    static bool imcs_rank_##TYPE##_next(imcs_iterator_h iterator)       \
    {                                                                   \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)iterator->context; \
        size_t i, j, count = (size_t)imcs_count(iterator->opd[0]);      \
        TYPE* arr = (TYPE*)palloc(count*sizeof(TYPE));                  \
        imcs_pos_t* permutation = (imcs_pos_t*)palloc(count*sizeof(imcs_pos_t)); \
        imcs_pos_t* rank = (imcs_pos_t*)imcs_alloc(count*sizeof(imcs_pos_t)); \
        imcs_to_array(iterator->opd[0], arr, count);                    \
        imcs_sort_array_##TYPE(arr, permutation, count, ctx->order);    \
        for (i = 0; i < count; i = j) {                                 \
            TYPE val = arr[permutation[i]];                             \
            rank[permutation[i]] = i + 1;                               \
            for (j = i+1; j < count && arr[permutation[j]] == val; j++) { \
                rank[permutation[j]] = i + 1;                           \
            }                                                           \
        }                                                               \
        pfree(arr);                                                     \
        pfree(permutation);                                             \
        imcs_from_array(iterator, rank, count);                         \
        return iterator->next(iterator);                                \
    }                                                                   \
    imcs_iterator_h imcs_rank_##TYPE(imcs_iterator_h input, imcs_order_t order) \
    {                                                                   \
        imcs_iterator_h result = imcs_new_iterator(sizeof(imcs_pos_t), sizeof(imcs_array_context_t)); \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)result->context; \
        IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                  \
        result->elem_type = TID_int64;                                  \
        result->opd[0] = imcs_operand(input);                           \
        result->next = imcs_rank_##TYPE##_next;                         \
        result->flags = FLAG_RANDOM_ACCESS;                             \
        ctx->order = order;                                             \
        return result;                                                  \
    }                                                                   \
    static bool imcs_dense_rank_##TYPE##_next(imcs_iterator_h iterator) \
    {                                                                   \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)iterator->context; \
        size_t i, j, count = (size_t)imcs_count(iterator->opd[0]);      \
        TYPE* arr = (TYPE*)palloc(count*sizeof(TYPE));                  \
        imcs_pos_t* permutation = (imcs_pos_t*)palloc(count*sizeof(imcs_pos_t)); \
        imcs_pos_t* rank = (imcs_pos_t*)imcs_alloc(count*sizeof(imcs_pos_t)); \
        size_t n_duplicates = 0;                                        \
        imcs_to_array(iterator->opd[0], arr, count);                    \
        imcs_sort_array_##TYPE(arr, permutation, count, ctx->order);    \
        for (i = 0; i < count; i = j) {                                 \
            TYPE val = arr[permutation[i]];                             \
            rank[permutation[i]] = i + 1 - n_duplicates;                \
            for (j = i+1; j < count && arr[permutation[j]] == val; j++) { \
                rank[permutation[j]] = i + 1 - n_duplicates;            \
            }                                                           \
            n_duplicates += j-i-1;                                      \
        }                                                               \
        pfree(arr);                                                     \
        pfree(permutation);                                             \
        imcs_from_array(iterator, rank, count);                         \
        return iterator->next(iterator);                                \
    }                                                                   \
    imcs_iterator_h imcs_dense_rank_##TYPE(imcs_iterator_h input, imcs_order_t order) \
    {                                                                   \
        imcs_iterator_h result = imcs_new_iterator(sizeof(imcs_pos_t), sizeof(imcs_array_context_t)); \
        imcs_sort_context_t* ctx = (imcs_sort_context_t*)result->context; \
        IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                  \
        result->elem_type = TID_int64;                                  \
        result->opd[0] = imcs_operand(input);                           \
        result->next = imcs_dense_rank_##TYPE##_next;                   \
        result->flags = FLAG_RANDOM_ACCESS;                             \
        ctx->order = order;                                             \
        return result;                                                  \
    }                                                                   \
    static bool imcs_quantile_##TYPE##_next(imcs_iterator_h iterator)   \
    {                                                                   \
        size_t i, count;                                                \
        size_t q_num = iterator->last_pos-1;                            \
        TYPE* arr;                                                      \
        imcs_pos_t* permutation;                                        \
        TYPE* quantiles;                                                \
        count = (size_t)imcs_count(iterator->opd[0]);                   \
        if (count == 0) {                                               \
            return false;                                               \
        }                                                               \
        arr = (TYPE*)palloc(count*sizeof(TYPE));                        \
        permutation = (imcs_pos_t*)palloc(count*sizeof(imcs_pos_t));    \
        quantiles = (TYPE*)imcs_alloc((q_num+1)*sizeof(TYPE));          \
        imcs_to_array(iterator->opd[0], arr, count);                    \
        imcs_sort_array_##TYPE(arr, permutation, count, IMCS_ASC_ORDER); \
        for (i = 0; i < q_num; i++) {                                   \
            quantiles[i] = arr[permutation[count*i/q_num]];             \
        }                                                               \
        quantiles[q_num] = arr[permutation[count-1]];                   \
        pfree(arr);                                                     \
        pfree(permutation);                                             \
        imcs_from_array(iterator, quantiles, q_num+1);                  \
        return iterator->next(iterator);                                \
    }                                                                   \
    imcs_iterator_h imcs_quantile_##TYPE(imcs_iterator_h input, size_t q_num) \
    {                                                                   \
        imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_array_context_t)); \
        IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                  \
        if (q_num == 0) {                                               \
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("Number of quantiles should be greater than zero")))); \
        }                                                               \
        result->elem_type = input->elem_type;                           \
        result->opd[0] = imcs_operand(input);                           \
        result->next = imcs_quantile_##TYPE##_next;                     \
        result->flags = FLAG_RANDOM_ACCESS;                             \
        result->last_pos = q_num+1;                                     \
        return result;                                                  \
    }                                                                   \
    static bool imcs_median_##TYPE##_next(imcs_iterator_h iterator)     \
    {                                                                   \
        size_t count = (size_t)imcs_count(iterator->opd[0]);            \
        TYPE* arr;                                                      \
        imcs_pos_t* permutation;                                        \
        if (iterator->next_pos != 0) {                                  \
            return false;                                               \
        }                                                               \
        count = (size_t)imcs_count(iterator->opd[0]);                   \
        if (count == 0) {                                               \
            return false;                                               \
        }                                                               \
        arr = (TYPE*)palloc(count*sizeof(TYPE));                        \
        permutation = (imcs_pos_t*)palloc(count*sizeof(imcs_pos_t));    \
        imcs_to_array(iterator->opd[0], arr, count);                    \
        imcs_sort_array_##TYPE(arr, permutation, count, IMCS_ASC_ORDER); \
        iterator->tile.arr_double[0] = (count & 1) ? arr[count >> 1] : (arr[(count >> 1)-1] + arr[count >> 1])/2; \
        iterator->tile_size = 1;                                        \
        iterator->next_pos = 1;                                         \
        pfree(arr);                                                     \
        pfree(permutation);                                             \
        return true;                                                    \
    }                                                                   \
    imcs_iterator_h imcs_median_##TYPE(imcs_iterator_h input)           \
    {                                                                   \
        imcs_iterator_h result = imcs_new_iterator(sizeof(double), 0);  \
        IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                  \
        result->elem_type = TID_double;                                 \
        result->opd[0] = imcs_operand(input);                           \
        result->next = imcs_median_##TYPE##_next;                       \
        return result;                                                  \
    }                                                                   \
     
IMCS_SORT_DEF(int8)
IMCS_SORT_DEF(int16)
IMCS_SORT_DEF(int32)
IMCS_SORT_DEF(int64)
IMCS_SORT_DEF(float)
IMCS_SORT_DEF(double)

typedef struct { 
    imcs_pos_t permutation[1];
} imcs_map_context_t;


#define IMCS_MAP_DEF(TYPE)                                              \
static bool imcs_map_##TYPE##_next(imcs_iterator_h iterator)            \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_map_context_t* ctx = (imcs_map_context_t*)iterator->context;   \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    imcs_sort_array_int64(iterator->opd[0]->tile.arr_int64, ctx->permutation, tile_size, IMCS_ASC_ORDER); \
    for (i = 0; i < tile_size; i++) {                                   \
        imcs_pos_t seq = iterator->opd[0]->tile.arr_int64[ctx->permutation[i]]; \
        while (true) {                                                  \
            while (iterator->opd[1]->next_pos <= seq) {                 \
                if (!iterator->opd[1]->next(iterator->opd[1])) {        \
                    return false;                                       \
                }                                                       \
            }                                                           \
            if (iterator->opd[1]->next_pos - seq <= iterator->opd[1]->tile_size) { \
                break;                                                  \
            }                                                           \
            iterator->opd[1]->reset(iterator->opd[1]);                  \
        }                                                               \
        iterator->tile.arr_##TYPE[ctx->permutation[i]] = iterator->opd[1]->tile.arr_##TYPE[seq - (iterator->opd[1]->next_pos - iterator->opd[1]->tile_size)]; \
    }                                                                   \
    iterator->next_pos += tile_size;                                    \
    iterator->tile_size = tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_map_##TYPE(imcs_iterator_h input, imcs_iterator_h positions) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), imcs_tile_size*sizeof(imcs_pos_t)); \
    IMCS_CHECK_TYPE(positions->elem_type, TID_int64);                   \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = input->elem_type;                               \
    result->opd[0] = imcs_operand(positions);                           \
    result->opd[1] = imcs_operand(input);                               \
    result->next = imcs_map_##TYPE##_next;                              \
    return result;                                                      \
}

IMCS_MAP_DEF(int8)
IMCS_MAP_DEF(int16)
IMCS_MAP_DEF(int32)
IMCS_MAP_DEF(int64)
IMCS_MAP_DEF(float)
IMCS_MAP_DEF(double)

static bool imcs_map_char_next(imcs_iterator_h iterator)    
{                                                                       
    size_t i, tile_size;                                            
    size_t elem_size = iterator->elem_size;
    imcs_map_context_t* ctx = (imcs_map_context_t*)iterator->context;   
    if (!iterator->opd[0]->next(iterator->opd[0])) {      
        return false;                                                      
    }                                                                   
    tile_size = iterator->opd[0]->tile_size;                              
    imcs_sort_array_int64(iterator->opd[0]->tile.arr_int64, ctx->permutation, tile_size, IMCS_ASC_ORDER); 
    for (i = 0; i < tile_size; i++) {                                   
        imcs_pos_t seq = iterator->opd[0]->tile.arr_int64[ctx->permutation[i]];           
        while (true) { 
           while (iterator ->opd[1]->next_pos <= seq) {                 
                if (!iterator->opd[1]->next(iterator->opd[1])) {                
                    return false;                                              
                }                                                           
            }                                                               
            if (iterator->opd[1]->next_pos - seq <= iterator->opd[1]->tile_size) { 
                break;
            }
            iterator->opd[1]->reset(iterator->opd[1]);
        }                                                               
        memcpy(iterator->tile.arr_char + elem_size*ctx->permutation[i], iterator->opd[1]->tile.arr_char + elem_size*(seq - (iterator->opd[1]->next_pos - iterator->opd[1]->tile_size)), elem_size); 
    }                                                                   
    iterator->next_pos += tile_size;                                 
    iterator->tile_size = tile_size;                                    
    return true;                                                    
}                                                                       
                                                                        
imcs_iterator_h imcs_map_char(imcs_iterator_h input, imcs_iterator_h positions) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(input->elem_size, imcs_tile_size*sizeof(int));     
    IMCS_CHECK_TYPE(positions->elem_type, TID_int64);                   
    IMCS_CHECK_TYPE(input->elem_type, TID_char);                      
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(positions);                                                
    result->opd[1] = imcs_operand(input);                                              
    result->next = imcs_map_char_next;                           
    return result;                                                    
}

#define IMCS_UNIQ_DEF(TYPE)                                             \
static bool imcs_unique_##TYPE##_next(imcs_iterator_h iterator)         \
{                                                                       \
    size_t i, j = 0, tile_size;                                         \
    size_t this_tile_size = imcs_tile_size;                             \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context;   \
    while (true) {                                                      \
        if (ctx->offset >= iterator->opd[0]->tile_size) {               \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                if (j != 0) {                                           \
                    iterator->tile_size = j;                            \
                    return true;                                        \
                }                                                       \
                return false;                                           \
            }                                                           \
            ctx->offset = 0;                                            \
        }                                                               \
        tile_size = iterator->opd[0]->tile_size;                        \
        for (i = ctx->offset; i < tile_size; i++) {                     \
            if (iterator->next_pos == 0 || ctx->accumulator.val_##TYPE != iterator->opd[0]->tile.arr_##TYPE[i]) { \
                iterator->tile.arr_##TYPE[j] = ctx->accumulator.val_##TYPE = iterator->opd[0]->tile.arr_##TYPE[i]; \
                iterator->next_pos += 1;                                \
                if (++j == this_tile_size) {                            \
                    ctx->offset = i + 1;                                \
                    iterator->tile_size = this_tile_size;               \
                    return true;                                        \
                }                                                       \
            }                                                           \
        }                                                               \
        ctx->offset = tile_size;                                        \
    }                                                                   \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_unique_##TYPE(imcs_iterator_h input)               \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_agg_context_t)); \
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context;     \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = input->elem_type;                               \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_unique_##TYPE##_next;                           \
    result->reset = imcs_reset_unary_agg_iterator;                      \
    ctx->offset = ctx->count = 0;                                       \
    return result;                                                      \
}
 
IMCS_UNIQ_DEF(int8)
IMCS_UNIQ_DEF(int16)
IMCS_UNIQ_DEF(int32)
IMCS_UNIQ_DEF(int64)
IMCS_UNIQ_DEF(float)
IMCS_UNIQ_DEF(double)

static bool imcs_unique_char_next(imcs_iterator_h iterator)  
{                                                                       
    size_t i, j = 0, tile_size;                                     
    size_t elem_size = iterator->elem_size;
    size_t this_tile_size = imcs_tile_size;                  
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context; 
    while (true) {                                                         
        if (ctx->offset >= iterator->opd[0]->tile_size) {                 
            if (!iterator->opd[0]->next(iterator->opd[0])) { 
                if (j != 0) {                 
                    iterator->tile_size = j;                            
                    return true;                                      
                }                                                       
                return false;                                              
            }                                                           
            ctx->offset = 0;                                            
        }                                                               
        tile_size = iterator->opd[0]->tile_size;                          
        for (i = ctx->offset; i < tile_size; i++) {                     
            if (iterator->next_pos == 0 || memcmp(ctx->accumulator.val_ptr, iterator->opd[0]->tile.arr_char + elem_size*i, elem_size) != 0) { 
                memcpy(iterator->tile.arr_char + elem_size*j, iterator->opd[0]->tile.arr_char + elem_size*i, elem_size);
                memcpy(ctx->accumulator.val_ptr, iterator->opd[0]->tile.arr_char + elem_size*i, elem_size); 
                iterator->next_pos += 1;                             
                if (++j == this_tile_size) {                         
                    ctx->offset = i + 1;                                
                    iterator->tile_size = this_tile_size;            
                    return true;                                    
                }                                                       
            }                                                           
        }                                                               
        ctx->offset = tile_size;                                        
    }                                                                   
}                                                                       
                                                                        
imcs_iterator_h imcs_unique_char(imcs_iterator_h input) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(input->elem_size, sizeof(imcs_agg_context_t)); 
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context; 
    IMCS_CHECK_TYPE(input->elem_type, TID_char);                
    result->elem_type = TID_char;                       
    result->opd[0] = imcs_operand(input);                                               
    result->next = imcs_unique_char_next;                          
    result->reset = imcs_reset_unary_agg_iterator;                   
    ctx->offset = ctx->count = 0;                                       
    ctx->accumulator.val_ptr = (char*)imcs_alloc(input->elem_size);
    return result;                                                    
}

typedef struct imcs_union_context_t_ {               
    size_t left_offs;
    size_t right_offs;
    bool left_end;
    bool right_end;
} imcs_union_context_t;                              

static void imcs_reset_union_iterator(imcs_iterator_h iterator)
{
    imcs_union_context_t* ctx = (imcs_union_context_t*)iterator->context;
    ctx->left_offs = 0;
    ctx->right_offs = 0;
    ctx->left_end = false;
    ctx->right_end = false;
    imcs_reset_iterator(iterator);
}


#define IMCS_UNION_DEF(TYPE)                                            \
static bool imcs_union_##TYPE##_next(imcs_iterator_h iterator)          \
{                                                                       \
    size_t i;                                                           \
    imcs_union_context_t* ctx = (imcs_union_context_t*)iterator->context; \
    size_t this_tile_size = imcs_tile_size;                             \
    if (ctx->left_end && ctx->right_end) {                              \
        return false;                                                   \
    }                                                                   \
    for (i = 0; i < this_tile_size; i++) {                              \
        if (!ctx->left_end && ctx->left_offs >= iterator->opd[0]->tile_size) { \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                ctx->left_end = true;                                   \
                if (ctx->right_end) {                                   \
                    if (i != 0) {                                       \
                        break;                                          \
                    }                                                   \
                    return false;                                       \
                }                                                       \
            } else {                                                    \
                Assert(iterator->opd[0]->tile_size > 0);                \
                ctx->left_offs = 0;                                     \
            }                                                           \
        }                                                               \
        if (!ctx->right_end && ctx->right_offs >= iterator->opd[1]->tile_size) { \
            if (!iterator->opd[1]->next(iterator->opd[1])) {            \
                ctx->right_end = true;                                  \
                if (ctx->left_end) {                                    \
                    if (i != 0) {                                       \
                        break;                                          \
                    }                                                   \
                    return false;                                       \
                }                                                       \
            } else {                                                    \
                Assert(iterator->opd[1]->tile_size > 0);                \
                ctx->right_offs = 0;                                    \
            }                                                           \
        }                                                               \
        if (ctx->left_end) {                                            \
            iterator->tile.arr_##TYPE[i] = iterator->opd[1]->tile.arr_##TYPE[ctx->right_offs++]; \
        } else if (ctx->right_end) {                                    \
            iterator->tile.arr_##TYPE[i] = iterator->opd[0]->tile.arr_##TYPE[ctx->left_offs++]; \
        } else {                                                        \
            if (iterator->opd[0]->tile.arr_##TYPE[ctx->left_offs] < iterator->opd[1]->tile.arr_##TYPE[ctx->right_offs]) { \
                iterator->tile.arr_##TYPE[i] = iterator->opd[0]->tile.arr_##TYPE[ctx->left_offs++]; \
            } else {                                                    \
                if (iterator->opd[0]->tile.arr_##TYPE[ctx->left_offs] == iterator->opd[1]->tile.arr_##TYPE[ctx->right_offs]) { \
                    ctx->left_offs += 1;                                \
                }                                                       \
                iterator->tile.arr_##TYPE[i] = iterator->opd[1]->tile.arr_##TYPE[ctx->right_offs++]; \
            }                                                           \
        }                                                               \
    }                                                                   \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_union_##TYPE(imcs_iterator_h left, imcs_iterator_h right) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(TYPE), sizeof(imcs_union_context_t)); \
    imcs_union_context_t* ctx = (imcs_union_context_t*)result->context; \
    IMCS_CHECK_TYPE(left->elem_type, TID_##TYPE);                       \
    IMCS_CHECK_TYPE(right->elem_type, TID_##TYPE);                      \
    result->elem_type = left->elem_type;                                \
    result->opd[0] = imcs_operand(left);                                \
    result->opd[1] = imcs_operand(right);                               \
    result->next = imcs_union_##TYPE##_next;                            \
    result->reset = imcs_reset_union_iterator;                          \
    ctx->left_offs = ctx->right_offs = 0;                               \
    ctx->left_end = ctx->right_end = false;                             \
    return result;                                                      \
}
 
IMCS_UNION_DEF(int8)
IMCS_UNION_DEF(int16)
IMCS_UNION_DEF(int32)
IMCS_UNION_DEF(int64)
IMCS_UNION_DEF(float)
IMCS_UNION_DEF(double)

static bool imcs_limit_next(imcs_iterator_h iterator)  
{                                                                       
   size_t j = 0, available;                                     
   imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context; 
   size_t elem_size = iterator->elem_size; 
   size_t this_tile_size = imcs_tile_size;  
   if ((int64)iterator->first_pos < 0 || (int64)iterator->last_pos < 0) { 
       imcs_count_t count = imcs_count(iterator->opd[0]);    
       iterator->opd[0]->reset(iterator->opd[0]);
       if ((int64)iterator->last_pos < 0) { 
           if (count < -iterator->last_pos) { 
               return false;
           }
           iterator->last_pos = count + iterator->last_pos;
       }
       if ((int64)iterator->first_pos < 0) { 
           iterator->first_pos = (count < -iterator->first_pos) ? 0 : count + iterator->first_pos;
       }
   }
   if (iterator->next_pos < iterator->first_pos) {               
       do {                                                                
           if (!iterator->opd[0]->next(iterator->opd[0])) { 
               return false;                                              
           }                                                           
           iterator->next_pos += iterator->opd[0]->tile_size;         
       } while (iterator->next_pos <= iterator->first_pos);      
       ctx->offset = (size_t)(iterator->first_pos - iterator->next_pos + iterator->opd[0]->tile_size);  
       iterator->next_pos = iterator->first_pos;                 
   }                                                                   
   while (iterator->next_pos <= iterator->last_pos && j < this_tile_size) {  
       if (ctx->offset >= iterator->opd[0]->tile_size) {                 
           if (!iterator->opd[0]->next(iterator->opd[0])) {
               if (j != 0) {                   
                   iterator->tile_size = j;                            
                   return true;                                      
               }                                                       
               return false;                                              
           }                                                           
           ctx->offset = 0;                                            
       }                                                               
       available = iterator->opd[0]->tile_size - ctx->offset;                          
       if (available > iterator->last_pos - iterator->next_pos + 1)  {
           available = (size_t)(iterator->last_pos - iterator->next_pos + 1);
       }
       if (available > this_tile_size - j) { 
           available = this_tile_size - j;
       }
       memcpy(&iterator->tile.arr_char[j*elem_size], &iterator->opd[0]->tile.arr_char[ctx->offset*elem_size], available*elem_size);
       ctx->offset += available;
       iterator->next_pos += available;
       j += available;
   }
   if (j != 0) { 
       iterator->tile_size = j;                            
       return true;                                      
   } 
   return false;
}                                                                       
                                                                        
imcs_iterator_h imcs_limit(imcs_iterator_h input, imcs_pos_t from, imcs_pos_t till) 
{                          
    if (input->flags & FLAG_RANDOM_ACCESS) { 
        imcs_subseq_random_access_iterator(input, from, till);
        return input;
    } else {
        imcs_iterator_h result = imcs_new_iterator(input->elem_size, sizeof(imcs_agg_context_t)); 
        imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context; 
        result->elem_type = input->elem_type;                       
        result->opd[0] = imcs_operand(input);                                               
        result->next_pos = 0;                                            
        result->first_pos = from;                                        
        result->last_pos = till;                                         
        result->next = imcs_limit_next;                                  
        result->reset = imcs_reset_unary_agg_iterator;                   
        ctx->offset = ctx->count = 0;                                       
        return result;                                           
    }         
}



static bool imcs_tee_next(imcs_iterator_h iterator) 
{                     
    size_t elem_size = iterator->elem_size; 
    imcs_iterator_h input;
    if (iterator->opd[1]->next_pos > iterator->next_pos) {        
        input = iterator->opd[1];
        Assert(iterator->tile_size == 0 || input->next_pos == iterator->next_pos + input->tile_size);
    } else {     
        if (!iterator->opd[0]->next(iterator->opd[0])) { 
            return false;                                                      
        }                                                                   
        input = iterator->opd[0];
    }
    memcpy(iterator->tile.arr_char, input->tile.arr_char, input->tile_size*elem_size);
    iterator->tile_size = input->tile_size;
    iterator->next_pos = input->next_pos;
    return true;
}

void imcs_tee(imcs_iterator_h out_iterator[2], imcs_iterator_h input)
{
    out_iterator[0] = imcs_new_iterator(input->elem_size, 0);
    out_iterator[1] = imcs_new_iterator(input->elem_size, 0);

    out_iterator[0]->elem_type = input->elem_type;
    out_iterator[0]->opd[0] = imcs_operand(input);                                                
    out_iterator[0]->opd[1] = out_iterator[1];                                              
    out_iterator[0]->next = imcs_tee_next;                      
                     
    out_iterator[1]->elem_type = input->elem_type;
    out_iterator[1]->opd[0] = out_iterator[0]->opd[0];
    out_iterator[1]->opd[1] = out_iterator[0];                                              
    out_iterator[1]->next = imcs_tee_next;                      
}

static void imcs_histogram_merge(imcs_iterator_h dst, imcs_iterator_h src)      
{                                                                       
     size_t i, tile_size = dst->tile_size;                              
     for (i = 0; i < tile_size; i++) {                                  
         dst->tile.arr_int64[i] += src->tile.arr_int64[i];
     }
}


#define IMCS_HISTOGRAM_DEF(TYPE)                                        \
 typedef struct imcs_histogram_##TYPE##_context_t_ {                    \
    TYPE min_value;                                                     \
    TYPE max_value;                                                     \
    size_t n_intervals;                                                 \
} imcs_histogram_##TYPE##_context_t;                                    \
static bool imcs_histogram_##TYPE##_next(imcs_iterator_h iterator)      \
{                                                                       \
    imcs_histogram_##TYPE##_context_t* ctx = (imcs_histogram_##TYPE##_context_t*)iterator->context; \
    TYPE min_value = ctx->min_value;                                    \
    TYPE max_value = ctx->max_value;                                    \
    size_t n_intervals = ctx->n_intervals;                              \
    if (iterator->flags & FLAG_PREPARED) {                              \
        return iterator->tile_size != 0;                                \
    }                                                                   \
    if (iterator->next_pos != 0) {                                      \
        return false;                                                   \
    }                                                                   \
    memset(iterator->tile.arr_int64, 0, n_intervals*sizeof(int64));     \
    while (iterator->opd[0]->next(iterator->opd[0])) {                  \
        size_t i, tile_size = iterator->opd[0]->tile_size;              \
        for (i = 0; i < tile_size; i++) {                               \
            TYPE val = iterator->opd[0]->tile.arr_##TYPE[i];            \
            if (val >= min_value && val < max_value) {                  \
                iterator->tile.arr_int64[(size_t)((val - min_value) * n_intervals / (max_value - min_value))] += 1; \
            }                                                           \
        }                                                               \
    }                                                                   \
    iterator->tile_size = n_intervals;                                  \
    iterator->next_pos = n_intervals;                                   \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_histogram_##TYPE(imcs_iterator_h input, TYPE min_value, TYPE max_value, size_t n_intervals) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_histogram_##TYPE##_context_t));  \
    imcs_histogram_##TYPE##_context_t* ctx = (imcs_histogram_##TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    if (n_intervals-1 >= (size_t)imcs_tile_size) {                      \
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("number of histogram intervals should not be larger than tile size")))); \
    }                                                                   \
    if (max_value <= min_value) {                                       \
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("max_value shouldbegreater or equal thanmin_value")))); \
    }                                                                   \
    result->elem_type = TID_int64;                                      \
    result->opd[0] = imcs_operand(input);                               \
    result->last_pos = n_intervals-1;                                   \
    result->next = imcs_histogram_##TYPE##_next;                        \
    result->prepare = imcs_histogram_##TYPE##_next;                     \
    result->merge = imcs_histogram_merge;                               \
    ctx->min_value = min_value;                                         \
    ctx->max_value = max_value;                                         \
    ctx->n_intervals = n_intervals;                                     \
    return result;                                                      \
}

IMCS_HISTOGRAM_DEF(int8)
IMCS_HISTOGRAM_DEF(int16)
IMCS_HISTOGRAM_DEF(int32)
IMCS_HISTOGRAM_DEF(int64)
IMCS_HISTOGRAM_DEF(float)
IMCS_HISTOGRAM_DEF(double)

typedef struct imcs_cross_context_t_ {
    int first_sign;
    int prev_sign;
    int n_zeros;
    bool eos;
    size_t offset;
} imcs_cross_context_t;

static void imcs_cross_reset_iterator(imcs_iterator_h iterator)
{
    imcs_cross_context_t* ctx = (imcs_cross_context_t*)iterator->context; 
    ctx->offset = 0;  
    ctx->eos = false;
    ctx->prev_sign = 0;
    ctx->n_zeros = 0;
    imcs_reset_iterator(iterator);
}

#define IMCS_CROSS_DEF(TYPE)                                            \
static bool imcs_cross_##TYPE##_next(imcs_iterator_h iterator)          \
{                                                                       \
    imcs_cross_context_t* ctx = (imcs_cross_context_t*)iterator->context; \
    size_t i = 0, j = ctx->offset, tile_size = iterator->opd[0]->tile_size; \
    size_t this_tile_size = imcs_tile_size;                             \
    int prev_sign = ctx->prev_sign;                                     \
    if (ctx->eos) {                                                     \
        return false;                                                   \
    }                                                                   \
    do {                                                      \
        if (j == tile_size) {                                           \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                if (i != 0) {                                           \
                    ctx->eos = true;                                    \
                    break;                                              \
                }                                                       \
                return false;                                           \
            }                                                           \
            tile_size = iterator->opd[0]->tile_size;                    \
            j = 0;                                                      \
        }                                                               \
        while (j < tile_size && i < this_tile_size) {                   \
            TYPE val = iterator->opd[0]->tile.arr_##TYPE[j++];          \
            int curr_sign = val < 0 ? -1 : val > 0 ? 1 : 0;             \
            if (curr_sign != prev_sign) {                               \
                if ((prev_sign&curr_sign) != 0 && (ctx->first_sign == 0 || iterator->next_pos != 0 || curr_sign == ctx->first_sign)) { \
                    iterator->tile.arr_int64[i++] = iterator->opd[0]->next_pos - tile_size + j - 1 - ctx->n_zeros; \
                }                                                       \
                if (curr_sign != 0) {                                   \
                    prev_sign = curr_sign;                              \
                    ctx->n_zeros = 0;                                   \
                } else {                                                \
                    ctx->n_zeros += 1;                                  \
                }                                                       \
            }                                                           \
        }                                                               \
    } while (i < this_tile_size);                                       \
    ctx->prev_sign = prev_sign;                                         \
    ctx->offset = j;                                                    \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_cross_##TYPE(imcs_iterator_h input, int first_cross_direction) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_cross_context_t)); \
    imcs_cross_context_t* ctx = (imcs_cross_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_int64;                                      \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_cross_##TYPE##_next;                            \
    result->reset = imcs_cross_reset_iterator;                          \
    ctx->offset = 0;                                                    \
    ctx->eos = false;                                                   \
    ctx->prev_sign = 0;                                                 \
    ctx->n_zeros = 0;                                                   \
    ctx->first_sign = first_cross_direction;                            \
    return result;                                                      \
}

IMCS_CROSS_DEF(int8)
IMCS_CROSS_DEF(int16)
IMCS_CROSS_DEF(int32)
IMCS_CROSS_DEF(int64)
IMCS_CROSS_DEF(float)
IMCS_CROSS_DEF(double)

#define IMCS_EXTREMA_DEF(TYPE)                                          \
typedef struct imcs_extrema_##TYPE##_context_t_ {                       \
    int first_extremum;                                                 \
    int prev_trend;                                                     \
    bool eos;                                                           \
    size_t offset;                                                      \
    TYPE prev_val;                                                      \
} imcs_extrema_##TYPE##_context_t;                                      \
                                                                        \
static void imcs_extrema_##TYPE##_reset_iterator(imcs_iterator_h iterator) \
{                                                                       \
    imcs_extrema_##TYPE##_context_t* ctx = (imcs_extrema_##TYPE##_context_t*)iterator->context; \
    ctx->offset = 0;                                                    \
    ctx->eos = false;                                                   \
    ctx->prev_trend = 0;                                                \
    imcs_reset_iterator(iterator);                                      \
}                                                                       \
                                                                        \
static bool imcs_extrema_##TYPE##_next(imcs_iterator_h iterator)        \
{                                                                       \
    imcs_extrema_##TYPE##_context_t* ctx = (imcs_extrema_##TYPE##_context_t*)iterator->context; \
    size_t i = 0, j = ctx->offset, tile_size = iterator->opd[0]->tile_size; \
    size_t this_tile_size = imcs_tile_size;                             \
    int prev_trend = ctx->prev_trend;                                   \
    TYPE prev_val = ctx->prev_val;                                      \
    bool bos = iterator->next_pos == 0;                                 \
    if (ctx->eos) {                                                     \
        return false;                                                   \
    }                                                                   \
    do {                                                                \
        if (j == tile_size) {                                           \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                if (i != 0) {                                           \
                    ctx->eos = true;                                    \
                    break;                                              \
                }                                                       \
                return false;                                           \
            }                                                           \
            tile_size = iterator->opd[0]->tile_size;                    \
            j = 0;                                                      \
        }                                                               \
        while (j < tile_size && i < this_tile_size) {                   \
            TYPE new_val = iterator->opd[0]->tile.arr_##TYPE[j++];      \
            int new_trend = bos ? 0 : new_val < prev_val ? -1 : new_val > prev_val ? 1 : 0; \
            if (new_trend != prev_trend) {                              \
                if ((prev_trend&new_trend) != 0 && (ctx->first_extremum == 0 || prev_trend == ctx->first_extremum)) { \
                    iterator->tile.arr_int64[i++] = iterator->opd[0]->next_pos - tile_size + j - 2; \
                }                                                       \
                if (new_trend != 0) prev_trend = new_trend;             \
            }                                                           \
            bos = false;                                                \
            prev_val = new_val;                                         \
        }                                                               \
    } while (i < this_tile_size);                                       \
    ctx->prev_trend = prev_trend;                                       \
    ctx->prev_val = prev_val;                                           \
    ctx->offset = j;                                                    \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_extrema_##TYPE(imcs_iterator_h input, int first_extremum) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_extrema_##TYPE##_context_t)); \
    imcs_extrema_##TYPE##_context_t* ctx = (imcs_extrema_##TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_int64;                                      \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_extrema_##TYPE##_next;                          \
    result->reset = imcs_extrema_##TYPE##_reset_iterator;               \
    ctx->offset = 0;                                                    \
    ctx->eos = false;                                                   \
    ctx->prev_trend = 0;                                                \
    ctx->first_extremum = first_extremum;                               \
    return result;                                                      \
}

IMCS_EXTREMA_DEF(int8)
IMCS_EXTREMA_DEF(int16)
IMCS_EXTREMA_DEF(int32)
IMCS_EXTREMA_DEF(int64)
IMCS_EXTREMA_DEF(float)
IMCS_EXTREMA_DEF(double)



#define IMCS_STRETCH_DEF(TS_TYPE, VAL_TYPE)                             \
typedef struct imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t_ {       \
    VAL_TYPE default_value;                                             \
    VAL_TYPE next_value;                                                \
    TS_TYPE  next_timestamp;                                            \
    bool end_of_seq;                                                    \
    size_t offs;                                                        \
} imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t;                      \
static void imcs_stretch_##TS_TYPE##_##VAL_TYPE##_reset(imcs_iterator_h iterator) \
{                                                                       \
    imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t*)iterator->context; \
    ctx->offs = 0;                                                      \
    ctx->next_timestamp = IMCS_MIN_##TS_TYPE;                           \
    ctx->end_of_seq = false;                                            \
    imcs_reset_iterator(iterator);                                      \
}                                                                       \
static bool imcs_stretch_##TS_TYPE##_##VAL_TYPE##_next(imcs_iterator_h iterator) \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t*)iterator->context; \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        if (!ctx->end_of_seq && iterator->opd[0]->tile.arr_##TS_TYPE[i] >= ctx->next_timestamp) { \
            if (++ctx->offs >= iterator->opd[1]->tile_size) {           \
                if (iterator->opd[1]->next(iterator->opd[1])) {         \
                    Assert(iterator->opd[1]->tile_size > 0);            \
                    ctx->offs = 0;                                      \
                    if (!iterator->opd[2]->next(iterator->opd[2]) || iterator->opd[2]->tile_size != iterator->opd[1]->tile_size) {    \
                        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("values timeseries is too short")))); \
                    }                                                   \
                    ctx->next_timestamp = iterator->opd[1]->tile.arr_##TS_TYPE[0]; \
                    ctx->next_value = iterator->opd[2]->tile.arr_##VAL_TYPE[0]; \
                } else {                                                \
                    ctx->end_of_seq = true;                             \
                    ctx->next_value = ctx->default_value;               \
                }                                                       \
            } else {                                                    \
                ctx->next_timestamp = iterator->opd[1]->tile.arr_##TS_TYPE[ctx->offs]; \
                ctx->next_value = iterator->opd[2]->tile.arr_##VAL_TYPE[ctx->offs]; \
            }                                                           \
        }                                                               \
        iterator->tile.arr_##VAL_TYPE[i] = ctx->next_value;             \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_stretch_##TS_TYPE##_##VAL_TYPE(imcs_iterator_h ts1, imcs_iterator_h ts2, imcs_iterator_h values, VAL_TYPE filler) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(VAL_TYPE), sizeof(imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t)); \
    imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_stretch_##TS_TYPE##_##VAL_TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(ts1->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(ts2->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(values->elem_type, TID_##VAL_TYPE);                 \
    result->elem_type = values->elem_type;                              \
    result->opd[0] = imcs_operand(ts1);                                 \
    result->opd[1] = imcs_operand(ts2);                                 \
    result->opd[2] = imcs_operand(values);                              \
    ctx->offs = 0;                                                      \
    ctx->default_value = filler;                                        \
    ctx->next_timestamp = IMCS_MIN_##TS_TYPE;                           \
    ctx->end_of_seq = false;                                            \
    result->next = imcs_stretch_##TS_TYPE##_##VAL_TYPE##_next;          \
    result->reset = imcs_stretch_##TS_TYPE##_##VAL_TYPE##_reset;        \
    return result;                                                      \
}

IMCS_STRETCH_DEF(int32, int8)
IMCS_STRETCH_DEF(int32, int16)
IMCS_STRETCH_DEF(int32, int32)
IMCS_STRETCH_DEF(int32, int64)
IMCS_STRETCH_DEF(int32, float)
IMCS_STRETCH_DEF(int32, double)

IMCS_STRETCH_DEF(int64, int8)
IMCS_STRETCH_DEF(int64, int16)
IMCS_STRETCH_DEF(int64, int32)
IMCS_STRETCH_DEF(int64, int64)
IMCS_STRETCH_DEF(int64, float)
IMCS_STRETCH_DEF(int64, double)

#define IMCS_STRETCH0_DEF(TS_TYPE, VAL_TYPE)                            \
typedef struct imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t_ {      \
    VAL_TYPE default_value;                                             \
    bool left_end;                                                      \
    bool right_end;                                                     \
    size_t left_offs;                                                   \
    size_t right_offs;                                                  \
} imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t;                     \
static void imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_reset(imcs_iterator_h iterator) \
{                                                                       \
    imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t*)iterator->context; \
    ctx->left_offs = ctx->right_offs = 0;                               \
    ctx->left_end = ctx->right_end = false;                             \
    imcs_reset_iterator(iterator);                                      \
}                                                                       \
static bool imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_next(imcs_iterator_h iterator) \
{                                                                       \
    size_t i;                                                           \
    size_t this_tile_size = imcs_tile_size;                             \
    imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t*)iterator->context; \
    if (ctx->left_end && ctx->right_end) {                              \
        return false;                                                   \
    }                                                                   \
    for (i = 0; i < this_tile_size; i++) {                              \
        if (!ctx->left_end && ctx->left_offs >= iterator->opd[0]->tile_size) { \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                ctx->left_end = true;                                   \
                if (ctx->right_end) {                                   \
                    if (i != 0) {                                       \
                        break;                                          \
                    }                                                   \
                    return false;                                       \
                }                                                       \
            } else {                                                    \
                Assert(iterator->opd[0]->tile_size > 0);                \
                ctx->left_offs = 0;                                     \
            }                                                           \
        }                                                               \
        if (!ctx->right_end && ctx->right_offs >= iterator->opd[1]->tile_size) { \
            if (!iterator->opd[1]->next(iterator->opd[1])) {            \
                ctx->right_end = true;                                  \
                if (ctx->left_end) {                                    \
                    if (i != 0) {                                       \
                        break;                                          \
                    }                                                   \
                    return false;                                       \
                }                                                       \
            } else {                                                    \
                Assert(iterator->opd[1]->tile_size > 0);                \
                if (!iterator->opd[2]->next(iterator->opd[2]) || iterator->opd[2]->tile_size != iterator->opd[1]->tile_size) {        \
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("values timeseries is too short")))); \
                }                                                       \
                ctx->right_offs = 0;                                    \
            }                                                           \
        }                                                               \
        if (ctx->left_end) {                                            \
            iterator->tile.arr_##VAL_TYPE[i] = iterator->opd[2]->tile.arr_##VAL_TYPE[ctx->right_offs++]; \
        } else if (ctx->right_end || iterator->opd[0]->tile.arr_##TS_TYPE[ctx->left_offs] < iterator->opd[1]->tile.arr_##TS_TYPE[ctx->right_offs]) { \
            iterator->tile.arr_##VAL_TYPE[i] = ctx->default_value;      \
            ctx->left_offs += 1;                                        \
        } else {                                                        \
            if (iterator->opd[0]->tile.arr_##TS_TYPE[ctx->left_offs] == iterator->opd[1]->tile.arr_##TS_TYPE[ctx->right_offs]) { \
                ctx->left_offs += 1;                                    \
            }                                                           \
            iterator->tile.arr_##VAL_TYPE[i] = iterator->opd[2]->tile.arr_##VAL_TYPE[ctx->right_offs++]; \
        }                                                               \
    }                                                                   \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_stretch0_##TS_TYPE##_##VAL_TYPE(imcs_iterator_h ts1, imcs_iterator_h ts2, imcs_iterator_h values, VAL_TYPE filler) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(VAL_TYPE), sizeof(imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t)); \
    imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(ts1->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(ts2->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(values->elem_type, TID_##VAL_TYPE);                 \
    result->elem_type = values->elem_type;                              \
    result->opd[0] = imcs_operand(ts1);                                 \
    result->opd[1] = imcs_operand(ts2);                                 \
    result->opd[2] = imcs_operand(values);                              \
    ctx->default_value = filler;                                        \
    ctx->left_offs = ctx->right_offs = 0;                               \
    ctx->left_end = ctx->right_end = false;                             \
    result->next = imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_next;         \
    result->reset = imcs_stretch0_##TS_TYPE##_##VAL_TYPE##_reset;       \
    return result;                                                      \
}

IMCS_STRETCH0_DEF(int32, int8)
IMCS_STRETCH0_DEF(int32, int16)
IMCS_STRETCH0_DEF(int32, int32)
IMCS_STRETCH0_DEF(int32, int64)
IMCS_STRETCH0_DEF(int32, float)
IMCS_STRETCH0_DEF(int32, double)

IMCS_STRETCH0_DEF(int64, int8)
IMCS_STRETCH0_DEF(int64, int16)
IMCS_STRETCH0_DEF(int64, int32)
IMCS_STRETCH0_DEF(int64, int64)
IMCS_STRETCH0_DEF(int64, float)
IMCS_STRETCH0_DEF(int64, double)


#define IMCS_ASOF_JOIN_DEF(TS_TYPE, VAL_TYPE)                           \
typedef struct imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t_ {     \
    VAL_TYPE next_value;                                                \
    VAL_TYPE prev_value;                                                \
    TS_TYPE  next_timestamp;                                            \
    TS_TYPE  prev_timestamp;                                            \
    bool end_of_seq;                                                    \
    size_t offs;                                                        \
} imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t;                    \
static void imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_reset(imcs_iterator_h iterator) \
{                                                                       \
    imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t*)iterator->context; \
    ctx->offs = 0;                                                      \
    ctx->next_timestamp = IMCS_MIN_##TS_TYPE;                           \
    ctx->end_of_seq = false;                                            \
    imcs_reset_iterator(iterator);                                      \
}                                                                       \
static bool imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_next(imcs_iterator_h iterator) \
{                                                                       \
    size_t i, tile_size;                                                \
    bool has_prev_value;                                                \
    imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t*)iterator->context; \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    has_prev_value = iterator->opd[1]->tile_size != 0;                  \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        while (!ctx->end_of_seq && iterator->opd[0]->tile.arr_##TS_TYPE[i] >= ctx->next_timestamp) { \
            ctx->prev_timestamp = ctx->next_timestamp;                  \
            ctx->prev_value = ctx->next_value;                          \
            if (++ctx->offs >= iterator->opd[1]->tile_size) {           \
                if (iterator->opd[1]->next(iterator->opd[1])) {         \
                    Assert(iterator->opd[1]->tile_size > 0);            \
                    ctx->offs = 0;                                      \
                    if (!iterator->opd[2]->next(iterator->opd[2]) || iterator->opd[2]->tile_size != iterator->opd[1]->tile_size) {     \
                        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("values timeseries is too short")))); \
                    }                                                   \
                    ctx->next_timestamp = iterator->opd[1]->tile.arr_##TS_TYPE[0]; \
                    ctx->next_value = iterator->opd[2]->tile.arr_##VAL_TYPE[0]; \
                    has_prev_value = true;                              \
                } else if (has_prev_value)  {                           \
                    ctx->end_of_seq = true;                             \
                } else {                                                \
                    return false;                                       \
                }                                                       \
            } else {                                                    \
                ctx->next_timestamp = iterator->opd[1]->tile.arr_##TS_TYPE[ctx->offs]; \
                ctx->next_value = iterator->opd[2]->tile.arr_##VAL_TYPE[ctx->offs]; \
            }                                                           \
        }                                                               \
        iterator->tile.arr_##VAL_TYPE[i] = ctx->end_of_seq || iterator->opd[0]->tile.arr_##TS_TYPE[i] - ctx->prev_timestamp <= ctx->next_timestamp - iterator->opd[0]->tile.arr_##TS_TYPE[i] ? ctx->prev_value : ctx->next_value; \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_asof_join_##TS_TYPE##_##VAL_TYPE(imcs_iterator_h ts1, imcs_iterator_h ts2, imcs_iterator_h values) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(VAL_TYPE), sizeof(imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t)); \
    imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t* ctx = (imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(ts1->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(ts2->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(values->elem_type, TID_##VAL_TYPE);                 \
    result->elem_type = values->elem_type;                              \
    result->opd[0] = imcs_operand(ts1);                                 \
    result->opd[1] = imcs_operand(ts2);                                 \
    result->opd[2] = imcs_operand(values);                              \
    ctx->offs = 0;                                                      \
    ctx->next_timestamp = IMCS_MIN_##TS_TYPE;                           \
    ctx->end_of_seq = false;                                            \
    result->next = imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_next;        \
    result->reset = imcs_asof_join_##TS_TYPE##_##VAL_TYPE##_reset;      \
    return result;                                                      \
}

IMCS_ASOF_JOIN_DEF(int32, int8)
IMCS_ASOF_JOIN_DEF(int32, int16)
IMCS_ASOF_JOIN_DEF(int32, int32)
IMCS_ASOF_JOIN_DEF(int32, int64)
IMCS_ASOF_JOIN_DEF(int32, float)
IMCS_ASOF_JOIN_DEF(int32, double)

IMCS_ASOF_JOIN_DEF(int64, int8)
IMCS_ASOF_JOIN_DEF(int64, int16)
IMCS_ASOF_JOIN_DEF(int64, int32)
IMCS_ASOF_JOIN_DEF(int64, int64)
IMCS_ASOF_JOIN_DEF(int64, float)
IMCS_ASOF_JOIN_DEF(int64, double)


#define IMCS_ASOF_JOIN_POS_DEF(TS_TYPE)                                 \
typedef struct imcs_asof_join_pos_##TS_TYPE##_context_t_ {              \
    TS_TYPE next_timestamp;                                             \
    TS_TYPE prev_timestamp;                                             \
    bool end_of_seq;                                                    \
    size_t offs;                                                        \
} imcs_asof_join_pos_##TS_TYPE##_context_t;                             \
static void imcs_asof_join_pos_##TS_TYPE##_reset(imcs_iterator_h iterator) \
{                                                                       \
    imcs_asof_join_pos_##TS_TYPE##_context_t* ctx = (imcs_asof_join_pos_##TS_TYPE##_context_t*)iterator->context; \
    ctx->offs = 0;                                                      \
    ctx->next_timestamp = IMCS_MIN_##TS_TYPE;                           \
    ctx->end_of_seq = false;                                            \
    imcs_reset_iterator(iterator);                                      \
}                                                                       \
static bool imcs_asof_join_pos_##TS_TYPE##_next(imcs_iterator_h iterator) \
{                                                                       \
    size_t i, tile_size;                                                \
    bool has_prev_value;                                                \
    int64 pos;                                                          \
    imcs_asof_join_pos_##TS_TYPE##_context_t* ctx = (imcs_asof_join_pos_##TS_TYPE##_context_t*)iterator->context; \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    has_prev_value = iterator->opd[1]->tile_size != 0;                  \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        while (!ctx->end_of_seq && iterator->opd[0]->tile.arr_##TS_TYPE[i] >= ctx->next_timestamp) { \
            ctx->prev_timestamp = ctx->next_timestamp;                  \
            if (++ctx->offs >= iterator->opd[1]->tile_size) {           \
                if (iterator->opd[1]->next(iterator->opd[1])) {         \
                    Assert(iterator->opd[1]->tile_size > 0);            \
                    ctx->offs = 0;                                      \
                    ctx->next_timestamp = iterator->opd[1]->tile.arr_##TS_TYPE[0]; \
                    has_prev_value = true;                              \
                } else if (has_prev_value)  {                           \
                    ctx->end_of_seq = true;                             \
                } else {                                                \
                    return false;                                       \
                }                                                       \
            } else {                                                    \
                ctx->next_timestamp = iterator->opd[1]->tile.arr_##TS_TYPE[ctx->offs]; \
            }                                                           \
        }                                                               \
        pos = iterator->opd[1]->next_pos - iterator->opd[1]->tile_size + ctx->offs; \
        if (!ctx->end_of_seq && iterator->opd[0]->tile.arr_##TS_TYPE[i] - ctx->prev_timestamp > ctx->next_timestamp - iterator->opd[0]->tile.arr_##TS_TYPE[i]) pos -= 1; \
        iterator->tile.arr_int64[i] = pos;                              \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_asof_join_pos_##TS_TYPE(imcs_iterator_h ts1, imcs_iterator_h ts2) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_asof_join_pos_##TS_TYPE##_context_t)); \
    imcs_asof_join_pos_##TS_TYPE##_context_t* ctx = (imcs_asof_join_pos_##TS_TYPE##_context_t*)result->context; \
    IMCS_CHECK_TYPE(ts1->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(ts2->elem_type, TID_##TS_TYPE);                     \
    result->elem_type = TID_int64;                                      \
    result->opd[0] = imcs_operand(ts1);                                 \
    result->opd[1] = imcs_operand(ts2);                                 \
    ctx->offs = 0;                                                      \
    ctx->next_timestamp = IMCS_MIN_##TS_TYPE;                           \
    ctx->end_of_seq = false;                                            \
    result->next = imcs_asof_join_pos_##TS_TYPE##_next;                 \
    result->reset = imcs_asof_join_pos_##TS_TYPE##_reset;               \
    return result;                                                      \
}

IMCS_ASOF_JOIN_POS_DEF(int32);
IMCS_ASOF_JOIN_POS_DEF(int64);

typedef struct imcs_join_context_t_ {                   
    size_t left_offs;
    size_t right_offs;
} imcs_join_context_t;

static void imcs_join_reset(imcs_iterator_h iterator)   
{                                                                       
    imcs_join_context_t* ctx = (imcs_join_context_t*)iterator->context; 
    ctx->left_offs = 0;                                                      
    ctx->right_offs = 0;                                                      
    imcs_reset_iterator(iterator);                                      
}                                                                       

#define IMCS_JOIN_POS_DEF(TS_TYPE)                                      \
static bool imcs_join_pos_##TS_TYPE##_next(imcs_iterator_h iterator)    \
{                                                                       \
    size_t i = 0, this_tile_size = imcs_tile_size;                      \
    imcs_join_context_t* ctx = (imcs_join_context_t*)iterator->context; \
    while (true) {                                                      \
        if (ctx->left_offs >= iterator->opd[0]->tile_size) {            \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                break;                                                  \
            }                                                           \
            ctx->left_offs = 0;                                         \
        }                                                               \
        if (ctx->right_offs >= iterator->opd[1]->tile_size) {           \
            if (!iterator->opd[1]->next(iterator->opd[1])) {            \
                break;                                                  \
            }                                                           \
            ctx->right_offs = 0;                                        \
        }                                                               \
        if (iterator->opd[0]->tile.arr_##TS_TYPE[ctx->left_offs] < iterator->opd[1]->tile.arr_##TS_TYPE[ctx->right_offs]) { \
            ctx->left_offs += 1;                                        \
        } else if (iterator->opd[0]->tile.arr_##TS_TYPE[ctx->left_offs] > iterator->opd[1]->tile.arr_##TS_TYPE[ctx->right_offs]) { \
            ctx->right_offs += 1;                                       \
        } else {                                                        \
            iterator->tile.arr_int64[i] = iterator->opd[1]->next_pos - iterator->opd[1]->tile_size + ctx->right_offs; \
            ctx->left_offs += 1;                                        \
            ctx->right_offs += 1;                                       \
            if (++i == this_tile_size) {                                \
                break;                                                  \
            }                                                           \
        }                                                               \
    }                                                                   \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return i != 0;                                                      \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_join_pos_##TS_TYPE(imcs_iterator_h ts1, imcs_iterator_h ts2) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_join_context_t)); \
    imcs_join_context_t* ctx = (imcs_join_context_t*)result->context;   \
    IMCS_CHECK_TYPE(ts1->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(ts2->elem_type, TID_##TS_TYPE);                     \
    result->elem_type = TID_int64;                                      \
    result->opd[0] = imcs_operand(ts1);                                 \
    result->opd[1] = imcs_operand(ts2);                                 \
    ctx->left_offs = 0;                                                 \
    ctx->right_offs = 0;                                                \
    result->next = imcs_join_pos_##TS_TYPE##_next;                      \
    result->reset = imcs_join_reset;                                    \
    return result;                                                      \
}

IMCS_JOIN_POS_DEF(int32);
IMCS_JOIN_POS_DEF(int64);

#define IMCS_JOIN_DEF(TS_TYPE,VAL_TYPE)                                 \
static bool imcs_join_##TS_TYPE##_##VAL_TYPE##_next(imcs_iterator_h iterator) \
{                                                                       \
    size_t i = 0, this_tile_size = imcs_tile_size;                      \
    imcs_join_context_t* ctx = (imcs_join_context_t*)iterator->context; \
    while (true) {                                                      \
        if (ctx->left_offs >= iterator->opd[0]->tile_size) {            \
            if (!iterator->opd[0]->next(iterator->opd[0])) {            \
                break;                                                  \
            }                                                           \
            ctx->left_offs = 0;                                         \
        }                                                               \
        if (ctx->right_offs >= iterator->opd[1]->tile_size) {           \
            if (!iterator->opd[1]->next(iterator->opd[1])) {            \
                break;                                                  \
            }                                                           \
            if (!iterator->opd[2]->next(iterator->opd[2]) || iterator->opd[2]->tile_size != iterator->opd[1]->tile_size) { \
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("values timeseries is too short")))); \
            }                                                           \
            ctx->right_offs = 0;                                        \
        }                                                               \
        if (iterator->opd[0]->tile.arr_##TS_TYPE[ctx->left_offs] < iterator->opd[1]->tile.arr_##TS_TYPE[ctx->right_offs]) { \
            ctx->left_offs += 1;                                        \
        } else if (iterator->opd[0]->tile.arr_##TS_TYPE[ctx->left_offs] > iterator->opd[1]->tile.arr_##TS_TYPE[ctx->right_offs]) { \
            ctx->right_offs += 1;                                       \
        } else {                                                        \
            iterator->tile.arr_##VAL_TYPE[i] = iterator->opd[2]->tile.arr_##VAL_TYPE[ctx->right_offs]; \
            ctx->left_offs += 1;                                        \
            ctx->right_offs += 1;                                       \
            if (++i == this_tile_size) {                                \
                break;                                                  \
            }                                                           \
        }                                                               \
    }                                                                   \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return i != 0;                                                      \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_join_##TS_TYPE##_##VAL_TYPE(imcs_iterator_h ts1, imcs_iterator_h ts2, imcs_iterator_h values) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(VAL_TYPE), sizeof(imcs_join_context_t)); \
    imcs_join_context_t* ctx = (imcs_join_context_t*)result->context;   \
    IMCS_CHECK_TYPE(ts1->elem_type, TID_##TS_TYPE);                     \
    IMCS_CHECK_TYPE(ts2->elem_type, TID_##TS_TYPE);                     \
    result->elem_type = TID_##VAL_TYPE;                                 \
    result->opd[0] = imcs_operand(ts1);                                 \
    result->opd[1] = imcs_operand(ts2);                                 \
    result->opd[2] = imcs_operand(values);                                 \
    ctx->left_offs = 0;                                                 \
    ctx->right_offs = 0;                                                \
    result->next = imcs_join_##TS_TYPE##_##VAL_TYPE##_next;             \
    result->reset = imcs_join_reset;                                    \
    return result;                                                      \
}

IMCS_JOIN_DEF(int32, int8)
IMCS_JOIN_DEF(int32, int16)
IMCS_JOIN_DEF(int32, int32)
IMCS_JOIN_DEF(int32, int64)
IMCS_JOIN_DEF(int32, float)
IMCS_JOIN_DEF(int32, double)

IMCS_JOIN_DEF(int64, int8)
IMCS_JOIN_DEF(int64, int16)
IMCS_JOIN_DEF(int64, int32)
IMCS_JOIN_DEF(int64, int64)
IMCS_JOIN_DEF(int64, float)
IMCS_JOIN_DEF(int64, double)

#define ROTL32(x, r) ((x) << (r)) | ((x) >> (32 - (r)))
#define HASH_BITS 25
#define N_HASHES (1 << (32 - HASH_BITS))
#define MURMUR_SEED 0x5C1DB

static uint32 murmur_hash3_32(const void* key, const int len, const uint32 seed)
{
    const uint8* data = (const uint8*)key;
    const int nblocks = len / 4;
    
    uint32 h1 = seed;
    
    uint32 c1 = 0xcc9e2d51;
    uint32 c2 = 0x1b873593;
    int i;
    uint32 k1;
    const uint8* tail;
    const uint32* blocks = (const uint32 *)(data + nblocks*4);
    
    for(i = -nblocks; i; i++)
    {
        k1 = blocks[i];
        
        k1 *= c1;
        k1 = ROTL32(k1,15);
        k1 *= c2;
        
        h1 ^= k1;
        h1 = ROTL32(h1,13); 
        h1 = h1*5+0xe6546b64;
    }
    
    tail = (const uint8*)(data + nblocks*4);
    
    k1 = 0;

    switch(len & 3)
    {
      case 3: 
        k1 ^= tail[2] << 16;
        /* no break */
      case 2: 
        k1 ^= tail[1] << 8;
        /* no break */
      case 1: 
        k1 ^= tail[0];
        k1 *= c1;
        k1 = ROTL32(k1,15); 
        k1 *= c2; 
        h1 ^= k1;
    }
    
    h1 ^= len;   
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    
    return h1;
} 

inline static void calculate_zero_bits(uint32 h, uint8* max_zero_bits)
{
    int j = h >> HASH_BITS;
    int zero_bits = 1;
    while ((h & 1) == 0 && zero_bits <= HASH_BITS)  {
        h >>= 1;
        zero_bits += 1;
    }
    if (max_zero_bits[j] < zero_bits) { 
        max_zero_bits[j] = zero_bits;
    }
}

inline static void calculate_hash_functions(void const* val, int size, uint8* max_zero_bits)
{
    uint32 h = murmur_hash3_32(val, size, MURMUR_SEED);
    calculate_zero_bits(h, max_zero_bits);
}

inline static void merge_zero_bits(uint8* dst_max_zero_bits, uint8* src_max_zero_bits)
{
    int i;
    for (i = 0; i < N_HASHES; i++) { 
        if (dst_max_zero_bits[i] < src_max_zero_bits[i]) { 
            dst_max_zero_bits[i] = src_max_zero_bits[i];
        }
    }
}

static uint32 approximate_distinct_count(uint8* max_zero_bits)
{
    const int m = N_HASHES;
    const double alpha_m = 0.7213 / (1 + 1.079 / (double)m);
    const double pow_2_32 = 0xffffffff;
    double E, c = 0;
    int i;
    for (i = 0; i < m; i++)
    {
        c += 1 / pow(2., (double)max_zero_bits[i]);
    }
    E = alpha_m * m * m / c;    

    if (E <= (5 / 2. * m))
    {
        double V = 0;
        for (i = 0; i < m; i++)
        {
            if (max_zero_bits[i] == 0) V++;
        }

        if (V > 0)
        {
            E = m * log(m / V);
        }
    }
    else if (E > (1 / 30. * pow_2_32))
    {
        E = -pow_2_32 * log(1 - E / pow_2_32);
    }
    return (uint32)E;
}

typedef struct {                                                        
    uint8 max_zero_bits[N_HASHES];
} imcs_agg_approxdc_context_t;                                  

static void imcs_approxdc_merge(imcs_iterator_h dst, imcs_iterator_h src) 
{                                                                       
    imcs_agg_approxdc_context_t* src_ctx = (imcs_agg_approxdc_context_t*)src->context; 
    imcs_agg_approxdc_context_t* dst_ctx = (imcs_agg_approxdc_context_t*)dst->context; 
    merge_zero_bits(dst_ctx->max_zero_bits, src_ctx->max_zero_bits);
    dst->tile.arr_int64[0] = approximate_distinct_count(dst_ctx->max_zero_bits);
}                             
                                          
static bool imcs_approxdc_next(imcs_iterator_h iterator) 
{                                                                       
    imcs_agg_approxdc_context_t* ctx = (imcs_agg_approxdc_context_t*)iterator->context; 
    size_t i, tile_size;                                            
    uint8 max_zero_bits[N_HASHES] = {0};
    size_t elem_size = iterator->opd[0]->elem_size;
    if (iterator->flags & FLAG_PREPARED) {                              
        return iterator->tile_size != 0;                                
    }                                                                   
    if (iterator->next_pos != 0) {                                   
        return false;                                        
    }                                                                   
    while (iterator->opd[0]->next(iterator->opd[0])) { 
        tile_size = iterator->opd[0]->tile_size;                      
        for (i = 0; i < tile_size; i++) { 
            calculate_hash_functions(iterator->opd[0]->tile.arr_char + i*elem_size, elem_size, max_zero_bits);
        }
    }
    iterator->next_pos = 1;                                          
    iterator->tile_size = 1;                                            
    iterator->tile.arr_int64[0] = approximate_distinct_count(max_zero_bits);
    memcpy(ctx->max_zero_bits, max_zero_bits, sizeof max_zero_bits);
    return true;                                                    
}                                                                       
 
imcs_iterator_h imcs_approxdc(imcs_iterator_h input) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_agg_approxdc_context_t));
    result->elem_type = TID_int64;                   
    result->opd[0] = imcs_operand(input);                                               
    result->next = imcs_approxdc_next;                      
    result->prepare = imcs_approxdc_next;                   
    result->merge = imcs_approxdc_merge;                    
    return result;                                                    
}


static bool imcs_group_approxdc_next(imcs_iterator_h iterator) 
{                                                                       
    size_t i, j = 0, tile_size;                                     
    size_t this_tile_size = imcs_tile_size;                         
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)iterator->context; 
    size_t grp_elem_size = iterator->opd[1]->elem_size;                  
    size_t inp_elem_size = iterator->opd[0]->elem_size;                  
    while (true) {                                                         
        if (ctx->offset >= iterator->opd[1]->tile_size) {                
            if (!iterator->opd[1]->next(iterator->opd[1])) { 
                if (j + ctx->count != 0) {    
                    if (ctx->count != 0) {                              
                        iterator->tile.arr_int64[j++] = approximate_distinct_count((uint8*)ctx->history.arr_int8);
                        iterator->next_pos += 1;                     
                        ctx->count = 0;                                 
                    }                                                   
                    iterator->tile_size = j;                            
                    return true;                                      
                }                                                       
                return false;                                                  
            } else {                                                    
                Assert(ctx->offset == iterator->opd[0]->tile_size); 
                if (!iterator->opd[0]->next(iterator->opd[0])) { 
                    return false;                                              
                }                                                       
                if (iterator->opd[1]->tile_size > iterator->opd[0]->tile_size) { 
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("group be sequence doesn't match values sequence")))); \
                }
            }                                                           
            ctx->offset = 0;                                            
        }                                                               
        tile_size = iterator->opd[1]->tile_size;                         
        for (i = ctx->offset; i < tile_size; i++) {                     
            bool same;                                              
            switch (grp_elem_size) {                                        
              case 1:                                                   
                same = ctx->accumulator.val_int8 == iterator->opd[1]->tile.arr_int8[i]; 
                ctx->accumulator.val_int8 = iterator->opd[1]->tile.arr_int8[i];       
                break;                                                  
              case 2:                                                   
                same = ctx->accumulator.val_int16 == iterator->opd[1]->tile.arr_int16[i]; 
                ctx->accumulator.val_int16 = iterator->opd[1]->tile.arr_int16[i];       
                break;                                                  
              case 4:                                                   
                same = ctx->accumulator.val_int32 == iterator->opd[1]->tile.arr_int32[i]; 
                ctx->accumulator.val_int32 = iterator->opd[1]->tile.arr_int32[i];       
                break;                                                  
              case 8:                                                  
                same = ctx->accumulator.val_int64 == iterator->opd[1]->tile.arr_int64[i]; 
                ctx->accumulator.val_int64 = iterator->opd[1]->tile.arr_int64[i];       
                break;
              default:                                                  
                same = memcmp(ctx->accumulator.val_ptr, &iterator->opd[1]->tile.arr_char[i*grp_elem_size], grp_elem_size) == 0; 
                memcpy(ctx->accumulator.val_ptr, &iterator->opd[1]->tile.arr_char[i*grp_elem_size], grp_elem_size);              
            }                                                           
            if (ctx->count != 0 && !same) {                             
                iterator->tile.arr_int64[j] = approximate_distinct_count((uint8*)ctx->history.arr_int8);
                iterator->next_pos += 1;                             
                ctx->count = 0;                                         
                j += 1;                                                 
            }                                                           
            if (ctx->count == 0) {
                memset(ctx->history.arr_int8, 0, N_HASHES);
            }
            calculate_hash_functions(iterator->opd[0]->tile.arr_char + i*inp_elem_size, inp_elem_size, (uint8*)ctx->history.arr_int8);
            ctx->count += 1;                                            
            if (j == this_tile_size) {                               
                ctx->offset = i + 1;                                    
                iterator->tile_size = this_tile_size;                
                return true;                                        
            }                                                           
        }                                                               
        ctx->offset = tile_size;                                        
    }                                                                   
}                                                                       
                                                                        
imcs_iterator_h imcs_group_approxdc(imcs_iterator_h input, imcs_iterator_h group_by) 
{                                                                       
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_agg_context_t) + N_HASHES);
    imcs_agg_context_t* ctx = (imcs_agg_context_t*)result->context; 
    result->elem_type = TID_int64;                   
    result->opd[0] = imcs_operand(input);                                               
    result->opd[1] = imcs_operand(group_by);                                           
    result->next = imcs_group_approxdc_next;                      
    ctx->offset = ctx->count = 0;           
    if (group_by->elem_type == TID_char) { 
        ctx->accumulator.val_ptr = (char*)imcs_alloc(group_by->elem_size);
    }
    return result;                                                    
}
                                                                        
typedef struct imcs_hash_elem_t_ { 
    imcs_key_t grp;
    imcs_key_t agg;
    struct imcs_hash_elem_t_* collision;
    imcs_count_t count;
    uint32 grp_hash;
    uint32 agg_hash;
} imcs_hash_elem_t;

#define IMCS_HASH_BASKET_N_ELEMS 1024
#define IMCS_HASH_BASKET_SIZE    (IMCS_HASH_BASKET_N_ELEMS*sizeof(imcs_hash_elem_t))

typedef struct imcs_hash_basket_t_ {    
    struct imcs_hash_basket_t_* next;
    union { 
        imcs_hash_elem_t elems[IMCS_HASH_BASKET_N_ELEMS];
        char keys[IMCS_HASH_BASKET_SIZE];
    } u;
} imcs_hash_basket_t;

typedef struct {
    size_t table_size;
    size_t table_used;
    imcs_hash_basket_t* baskets;
    imcs_hash_elem_t** table;
} imcs_hash_t;

typedef struct { 
    imcs_hash_t* hash;
} imcs_shared_hash_t;    

typedef struct imcs_hash_iterator_context_t_ { 
    size_t n_groups;
    size_t chain_no;
    imcs_hash_elem_t* curr_elem;    
    imcs_shared_hash_t* shared;
    imcs_hash_t* private_hash;
} imcs_hash_iterator_context_t;

static void imcs_hash_agg_reset(imcs_iterator_h iterator) 
{
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context;
    ctx->curr_elem = 0;
    ctx->chain_no = 0;
    imcs_reset_iterator(iterator);
}

#define IMCS_HASH_AGG_DEF(AGG_TYPE, IN_TYPE, OP, INITIALIZE, ACCUMULATE, RESULT) \
static bool imcs_hash_initialize_##OP##_##IN_TYPE(imcs_iterator_h iterator) \
{                                                                       \
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; \
    size_t i, tile_size;                                                \
    size_t elem_size = iterator->opd[1]->elem_size;                     \
    imcs_hash_elem_t* elem;                                             \
    imcs_key_t val;                                                     \
    size_t elems_basket_used = IMCS_HASH_BASKET_N_ELEMS;                \
    size_t keys_basket_used = IMCS_HASH_BASKET_SIZE;                    \
    imcs_hash_basket_t* elems_basket = 0;                               \
    imcs_hash_basket_t* keys_basket = 0;                                \
    size_t distinct_count = 0;                                          \
    size_t hash_table_size = ctx->n_groups;                             \
    size_t threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; \
    imcs_hash_t* hash = (imcs_hash_t*)imcs_alloc(sizeof(imcs_hash_t)); \
    hash->table_size = hash_table_size;                                 \
    hash->table = (imcs_hash_elem_t**)imcs_alloc(hash_table_size*sizeof(imcs_hash_elem_t*)); \
    hash->baskets = 0;                                                  \
    memset(hash->table, 0, sizeof(imcs_hash_elem_t*)*hash_table_size);  \
    ctx->private_hash = hash;                                           \
    val.val_int64 = 0;                                                  \
    while (iterator->opd[1]->next(iterator->opd[1])) {                  \
        if (iterator->opd[0] != 0) {                                    \
            if (!iterator->opd[0]->next(iterator->opd[0]) || iterator->opd[0]->tile_size != iterator->opd[1]->tile_size) { \
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("group be sequence doesn't match values sequence")))); \
            }                                                           \
        }                                                               \
        tile_size = iterator->opd[1]->tile_size;                        \
        for (i = 0; i < tile_size; i++) {                               \
            uint32 hash_value = elem_size == 4 ? iterator->opd[1]->tile.arr_int32[i] : murmur_hash3_32(iterator->opd[1]->tile.arr_char + i*elem_size, elem_size, MURMUR_SEED); \
            uint32 hash_index = hash_value % hash_table_size;           \
            int64 diff = 1;                                             \
            imcs_hash_elem_t** pprev = &hash->table[hash_index];        \
            if (elem_size <= sizeof(imcs_key_t)) {                      \
                memcpy(&val.val_int8, iterator->opd[1]->tile.arr_char + i*elem_size, elem_size); \
                for (elem = *pprev; elem != NULL && (diff = elem->grp.val_int64 - val.val_int64) < 0; elem = *(pprev = &elem->collision)); \
            } else {                                                    \
                val.val_ptr = iterator->opd[1]->tile.arr_char + i*elem_size; \
                for (elem = *pprev; elem != NULL && (diff = memcmp(elem->grp.val_ptr, val.val_ptr, elem_size)) < 0; elem = *(pprev = &elem->collision)); \
            }                                                           \
            if (diff == 0) {                                            \
                ACCUMULATE(elem->agg.val_##AGG_TYPE, iterator->opd[0]->tile.arr_##IN_TYPE[i]); \
                elem->count += 1;                                       \
            } else {                                                    \
                if (elems_basket_used == IMCS_HASH_BASKET_N_ELEMS) {    \
                    elems_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); \
                    elems_basket->next = hash->baskets;                 \
                    hash->baskets = elems_basket;                       \
                    elems_basket_used = 0;                              \
                }                                                       \
                elem = &elems_basket->u.elems[elems_basket_used++];     \
                if (elem_size <= sizeof(imcs_key_t)) {                  \
                    elem->grp.val_int64 = val.val_int64;                \
                } else {                                                \
                    if (keys_basket_used + elem_size > IMCS_HASH_BASKET_SIZE) { \
                        keys_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); \
                        keys_basket->next = hash->baskets;              \
                        hash->baskets = keys_basket;                    \
                        keys_basket_used = 0;                           \
                    }                                                   \
                    elem->grp.val_ptr = &keys_basket->u.keys[keys_basket_used]; \
                    keys_basket_used += elem_size;                      \
                    memcpy(elem->grp.val_ptr, val.val_ptr, elem_size);  \
                }                                                       \
                elem->agg.val_##AGG_TYPE = INITIALIZE(iterator->opd[0]->tile.arr_##IN_TYPE[i]); \
                elem->grp_hash = hash_value;                            \
                elem->collision = *pprev;                               \
                *pprev = elem;                                          \
                elem->count = 1;                                        \
                if (++distinct_count >= threshold) {                    \
                    imcs_hash_elem_t** table;                           \
                    size_t new_hash_table_size = imcs_next_prime_number(hash_table_size); \
                    size_t j;                                           \
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); \
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); \
                    for (j = 0; j < hash_table_size; j++) {             \
                        imcs_hash_elem_t* next;                         \
                        for (elem = hash->table[j]; elem != NULL; elem = next) { \
                            imcs_hash_elem_t* ep;                       \
                            next = elem->collision;                     \
                            pprev = &table[elem->grp_hash % new_hash_table_size]; \
                            if (elem_size <= sizeof(imcs_key_t)) {      \
                                for (ep = *pprev; ep != NULL && (diff = ep->grp.val_int64 - elem->grp.val_int64) < 0; ep = *(pprev = &ep->collision)); \
                            } else {                                    \
                                for (ep = *pprev; ep != NULL && (diff = memcmp(ep->grp.val_ptr, elem->grp.val_ptr, elem_size)) < 0; ep = *(pprev = &ep->collision)); \
                            }                                           \
                            elem->collision = *pprev;                   \
                            *pprev = elem;                              \
                        }                                               \
                    }                                                   \
                    hash->table_size = hash_table_size = new_hash_table_size; \
                    imcs_free(hash->table);                             \
                    hash->table = table;                                \
                    threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; \
                }                                                       \
            }                                                           \
        }                                                               \
    }                                                                   \
    hash->table_used = distinct_count;                                  \
    return true;                                                        \
}                                                                       \
static void imcs_hash_merge_##OP##_##IN_TYPE(imcs_iterator_h dst, imcs_iterator_h src) \
{                                                                       \
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)dst->context; \
    imcs_hash_iterator_context_t* src_ctx  = (imcs_hash_iterator_context_t*)src->context; \
    imcs_hash_t* hash = ctx->private_hash;                              \
    imcs_hash_t* src_hash = src_ctx->private_hash;                      \
    size_t i, n;                                                        \
    size_t elem_size = dst->opd[1]->elem_size;                          \
    imcs_hash_elem_t* elem;                                             \
    imcs_hash_elem_t* next;                                             \
    imcs_hash_elem_t* src_elem;                                         \
    size_t distinct_count = hash->table_used;                           \
    size_t hash_table_size = hash->table_size;                          \
    size_t threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; \
                                                                        \
    for (i = 0, n = src_hash->table_size; i < n; i++) {                 \
        for (src_elem = src_hash->table[i]; src_elem != NULL; src_elem = next) { \
            uint32 hash_value = src_elem->grp_hash;                     \
            uint32 hash_index = hash_value % hash_table_size;           \
            imcs_hash_elem_t** pprev = &hash->table[hash_index];        \
            int64 diff = 1;                                             \
            next = src_elem->collision;                                 \
            if (elem_size <= sizeof(imcs_key_t)) {                      \
                for (elem = *pprev; elem != NULL && (diff = elem->grp.val_int64 - src_elem->grp.val_int64) < 0; elem = *(pprev = &elem->collision)); \
            } else {                                                    \
                for (elem = *pprev; elem != NULL && (diff = memcmp(elem->grp.val_ptr, src_elem->grp.val_ptr, elem_size)) < 0; elem = *(pprev = &elem->collision)); \
            }                                                           \
            if (diff == 0) {                                            \
                ACCUMULATE(elem->agg.val_##AGG_TYPE, src_elem->agg.val_##AGG_TYPE); \
                elem->count += src_elem->count;                         \
            } else {                                                    \
                src_elem->collision = *pprev;                           \
                *pprev = src_elem;                                      \
                if (++distinct_count >= threshold) {                    \
                    imcs_hash_elem_t** table;                           \
                    size_t new_hash_table_size = imcs_next_prime_number(hash_table_size); \
                    size_t j;                                           \
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); \
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); \
                    for (j = 0; j < hash_table_size; j++) {             \
                        imcs_hash_elem_t* next;                         \
                        for (elem = hash->table[j]; elem != NULL; elem = next) { \
                            imcs_hash_elem_t* ep;                       \
                            next = elem->collision;                     \
                            pprev = &table[elem->grp_hash % new_hash_table_size]; \
                            if (elem_size <= sizeof(imcs_key_t)) {      \
                                for (ep = *pprev; ep != NULL && (diff = ep->grp.val_int64 - elem->grp.val_int64) < 0; ep = *(pprev = &ep->collision)); \
                            } else {                                    \
                                for (ep = *pprev; ep != NULL && (diff = memcmp(ep->grp.val_ptr, elem->grp.val_ptr, elem_size)) < 0; ep = *(pprev = &ep->collision)); \
                            }                                           \
                            elem->collision = *pprev;                   \
                            *pprev = elem;                              \
                        }                                               \
                    }                                                   \
                    hash->table_size = hash_table_size = new_hash_table_size; \
                    imcs_free(hash->table);                             \
                    hash->table = table;                                \
                    threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; \
                }                                                       \
            }                                                           \
        }                                                               \
    }                                                                   \
    hash->table_used = distinct_count;                                  \
    ctx->shared->hash = hash;                                           \
}                                                                       \
static bool imcs_hash_##OP##_##IN_TYPE##_next_agg(imcs_iterator_h iterator) \
{                                                                       \
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; \
    size_t this_tile_size = imcs_tile_size;                             \
    size_t i;                                                           \
    imcs_hash_elem_t* elem;                                             \
    size_t chain_no;                                                    \
    size_t table_size;                                                  \
    imcs_hash_t* hash = ctx->shared->hash;                              \
    if (!hash) {                                                        \
        imcs_hash_initialize_##OP##_##IN_TYPE(iterator);                \
        hash = ctx->shared->hash = ctx->private_hash;                   \
    }                                                                   \
    elem = ctx->curr_elem;                                              \
    chain_no = ctx->chain_no;                                           \
    table_size = hash->table_size;                                      \
    for (i = 0; i < this_tile_size; i++) {                              \
        while (elem == NULL && chain_no < table_size) {                 \
            elem = hash->table[chain_no++];                             \
        }                                                               \
        if (elem != NULL) {                                             \
            iterator->tile.arr_##AGG_TYPE[i] = RESULT(elem->agg.val_##AGG_TYPE, elem->count); \
            elem = elem->collision;                                     \
        } else {                                                        \
            break;                                                      \
        }                                                               \
    }                                                                   \
    ctx->curr_elem = elem;                                              \
    ctx->chain_no = chain_no;                                           \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return i != 0;                                                      \
}                                                                       \
static bool imcs_hash_##OP##_##IN_TYPE##_next_grp(imcs_iterator_h iterator) \
{                                                                       \
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; \
    size_t this_tile_size = imcs_tile_size;                             \
    size_t i;                                                           \
    imcs_hash_elem_t* elem;                                             \
    size_t chain_no;                                                    \
    size_t table_size;                                                  \
    size_t elem_size = iterator->elem_size;                             \
    imcs_hash_t* hash = ctx->shared->hash;                              \
    if (!hash) {                                                        \
        imcs_hash_initialize_##OP##_##IN_TYPE(iterator);                \
        hash = ctx->shared->hash = ctx->private_hash;                   \
    }                                                                   \
    elem = ctx->curr_elem;                                              \
    chain_no = ctx->chain_no;                                           \
    table_size = hash->table_size;                                      \
    for (i = 0; i < this_tile_size; i++) {                              \
        while (elem == NULL && chain_no < table_size) {                 \
            elem = hash->table[chain_no++];                             \
        }                                                               \
        if (elem != NULL) {                                             \
            memcpy(iterator->tile.arr_char + i*elem_size, (elem_size <= sizeof(imcs_key_t)) ? (char*)&elem->grp.val_int8 : elem->grp.val_ptr, elem_size); \
            elem = elem->collision;                                     \
        } else {                                                        \
            break;                                                      \
        }                                                               \
    }                                                                   \
    ctx->curr_elem = elem;                                              \
    ctx->chain_no = chain_no;                                           \
    iterator->tile_size = i;                                            \
    iterator->next_pos += i;                                            \
    return i != 0;                                                      \
}                                                                       \
void imcs_hash_##OP##_##IN_TYPE(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by) \
{                                                                       \
    imcs_iterator_h result_agg = imcs_new_iterator(sizeof(AGG_TYPE), sizeof(imcs_hash_iterator_context_t)); \
    imcs_iterator_h result_grp = imcs_new_iterator(group_by->elem_size, sizeof(imcs_hash_iterator_context_t)); \
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)result_agg->context; \
    imcs_shared_hash_t* shared;                                         \
    if (input != 0) {                                                   \
        IMCS_CHECK_TYPE(input->elem_type, TID_##IN_TYPE);               \
    }                                                                   \
    shared = (imcs_shared_hash_t*)imcs_alloc(sizeof(imcs_shared_hash_t)); \
    shared->hash = 0;                                                   \
                                                                        \
    ctx->n_groups = imcs_hash_table_init_size;                          \
    ctx->shared = shared;                                               \
    ctx->curr_elem = 0;                                                 \
    ctx->chain_no = 0;                                                  \
    result_agg->opd[0] = input != NULL ? imcs_operand(input) : NULL;    \
    result_agg->opd[1] = imcs_operand(group_by);                        \
    result_agg->elem_type = TID_##AGG_TYPE;                             \
    result_agg->next = imcs_hash_##OP##_##IN_TYPE##_next_agg;           \
    result_agg->reset = imcs_hash_agg_reset;                            \
    result_agg->prepare = imcs_hash_initialize_##OP##_##IN_TYPE;        \
    result_agg->merge = imcs_hash_merge_##OP##_##IN_TYPE;               \
                                                                        \
    ctx = (imcs_hash_iterator_context_t*)result_grp->context;           \
    ctx->n_groups = imcs_hash_table_init_size;                          \
    ctx->shared = shared;                                               \
    ctx->curr_elem = 0;                                                 \
    ctx->chain_no = 0;                                                  \
    result_grp->opd[0] = input != NULL ? imcs_operand(input) : NULL;    \
    result_grp->opd[1] = imcs_operand(group_by);                        \
    result_grp->elem_type = group_by->elem_type;                        \
    result_grp->next = imcs_hash_##OP##_##IN_TYPE##_next_grp;           \
    result_grp->reset = imcs_hash_agg_reset;                            \
    result[0] = result_agg;                                             \
    result[1] = result_grp;                                             \
}

#define IMCS_HASH_AGG_INIT(val) val
#define IMCS_HASH_MAX_ACCUMULATE(acc, val) if (val > acc) acc = val
#define IMCS_HASH_AGG_RESULT(acc, count) acc
IMCS_HASH_AGG_DEF(int8, int8, max, IMCS_HASH_AGG_INIT, IMCS_HASH_MAX_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int16, int16, max, IMCS_HASH_AGG_INIT, IMCS_HASH_MAX_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int32, int32, max, IMCS_HASH_AGG_INIT, IMCS_HASH_MAX_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int64, int64, max, IMCS_HASH_AGG_INIT, IMCS_HASH_MAX_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(float, float, max, IMCS_HASH_AGG_INIT, IMCS_HASH_MAX_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(double, double, max, IMCS_HASH_AGG_INIT, IMCS_HASH_MAX_ACCUMULATE, IMCS_HASH_AGG_RESULT)

#define IMCS_HASH_MIN_ACCUMULATE(acc, val) if (val < acc) acc = val
IMCS_HASH_AGG_DEF(int8, int8, min, IMCS_HASH_AGG_INIT, IMCS_HASH_MIN_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int16, int16, min, IMCS_HASH_AGG_INIT, IMCS_HASH_MIN_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int32, int32, min, IMCS_HASH_AGG_INIT, IMCS_HASH_MIN_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int64, int64, min, IMCS_HASH_AGG_INIT, IMCS_HASH_MIN_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(float, float, min, IMCS_HASH_AGG_INIT, IMCS_HASH_MIN_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(double, double, min, IMCS_HASH_AGG_INIT, IMCS_HASH_MIN_ACCUMULATE, IMCS_HASH_AGG_RESULT)

#define IMCS_HASH_SUM_ACCUMULATE(acc, val) acc += val
IMCS_HASH_AGG_DEF(int64, int8, sum, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int64, int16, sum, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int64, int32, sum, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int64, int64, sum, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(double, float, sum, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(double, double, sum, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AGG_RESULT)

#define IMCS_HASH_ANY_ACCUMULATE(acc, val) acc |= val
IMCS_HASH_AGG_DEF(int8, int8, any, IMCS_HASH_AGG_INIT, IMCS_HASH_ANY_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int16, int16, any, IMCS_HASH_AGG_INIT, IMCS_HASH_ANY_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int32, int32, any, IMCS_HASH_AGG_INIT, IMCS_HASH_ANY_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int64, int64, any, IMCS_HASH_AGG_INIT, IMCS_HASH_ANY_ACCUMULATE, IMCS_HASH_AGG_RESULT)

void imcs_hash_any_float(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by) 
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_HASH_ANY is supported only for integer types")))); 
}
void imcs_hash_any_double(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by) 
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_HASH_ANY is supported only for integer types")))); 
}

#define IMCS_HASH_ALL_ACCUMULATE(acc, val) acc &= val
IMCS_HASH_AGG_DEF(int8, int8, all, IMCS_HASH_AGG_INIT, IMCS_HASH_ALL_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int16, int16, all, IMCS_HASH_AGG_INIT, IMCS_HASH_ALL_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int32, int32, all, IMCS_HASH_AGG_INIT, IMCS_HASH_ALL_ACCUMULATE, IMCS_HASH_AGG_RESULT)
IMCS_HASH_AGG_DEF(int64, int64, all, IMCS_HASH_AGG_INIT, IMCS_HASH_ALL_ACCUMULATE, IMCS_HASH_AGG_RESULT)

void imcs_hash_all_float(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by) 
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_HASH_ALL is supported only for integer types")))); 
}
void imcs_hash_all_double(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by) 
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Aggregate CS_HASH_ALL is supported only for integer types")))); 
}

#define IMCS_HASH_AVG_RESULT(acc, count) acc/count
IMCS_HASH_AGG_DEF(int64, int8, avg, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AVG_RESULT)
IMCS_HASH_AGG_DEF(int64, int16, avg, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AVG_RESULT)
IMCS_HASH_AGG_DEF(int64, int32, avg, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AVG_RESULT)
IMCS_HASH_AGG_DEF(int64, int64, avg, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AVG_RESULT)
IMCS_HASH_AGG_DEF(double, float, avg, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AVG_RESULT)
IMCS_HASH_AGG_DEF(double, double, avg, IMCS_HASH_AGG_INIT, IMCS_HASH_SUM_ACCUMULATE, IMCS_HASH_AVG_RESULT)

#define IMCS_HASH_COUNT_INIT(val) 0
#define IMCS_HASH_COUNT_ACCUMULATE(acc, val) 
#define IMCS_HASH_COUNT_RESULT(acc, count) count

void imcs_hash_count_int64(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by);
IMCS_HASH_AGG_DEF(int64, int64, count, IMCS_HASH_COUNT_INIT, IMCS_HASH_COUNT_ACCUMULATE, IMCS_HASH_COUNT_RESULT)

void imcs_hash_count(imcs_iterator_h result[2], imcs_iterator_h group_by)
{
    imcs_hash_count_int64(result, NULL, group_by);
}

static bool imcs_hash_initialize_approxdc(imcs_iterator_h iterator) 
{                                                                       
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; 
    imcs_hash_t* hash;                                               
    size_t i, tile_size;                                            
    size_t agg_elem_size = iterator->opd[0]->elem_size;                  
    size_t grp_elem_size = iterator->opd[1]->elem_size;                  
    imcs_hash_elem_t* elem;                                          
    imcs_key_t val;                                                
    size_t elems_basket_used = IMCS_HASH_BASKET_N_ELEMS;         
    size_t keys_basket_used = IMCS_HASH_BASKET_SIZE;             
    imcs_hash_basket_t* elems_basket = 0;                            
    imcs_hash_basket_t* keys_basket = 0;                             
    size_t distinct_count = 0;                                      
    size_t hash_table_size = ctx->n_groups;                         
    size_t threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; 
                                                                        
    hash = (imcs_hash_t*)imcs_alloc(sizeof(imcs_hash_t));     
    hash->table_size = hash_table_size;                                 
    hash->table = (imcs_hash_elem_t**)imcs_alloc(hash_table_size*sizeof(imcs_hash_elem_t*)); 
    hash->baskets = 0;                                                  
    memset(hash->table, 0, sizeof(imcs_hash_elem_t*)*hash_table_size); 
    ctx->private_hash = hash;                                           
    val.val_int64 = 0;                                                   
    while (iterator->opd[1]->next(iterator->opd[1])) { 
        if (iterator->opd[0] != 0) {                                      
            if (!iterator->opd[0]->next(iterator->opd[0]) || iterator->opd[0]->tile_size != iterator->opd[1]->tile_size) { 
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("group be sequence doesn't match values sequence")))); 
            }                                                           
        }                                                               
        tile_size = iterator->opd[1]->tile_size;                         
        for (i = 0; i < tile_size; i++) {                               
            uint32 hash_value = murmur_hash3_32(iterator->opd[1]->tile.arr_char + i*grp_elem_size, grp_elem_size, MURMUR_SEED); 
            uint32 hash_index = hash_value % hash_table_size;            
            imcs_hash_elem_t** pprev = &hash->table[hash_index];    
            int64 diff = 1;
            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                memcpy(&val.val_int8, iterator->opd[1]->tile.arr_char + i*grp_elem_size, grp_elem_size); 
                for (elem = *pprev; elem != NULL && (diff = elem->grp.val_int64 - val.val_int64) < 0; elem = *(pprev = &elem->collision));
            } else {                                                    
                val.val_ptr = iterator->opd[1]->tile.arr_char + i*grp_elem_size; 
                for (elem = *pprev; elem != NULL && (diff = memcmp(elem->grp.val_ptr, val.val_ptr, grp_elem_size)) < 0; elem = *(pprev = &elem->collision));
            } 
            if (diff == 0) {                                         
                calculate_hash_functions(iterator->opd[0]->tile.arr_char + i*agg_elem_size, agg_elem_size, (uint8*)elem->agg.val_ptr); 
            } else {                                                    
                if (elems_basket_used == IMCS_HASH_BASKET_N_ELEMS) { 
                    elems_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                    elems_basket->next = hash->baskets;                 
                    hash->baskets = elems_basket;                       
                    elems_basket_used = 0;                              
                }                                                       
                elem = &elems_basket->u.elems[elems_basket_used++];     
                if (grp_elem_size <= sizeof(imcs_key_t)) {               
                    elem->grp.val_int64 = val.val_int64;                  
                } else {                                                
                    if (keys_basket_used + grp_elem_size > IMCS_HASH_BASKET_SIZE) { 
                        keys_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                        keys_basket->next = hash->baskets;              
                        hash->baskets = keys_basket;                    
                        keys_basket_used = 0;                           
                    }                                                   
                    elem->grp.val_ptr = &keys_basket->u.keys[keys_basket_used]; 
                    keys_basket_used += grp_elem_size;                      
                    memcpy(elem->grp.val_ptr, val.val_ptr, grp_elem_size); 
                }               
                if (keys_basket_used + N_HASHES > IMCS_HASH_BASKET_SIZE) { 
                    keys_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                    keys_basket->next = hash->baskets;              
                    hash->baskets = keys_basket;                    
                    keys_basket_used = 0;                           
                }                                                   
                elem->agg.val_ptr = &keys_basket->u.keys[keys_basket_used]; 
                keys_basket_used += N_HASHES;          
                memset(elem->agg.val_ptr, 0, N_HASHES);
                calculate_hash_functions(iterator->opd[0]->tile.arr_char + i*agg_elem_size, agg_elem_size, (uint8*)elem->agg.val_ptr); 
                elem->grp_hash = hash_value;                            
                elem->collision = *pprev;              
                *pprev = elem;                         

                if (++distinct_count >= threshold) {                    
                    imcs_hash_elem_t** table;                        
                    size_t new_hash_table_size = imcs_next_prime_number(hash_table_size);                     
                    size_t j;                                   
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    for (j = 0; j < hash_table_size; j++) {             
                        imcs_hash_elem_t* next;                      
                        for (elem = hash->table[j]; elem != NULL; elem = next) { 
                            imcs_hash_elem_t* ep;      
                            next = elem->collision;                     
                            pprev = &table[elem->grp_hash % new_hash_table_size];    
                            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                                for (ep = *pprev; ep != NULL && (diff = ep->grp.val_int64 - elem->grp.val_int64) < 0; ep = *(pprev = &ep->collision));
                            } else {                                                    
                                for (ep = *pprev; ep != NULL && (diff = memcmp(ep->grp.val_ptr, elem->grp.val_ptr, grp_elem_size)) < 0; ep = *(pprev = &ep->collision));
                            }
                            elem->collision = *pprev;
                            *pprev = elem;
                        }                                               
                    }                                                   
                    hash->table_size = hash_table_size = new_hash_table_size; 
                    imcs_free(hash->table);                          
                    hash->table = table;                                
                    threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; 
                }                                                       
            }                                                           
        }                                                               
    }                                                                   
    hash->table_used = distinct_count;
    return true;
}                                                                       
static void imcs_hash_merge_approxdc(imcs_iterator_h dst, imcs_iterator_h src) 
{                                                                       
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)dst->context; 
    imcs_hash_iterator_context_t* src_ctx  = (imcs_hash_iterator_context_t*)src->context; 
    imcs_hash_t* hash = ctx->private_hash;                           
    imcs_hash_t* src_hash = src_ctx->private_hash;                   
    size_t i, n;                                                    
    size_t grp_elem_size = dst->opd[1]->elem_size;                       
    imcs_hash_elem_t* elem;                                          
    imcs_hash_elem_t* next;                                          
    imcs_hash_elem_t* src_elem;                                      
    size_t distinct_count = hash->table_used;                       
    size_t hash_table_size = hash->table_size;                      
    size_t threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; 
                                                                        
    for (i = 0, n = src_hash->table_size; i < n; i++) {                 
        for (src_elem = src_hash->table[i]; src_elem != NULL; src_elem = next) { 
            uint32 hash_value = src_elem->grp_hash;                      
            uint32 hash_index = hash_value % hash_table_size;            
            imcs_hash_elem_t** pprev = &hash->table[hash_index];    
            int64 diff = 1;
            next = src_elem->collision;                                 
            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                for (elem = *pprev; elem != NULL && (diff = elem->grp.val_int64 - src_elem->grp.val_int64) < 0; elem = *(pprev = &elem->collision));
            } else {                                                    
                for (elem = *pprev; elem != NULL && (diff = memcmp(elem->grp.val_ptr, src_elem->grp.val_ptr, grp_elem_size)) < 0; elem = *(pprev = &elem->collision));
            } 
            if (diff == 0) {                                         
                merge_zero_bits((uint8*)elem->agg.val_ptr, (uint8*)src_elem->agg.val_ptr);
            } else {                                                    
                src_elem->collision = *pprev;
                *pprev = src_elem;                      
                if (++distinct_count >= threshold) {                    
                    imcs_hash_elem_t** table;                        
                    size_t new_hash_table_size = imcs_next_prime_number(hash_table_size);                     
                    size_t j;                                   
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    for (j = 0; j < hash_table_size; j++) {             
                        imcs_hash_elem_t* next;                      
                        for (elem = hash->table[j]; elem != NULL; elem = next) { 
                            imcs_hash_elem_t* ep;      
                            next = elem->collision;                     
                            pprev = &table[elem->grp_hash % new_hash_table_size];    
                            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                                for (ep = *pprev; ep != NULL && (diff = ep->grp.val_int64 - elem->grp.val_int64) < 0; ep = *(pprev = &ep->collision));
                            } else {                                                    
                                for (ep = *pprev; ep != NULL && (diff = memcmp(ep->grp.val_ptr, elem->grp.val_ptr, grp_elem_size)) < 0; ep = *(pprev = &ep->collision));
                            }
                            elem->collision = *pprev;
                            *pprev = elem;
                        }                                               
                    }                                                   
                    hash->table_size = hash_table_size = new_hash_table_size; 
                    imcs_free(hash->table);                          
                    hash->table = table;                                
                    threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; 
                }                                                                       
            }                                                           
        }                                                               
    }                                                                   
    hash->table_used = distinct_count;                                  
    ctx->shared->hash = hash;                                           
}                                                                       
static bool imcs_hash_approxdc_next_agg(imcs_iterator_h iterator) 
{                                                                       
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; 
    size_t this_tile_size = imcs_tile_size;                         
    size_t i;                                                       
    imcs_hash_elem_t* elem;                                          
    size_t chain_no;                                                
    size_t table_size;                                              
    imcs_hash_t* hash = ctx->shared->hash;                           
    if (!hash) {                                                        
        imcs_hash_initialize_approxdc(iterator); 
        hash = ctx->shared->hash = ctx->private_hash;                                       
    }                                                                   
    elem = ctx->curr_elem;                                              
    chain_no = ctx->chain_no;                                           
    table_size = hash->table_size;                                      
    for (i = 0; i < this_tile_size; i++) {                           
        while (elem == NULL && chain_no < table_size) {                    
            elem = hash->table[chain_no++];                        
        }                                                               
        if (elem != NULL) {                                                
            iterator->tile.arr_int64[i] = approximate_distinct_count((uint8*)elem->agg.val_ptr); 
            elem = elem->collision;                                     
        } else {                                                        
            break;                                                      
        }                                                               
    }                                                                   
    ctx->curr_elem = elem;                                              
    ctx->chain_no = chain_no;                                           
    iterator->tile_size = i;                                            
    iterator->next_pos += i;                                         
    return i != 0;                        
}                                                                       
static bool imcs_hash_approxdc_next_grp(imcs_iterator_h iterator) 
{                                                                       
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; 
    size_t this_tile_size = imcs_tile_size;                         \
    size_t i;                                                       
    imcs_hash_elem_t* elem;                                          
    size_t chain_no;                                                
    size_t table_size;                                              
    size_t grp_elem_size = iterator->elem_size;                  
    imcs_hash_t* hash = ctx->shared->hash;                           
    if (!hash) {                                                        
        imcs_hash_initialize_approxdc(iterator); 
        hash = ctx->shared->hash = ctx->private_hash;                                       
    }                                                                   
    elem = ctx->curr_elem;                                              
    chain_no = ctx->chain_no;                                           
    table_size = hash->table_size;                                      
    for (i = 0; i < this_tile_size; i++) {                           
        while (elem == NULL && chain_no < table_size) {                    
            elem = hash->table[chain_no++];                             
        }                                                               
        if (elem != NULL) {                                                
            memcpy(iterator->tile.arr_char + i*grp_elem_size, (grp_elem_size <= sizeof(imcs_key_t)) ? (char*)&elem->grp.val_int8 : elem->grp.val_ptr, grp_elem_size); 
            elem = elem->collision;                                     
        } else {                                                        
            break;                                                      
        }                                                               
    }                                                                   
    ctx->curr_elem = elem;                                              
    ctx->chain_no = chain_no;                                           
    iterator->tile_size = i;                                            
    iterator->next_pos += i;                                         
    return i != 0;                        
}                                                                       
void imcs_hash_approxdc(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by) 
{                                                                       
    imcs_iterator_h result_agg = imcs_new_iterator(sizeof(imcs_count_t), sizeof(imcs_hash_iterator_context_t)); 
    imcs_iterator_h result_grp = imcs_new_iterator(group_by->elem_size, sizeof(imcs_hash_iterator_context_t)); 
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)result_agg->context; 
    imcs_shared_hash_t* shared;                                      
    shared = (imcs_shared_hash_t*)imcs_alloc(sizeof(imcs_shared_hash_t)); 
    shared->hash = 0;                                                   

    ctx->n_groups = imcs_hash_table_init_size;
    ctx->shared = shared;                                               
    ctx->curr_elem = 0;                                                 
    ctx->chain_no = 0;                                                  
    result_agg->opd[0] = imcs_operand(input);                                           
    result_agg->opd[1] = imcs_operand(group_by);                                       
    result_agg->elem_type = TID_int64;               
    result_agg->next = imcs_hash_approxdc_next_agg;    
    result_agg->reset = imcs_hash_agg_reset;                         
    result_agg->prepare = imcs_hash_initialize_approxdc;     
    result_agg->merge = imcs_hash_merge_approxdc;            
                                                                        
    ctx = (imcs_hash_iterator_context_t*)result_grp->context;        
    ctx->n_groups = imcs_hash_table_init_size;
    ctx->shared = shared;                                               
    ctx->curr_elem = 0;                                                 
    ctx->chain_no = 0;                                                  
    result_grp->opd[0] = imcs_operand(input);                                           
    result_grp->opd[1] = imcs_operand(group_by);                                       
    result_grp->elem_type = group_by->elem_type;                        
    result_grp->next = imcs_hash_approxdc_next_grp;    
    result_grp->reset = imcs_hash_agg_reset;                         
    
    result[0] = result_agg;
    result[1] = result_grp;
}



typedef struct { 
    imcs_hash_iterator_context_t groups;
    size_t n_pairs;
    size_t min_occurrences;
    imcs_hash_t* agg_hash;
} imcs_dup_hash_iterator_context_t;


static bool imcs_dup_hash_initialize(imcs_iterator_h iterator) 
{                                                                       
    imcs_dup_hash_iterator_context_t* ctx = (imcs_dup_hash_iterator_context_t*)iterator->context; 
    imcs_hash_t* hash;
    imcs_hash_t* agg_hash;
    size_t i, tile_size;
    size_t agg_elem_size = iterator->opd[0]->elem_size; 
    size_t grp_elem_size = iterator->opd[1]->elem_size; 
    imcs_hash_elem_t* elem;                                          
    imcs_key_t grp_val;                                                
    imcs_key_t agg_val;                                                
    size_t grp_elems_basket_used = IMCS_HASH_BASKET_N_ELEMS;         
    size_t grp_keys_basket_used = IMCS_HASH_BASKET_SIZE;             
    size_t agg_elems_basket_used = IMCS_HASH_BASKET_N_ELEMS;         
    size_t agg_keys_basket_used = IMCS_HASH_BASKET_SIZE;             
    imcs_hash_basket_t* grp_elems_basket = 0;                            
    imcs_hash_basket_t* grp_keys_basket = 0;                             
    imcs_hash_basket_t* agg_elems_basket = 0;                            
    imcs_hash_basket_t* agg_keys_basket = 0;                             
    size_t hash_table_size = ctx->groups.n_groups; 
    size_t agg_hash_table_size = ctx->n_pairs; 
    size_t threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1;
    size_t agg_threshold = (size_t)(imcs_hash_table_load_factor*agg_hash_table_size)-1; 
    size_t distinct_count = 0;                                      
    size_t agg_distinct_count = 0;                                      

    hash = (imcs_hash_t*)imcs_alloc(sizeof(imcs_hash_t));
    hash->table = (imcs_hash_elem_t**)imcs_alloc(hash_table_size*sizeof(imcs_hash_elem_t*)); 
    memset(hash->table, 0, hash_table_size*sizeof(imcs_hash_elem_t*)); 
    hash->table_size = hash_table_size;
    ctx->groups.private_hash = hash;

    agg_hash = (imcs_hash_t*)imcs_alloc(sizeof(imcs_hash_t));
    agg_hash->table = (imcs_hash_elem_t**)imcs_alloc(agg_hash_table_size*sizeof(imcs_hash_elem_t*));
    memset(agg_hash->table, 0, agg_hash_table_size*sizeof(imcs_hash_elem_t*)); 
    agg_hash->table_size = agg_hash_table_size;
    ctx->agg_hash = agg_hash;

    hash->baskets = 0;                                             
    agg_hash->baskets = 0;                                             
    agg_hash->table_size = agg_hash_table_size;
    grp_val.val_int64 = 0;                                                   
    agg_val.val_int64 = 0;                                                   

    while (iterator->opd[1]->next(iterator->opd[1])) {   
        if (!iterator->opd[0]->next(iterator->opd[0]) || iterator->opd[0]->tile_size != iterator->opd[1]->tile_size) { 
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("group be sequence doesn't match values sequence")))); 
        }
        tile_size = iterator->opd[1]->tile_size; 
        for (i = 0; i < tile_size; i++) {      
            uint32 grp_hash_value = murmur_hash3_32(iterator->opd[1]->tile.arr_char + i*grp_elem_size, grp_elem_size, MURMUR_SEED); 
            uint32 agg_hash_value = murmur_hash3_32(iterator->opd[0]->tile.arr_char + i*agg_elem_size, agg_elem_size, grp_hash_value);         
            uint32 grp_index = grp_hash_value % hash_table_size;
            uint32 agg_index = agg_hash_value % agg_hash_table_size;         
            imcs_hash_elem_t* agg_elem = 0;    
            imcs_hash_elem_t** pprev = &hash->table[grp_index];    
            int64 diff = 1;
            if (agg_elem_size <= sizeof(imcs_key_t)) { 
                memcpy(&agg_val.val_int8, iterator->opd[0]->tile.arr_char + i*agg_elem_size, agg_elem_size); 
            } else { 
                agg_val.val_ptr = iterator->opd[0]->tile.arr_char + i*agg_elem_size;
            }
            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                memcpy(&grp_val.val_int8, iterator->opd[1]->tile.arr_char + i*grp_elem_size, grp_elem_size); 
                for (elem = *pprev; elem != NULL && (diff = elem->grp.val_int64 - grp_val.val_int64) < 0; elem = *(pprev = &elem->collision));
            } else {                                                    
                grp_val.val_ptr = iterator->opd[1]->tile.arr_char + i*grp_elem_size; 
                for (elem = *pprev; elem != NULL && (diff = memcmp(elem->grp.val_ptr, grp_val.val_ptr, grp_elem_size)) < 0; elem = *(pprev = &elem->collision));
            } 
            if (diff != 0) {                                         
                if (grp_elems_basket_used == IMCS_HASH_BASKET_N_ELEMS) {          
                    grp_elems_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                    grp_elems_basket->next = hash->baskets;                  
                    hash->baskets = grp_elems_basket;                        
                    grp_elems_basket_used = 0;                                    
                }             
                elem = &grp_elems_basket->u.elems[grp_elems_basket_used++];       
                if (grp_elem_size <= sizeof(imcs_key_t)) {               
                    elem->grp.val_int64 = grp_val.val_int64;                  
                } else {                                                
                    if (grp_keys_basket_used + grp_elem_size > IMCS_HASH_BASKET_SIZE) { 
                        grp_keys_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                        grp_keys_basket->next = hash->baskets;              
                        hash->baskets = grp_keys_basket;                    
                        grp_keys_basket_used = 0;                           
                    }                                                   
                    elem->grp.val_ptr = &grp_keys_basket->u.keys[grp_keys_basket_used]; 
                    grp_keys_basket_used += grp_elem_size;                      
                    memcpy(elem->grp.val_ptr, grp_val.val_ptr, grp_elem_size); 
                }                                                       
                elem->count = 0;
                elem->grp_hash = grp_hash_value;
                elem->collision = *pprev;
                *pprev = elem;                             

                if (++distinct_count >= threshold) {                    
                    imcs_hash_elem_t** table;                         
                    size_t new_hash_table_size = imcs_next_prime_number(hash_table_size);                     
                    size_t j;                                   
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    for (j = 0; j < hash_table_size; j++) {             
                        imcs_hash_elem_t* next;                      
                        for (elem = hash->table[j]; elem != NULL; elem = next) { 
                            imcs_hash_elem_t* ep;      
                            next = elem->collision;                     
                            pprev = &table[elem->grp_hash % new_hash_table_size];    
                            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                                for (ep = *pprev; ep != NULL && (diff = ep->grp.val_int64 - elem->grp.val_int64) < 0; ep = *(pprev = &ep->collision));
                            } else {                                                    
                                for (ep = *pprev; ep != NULL && (diff = memcmp(ep->grp.val_ptr, elem->grp.val_ptr, grp_elem_size)) < 0; ep = *(pprev = &ep->collision));
                            }
                            elem->collision = *pprev;
                            *pprev = elem;
                        }                                               
                    }                                                   
                    hash->table_size = hash_table_size = new_hash_table_size; 
                    imcs_free(hash->table);                          
                    hash->table = table;                                
                    threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; 
                }                                                                        
            } else {    
                if (agg_elem_size > sizeof(imcs_key_t) && grp_elem_size > sizeof(imcs_key_t)) { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg_hash != agg_hash_value || memcmp(agg_elem->agg.val_ptr, agg_val.val_ptr, agg_elem_size) != 0 || memcmp(agg_elem->grp.val_ptr, grp_val.val_ptr, grp_elem_size) != 0); 
                         agg_elem = agg_elem->collision);
                } else if (agg_elem_size > sizeof(imcs_key_t) && grp_elem_size <= sizeof(imcs_key_t)) { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg_hash != agg_hash_value || memcmp(agg_elem->agg.val_ptr, agg_val.val_ptr, agg_elem_size) != 0 || agg_elem->grp.val_int64 != grp_val.val_int64); 
                         agg_elem = agg_elem->collision);
                } else if (agg_elem_size <= sizeof(imcs_key_t) && grp_elem_size > sizeof(imcs_key_t)) { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg_hash != agg_hash_value || agg_elem->agg.val_int64 != agg_val.val_int64 || memcmp(agg_elem->grp.val_ptr, grp_val.val_ptr, grp_elem_size) != 0); 
                         agg_elem = agg_elem->collision);
                } else { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg.val_int64 != agg_val.val_int64 || agg_elem->grp.val_int64 != grp_val.val_int64); 
                         agg_elem = agg_elem->collision);
                }
            }
            if (agg_elem == NULL) {
                if (agg_elems_basket_used == IMCS_HASH_BASKET_N_ELEMS) {          
                    agg_elems_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                    agg_elems_basket->next = agg_hash->baskets;                  
                    agg_hash->baskets = agg_elems_basket;                        
                    agg_elems_basket_used = 0;                                    
                }                                                       
                if (++agg_distinct_count >= agg_threshold) {                    
                    imcs_hash_elem_t** table;                         
                    size_t new_hash_table_size = imcs_next_prime_number(hash_table_size);                     
                    size_t j;                                   
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    for (j = 0; j < agg_hash_table_size; j++) {             
                        imcs_hash_elem_t* next;                      
                        imcs_hash_elem_t* curr;                      
                        for (curr = agg_hash->table[j]; curr != NULL; curr = next) { 
                            uint32 eh = curr->agg_hash % new_hash_table_size; 
                            next = curr->collision;                     
                            curr->collision = table[eh];                
                            table[eh] = curr;                           
                        }                                               
                    }                                                   
                    agg_hash->table_size = agg_hash_table_size = new_hash_table_size; 
                    imcs_free(agg_hash->table);                          
                    agg_hash->table = table;                                
                    agg_threshold = (size_t)(imcs_hash_table_load_factor*agg_hash_table_size)-1; 
                    agg_index = agg_hash_value % agg_hash_table_size;   
                }                                                                        
                agg_elem = &agg_elems_basket->u.elems[agg_elems_basket_used++];       
                if (agg_elem_size <= sizeof(imcs_key_t)) {               
                    agg_elem->agg.val_int64 = agg_val.val_int64;                  
                } else {                                                
                    if (agg_keys_basket_used + agg_elem_size > IMCS_HASH_BASKET_SIZE) { 
                        agg_keys_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                        agg_keys_basket->next = agg_hash->baskets;              
                        agg_hash->baskets = agg_keys_basket;                    
                        agg_keys_basket_used = 0;                           
                    }                                                   
                    agg_elem->agg.val_ptr = &agg_keys_basket->u.keys[agg_keys_basket_used]; 
                    agg_keys_basket_used += agg_elem_size;                      
                    memcpy(agg_elem->agg.val_ptr, agg_val.val_ptr, agg_elem_size); 
                }                                                       
                if (grp_elem_size <= sizeof(imcs_key_t)) {               
                    agg_elem->grp.val_int64 = grp_val.val_int64;                  
                } else {                                                
                    if (agg_keys_basket_used + grp_elem_size > IMCS_HASH_BASKET_SIZE) { 
                        agg_keys_basket = (imcs_hash_basket_t*)imcs_alloc(sizeof(imcs_hash_basket_t)); 
                        agg_keys_basket->next = agg_hash->baskets;              
                        agg_hash->baskets = agg_keys_basket;                    
                        agg_keys_basket_used = 0;                           
                    }                                                   
                    agg_elem->grp.val_ptr = &agg_keys_basket->u.keys[agg_keys_basket_used]; 
                    agg_keys_basket_used += grp_elem_size;                      
                    memcpy(agg_elem->grp.val_ptr, grp_val.val_ptr, grp_elem_size); 
                }                                                       
                agg_elem->agg_hash = agg_hash_value;
                agg_elem->grp_hash = grp_hash_value;
                agg_elem->count = 0;
                agg_elem->collision = agg_hash->table[agg_index];                  
                agg_hash->table[agg_index] = agg_elem;                             
            }
            if (++agg_elem->count == ctx->min_occurrences) { 
                elem->count += 1;
            }                
        }                                                               
    }  
    agg_hash->table_used = agg_distinct_count;
    hash->table_used = distinct_count;
    return true;
}                       
                                                
static void imcs_dup_hash_merge(imcs_iterator_h dst, imcs_iterator_h src) 
{                                                                       
    imcs_dup_hash_iterator_context_t* ctx = (imcs_dup_hash_iterator_context_t*)dst->context; 
    imcs_dup_hash_iterator_context_t* src_ctx = (imcs_dup_hash_iterator_context_t*)src->context; 
    imcs_hash_t* hash = ctx->groups.private_hash;                                   
    imcs_hash_t* agg_hash = ctx->agg_hash;
    imcs_hash_t* src_agg_hash = src_ctx->agg_hash;                                   
    imcs_hash_t* src_hash = src_ctx->groups.private_hash;                                   
    size_t i, n;
    size_t agg_elem_size = dst->opd[0]->elem_size; 
    size_t grp_elem_size = dst->opd[1]->elem_size; 
    imcs_hash_elem_t* elem;                                          
    imcs_hash_elem_t* next;                                          
    imcs_hash_elem_t* src_elem;
    size_t hash_table_size = hash->table_size; 
    size_t agg_hash_table_size = agg_hash->table_size; 
    size_t threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1;
    size_t agg_threshold = (size_t)(imcs_hash_table_load_factor*agg_hash_table_size)-1; 
    size_t distinct_count = hash->table_used;                                      
    size_t agg_distinct_count = agg_hash->table_used;                                      
    
    for (i = 0, n = src_agg_hash->table_size; i < n; i++) { 
        for (src_elem = src_agg_hash->table[i]; src_elem != NULL; src_elem = next) { 
            uint32 grp_hash_value = src_elem->grp_hash;
            uint32 agg_hash_value = src_elem->agg_hash;
            uint32 grp_index = grp_hash_value % hash_table_size;
            uint32 agg_index = agg_hash_value % agg_hash_table_size;         
            imcs_hash_elem_t* agg_elem = NULL;    
            imcs_hash_elem_t** pprev = &hash->table[grp_index];    
            int64 diff = 1;

            next = src_elem->collision;
            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                for (elem = *pprev; elem != NULL && (diff = elem->grp.val_int64 - src_elem->grp.val_int64) < 0; elem = *(pprev = &elem->collision));
            } else {                                                    
                for (elem = *pprev; elem != NULL && (diff = memcmp(elem->grp.val_ptr, src_elem->grp.val_ptr, grp_elem_size)) < 0; elem = *(pprev = &elem->collision));
            } 
            if (diff != 0) {                                         
                elem = src_hash->table[grp_hash_value % src_hash->table_size]; /* resuse element from source merged table */
                src_hash->table[grp_hash_value % src_hash->table_size] = elem->collision;
                elem->grp_hash = grp_hash_value;
                elem->grp = src_elem->grp;
                elem->count = 0;
                elem->collision = *pprev;
                *pprev = elem;                                             

                if (++distinct_count >= threshold) {                    
                    imcs_hash_elem_t** table;                         
                    size_t new_hash_table_size = imcs_next_prime_number(hash_table_size);                     
                    size_t j;                                   
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    for (j = 0; j < hash_table_size; j++) {             
                        imcs_hash_elem_t* next;                      
                        for (elem = hash->table[j]; elem != NULL; elem = next) { 
                            imcs_hash_elem_t* ep;      
                            next = elem->collision;                     
                            pprev = &table[elem->grp_hash % new_hash_table_size];    
                            if (grp_elem_size <= sizeof(imcs_key_t)) {                   
                                for (ep = *pprev; ep != NULL && (diff = ep->grp.val_int64 - elem->grp.val_int64) < 0; ep = *(pprev = &ep->collision));
                            } else {                                                    
                                for (ep = *pprev; ep != NULL && (diff = memcmp(ep->grp.val_ptr, elem->grp.val_ptr, grp_elem_size)) < 0; ep = *(pprev = &ep->collision));
                            }
                            elem->collision = *pprev;
                            *pprev = elem;
                        }                                               
                    }                                                   
                    hash->table_size = hash_table_size = new_hash_table_size; 
                    imcs_free(hash->table);                          
                    hash->table = table;                                
                    threshold = (size_t)(imcs_hash_table_load_factor*hash_table_size)-1; 
                }                                                                        
            } else {    
                if (agg_elem_size > sizeof(imcs_key_t) && grp_elem_size > sizeof(imcs_key_t)) { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg_hash != agg_hash_value
                                              || memcmp(agg_elem->agg.val_ptr, src_elem->agg.val_ptr, agg_elem_size) != 0 
                                              || memcmp(agg_elem->grp.val_ptr, src_elem->grp.val_ptr, grp_elem_size) != 0); 
                         agg_elem = agg_elem->collision);
                } else if (agg_elem_size > sizeof(imcs_key_t) && grp_elem_size <= sizeof(imcs_key_t)) { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg_hash != agg_hash_value 
                                              || memcmp(agg_elem->agg.val_ptr, src_elem->agg.val_ptr, agg_elem_size) != 0 
                                              || agg_elem->grp.val_int64 != src_elem->grp.val_int64); 
                         agg_elem = agg_elem->collision);
                } else if (agg_elem_size <= sizeof(imcs_key_t) && grp_elem_size > sizeof(imcs_key_t)) { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg_hash != agg_hash_value 
                                              || agg_elem->agg.val_int64 != src_elem->agg.val_int64 
                                              || memcmp(agg_elem->grp.val_ptr, src_elem->grp.val_ptr, grp_elem_size) != 0); 
                         agg_elem = agg_elem->collision);
                } else { 
                    for (agg_elem = agg_hash->table[agg_index]; 
                         agg_elem != NULL && (agg_elem->agg.val_int64 != src_elem->agg.val_int64 || agg_elem->grp.val_int64 != src_elem->grp.val_int64); 
                         agg_elem = agg_elem->collision);
                }
            }
            if (agg_elem == NULL) {
                if (++agg_distinct_count >= agg_threshold) {                    
                    imcs_hash_elem_t** table;                         
                    size_t new_hash_table_size = imcs_next_prime_number(agg_hash_table_size);                     
                    size_t j;                                   
                    table = (imcs_hash_elem_t**)imcs_alloc(new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    memset(table, 0, new_hash_table_size*sizeof(imcs_hash_elem_t*)); 
                    for (j = 0; j < agg_hash_table_size; j++) {             
                        imcs_hash_elem_t* next;                      
                        imcs_hash_elem_t* curr;                      
                        for (curr = agg_hash->table[j]; curr != NULL; curr = next) { 
                            uint32 eh = curr->agg_hash % new_hash_table_size; 
                            next = curr->collision;                     
                            curr->collision = table[eh];                
                            table[eh] = curr;                           
                        }                                               
                    }                                                   
                    agg_hash->table_size = agg_hash_table_size = new_hash_table_size; 
                    imcs_free(agg_hash->table);                          
                    agg_hash->table = table;                                
                    agg_threshold = (size_t)(imcs_hash_table_load_factor*agg_hash_table_size)-1; 
                    agg_index = agg_hash_value % agg_hash_table_size;   
                }                                                                        
                agg_elem = src_elem;
                if (agg_elem->count >= ctx->min_occurrences) { 
                    elem->count += 1;
                }                
                agg_elem->collision = agg_hash->table[agg_index];                  
                agg_hash->table[agg_index] = agg_elem;                             
            } else { 
                if (agg_elem->count < ctx->min_occurrences && agg_elem->count + src_elem->count >= ctx->min_occurrences) { 
                    elem->count += 1;
                }                
                agg_elem->count += src_elem->count;                                    
            }
        }                                                               
    }  
    agg_hash->table_used = agg_distinct_count;
    hash->table_used = distinct_count;
    ctx->groups.shared->hash = hash;    
}                       
                                                
static bool imcs_dup_hash_next_count(imcs_iterator_h iterator) 
{                                                                       
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; /* corresponds to imcs_dup_hash_iterator_context_t::groups */
    size_t this_tile_size = imcs_tile_size;                         
    size_t i;                                            
    imcs_hash_elem_t* elem;  
    size_t chain_no;   
    size_t table_size;   
    imcs_hash_t* hash = ctx->shared->hash; 
    if (!hash) {
        imcs_dup_hash_initialize(iterator); 
        hash = ctx->shared->hash = ctx->private_hash;                                                                 
    }                                                                   
    elem = ctx->curr_elem;
    chain_no = ctx->chain_no;
    table_size = hash->table_size;
    for (i = 0; i < this_tile_size; i++) {                           
        while (elem == NULL && chain_no < table_size) {    
            elem = hash->table[chain_no++];                   
        }                                                               
        if (elem != NULL) {                                                
            iterator->tile.arr_int64[i] = elem->count;
            elem = elem->collision;
        } else {                                                        
            break;                                                      
        }                                                               
    }                                                                   
    ctx->curr_elem = elem;      
    ctx->chain_no = chain_no;                                         
    iterator->tile_size = i;                                            
    iterator->next_pos += i;                                         
    return i != 0;
}
                                                                       
static bool imcs_dup_hash_next_grp(imcs_iterator_h iterator) 
{                                                                       
    imcs_hash_iterator_context_t* ctx = (imcs_hash_iterator_context_t*)iterator->context; /* corresponds to imcs_dup_hash_iterator_context_t::groups */
    size_t this_tile_size = imcs_tile_size;                         
    size_t i;                                            
    imcs_hash_elem_t* elem;  
    size_t chain_no;   
    size_t table_size;   
    size_t grp_elem_size = iterator->elem_size; 
    imcs_hash_t* hash = ctx->shared->hash; 
    if (!hash) {
        imcs_dup_hash_initialize(iterator); 
        hash = ctx->shared->hash = ctx->private_hash;                                                                 
    }                                                                   
    elem = ctx->curr_elem;
    chain_no = ctx->chain_no;
    table_size = hash->table_size;
    for (i = 0; i < this_tile_size; i++) {                           
        while (elem == NULL && chain_no < table_size) {    
            elem = hash->table[chain_no++];                   
        }                                                               
        if (elem != NULL) {                                                
            memcpy(iterator->tile.arr_char + i*grp_elem_size, (grp_elem_size <= sizeof(imcs_key_t)) ? (char*)&elem->grp.val_int8 : elem->grp.val_ptr, grp_elem_size); 
            elem = elem->collision;
        } else {                                                        
            break;                                                      
        }                                                               
    }                                                                   
    ctx->curr_elem = elem;                                              
    ctx->chain_no = chain_no;                                         
    iterator->tile_size = i;                                            
    iterator->next_pos += i;                                         
    return i != 0;
}                                                                       

void imcs_hash_dup_count(imcs_iterator_h result[2], imcs_iterator_h input, imcs_iterator_h group_by, size_t min_occurrences)
{                                                                       
    imcs_iterator_h result_cnt = imcs_new_iterator(sizeof(imcs_count_t), sizeof(imcs_dup_hash_iterator_context_t)); 
    imcs_iterator_h result_grp = imcs_new_iterator(group_by->elem_size, sizeof(imcs_dup_hash_iterator_context_t)); 
    imcs_dup_hash_iterator_context_t* ctx = (imcs_dup_hash_iterator_context_t*)result_cnt->context; 
    imcs_shared_hash_t* shared;

    shared = (imcs_shared_hash_t*)imcs_alloc(sizeof(imcs_shared_hash_t)); 
    shared->hash = 0;                                                               

    ctx->groups.curr_elem = 0;                                                 
    ctx->groups.chain_no = 0; 
    ctx->groups.n_groups = imcs_hash_table_init_size;
    ctx->groups.shared = shared; 
    ctx->n_pairs = imcs_hash_table_init_size;    
    ctx->min_occurrences = min_occurrences;
    result_cnt->opd[0] = imcs_operand(input);                                           
    result_cnt->opd[1] = imcs_operand(group_by);                                       
    result_cnt->elem_type = TID_int64;               
    result_cnt->next = imcs_dup_hash_next_count;    
    result_cnt->reset = imcs_hash_agg_reset;                         
    result_cnt->prepare =  imcs_dup_hash_initialize;                                             
    result_cnt->merge = imcs_dup_hash_merge;                                               
                                                                        
    ctx = (imcs_dup_hash_iterator_context_t*)result_grp->context;        
    ctx->groups.curr_elem = 0;                                                 
    ctx->groups.chain_no = 0;                                                  
    ctx->groups.n_groups = imcs_hash_table_init_size;
    ctx->groups.shared = shared; 
    ctx->n_pairs = imcs_hash_table_init_size;
    ctx->min_occurrences = min_occurrences;
    result_grp->opd[0] = imcs_operand(input);                                           
    result_grp->opd[1] = imcs_operand(group_by);                                       
    result_grp->elem_type = group_by->elem_type;                        
    result_grp->next = imcs_dup_hash_next_grp;    
    result_grp->reset = imcs_hash_agg_reset;                         

    result[0] = result_cnt;
    result[1] = result_grp;
}

typedef struct { 
    FunctionCallInfoData fcinfo;
    FmgrInfo flinfo;
} imcs_call_context_t;

#define IMCS_CALL_DEF(RET_TYPE, PG_RET_TYPE, ARG_TYPE, PG_ARG_TYPE) \
static bool imcs_##RET_TYPE##_call_##ARG_TYPE##_next(imcs_iterator_h iterator) \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_call_context_t* ctx = (imcs_call_context_t*)iterator->context; \
    FunctionCallInfoData* fcinfo = &ctx->fcinfo;                        \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    for (i = 0; i < tile_size; i++) {                                   \
        fcinfo->arg[0] = PG_ARG_TYPE##GetDatum(iterator->opd[0]->tile.arr_##ARG_TYPE[i]); \
        iterator->tile.arr_##RET_TYPE[i] = DatumGet##PG_RET_TYPE(FunctionCallInvoke(fcinfo)); \
        if (fcinfo->isnull) {                                           \
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), (errmsg("function returns null")))); \
        }                                                               \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos += tile_size;                                    \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_##RET_TYPE##_call_##ARG_TYPE(imcs_iterator_h input, Oid funcid) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(RET_TYPE), sizeof(FunctionCallInfoData)); \
    imcs_call_context_t* ctx = (imcs_call_context_t*)result->context;   \
    IMCS_CHECK_TYPE(input->elem_type, TID_##ARG_TYPE);                  \
    result->elem_type = TID_##RET_TYPE;                                 \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_##RET_TYPE##_call_##ARG_TYPE##_next;            \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    fmgr_info(funcid, &ctx->flinfo);                                    \
    InitFunctionCallInfoData(ctx->fcinfo, &ctx->flinfo, 1, 0, 0, 0);    \
    ctx->fcinfo.argnull[0] = false;                                     \
    return result;                                                      \
}

#define IMCS_CALLS_DEF(RET_TYPE, PG_RET_TYPE)                           \
    IMCS_CALL_DEF(RET_TYPE, PG_RET_TYPE, int8, Char)                    \
    IMCS_CALL_DEF(RET_TYPE, PG_RET_TYPE, int16, Int16)                  \
    IMCS_CALL_DEF(RET_TYPE, PG_RET_TYPE, int32, Int32)                  \
    IMCS_CALL_DEF(RET_TYPE, PG_RET_TYPE, int64, Int64)                  \
    IMCS_CALL_DEF(RET_TYPE, PG_RET_TYPE, float, Float4)                 \
    IMCS_CALL_DEF(RET_TYPE, PG_RET_TYPE, double, Float8) 

IMCS_CALLS_DEF(int8, Char) 
IMCS_CALLS_DEF(int16, Int16) 
IMCS_CALLS_DEF(int32, Int32) 
IMCS_CALLS_DEF(int64, Int64) 
IMCS_CALLS_DEF(float, Float4)
IMCS_CALLS_DEF(double, Float8)

#define MATCH_ANY_ONE_CHAR  '?'
#define MATCH_ANY_SUBSTRING '%'
#define ESCAPE_CHAR         '\\'

typedef struct { 
    char const* pattern;
} imcs_like_context_t;


/* We use own implementation of string match instead of PostgreSQL textlike and texticlike functions 
 * because:
 * - them are 1/3 faster: 150 vs 240 msec for performing 10 million case sensitive matches.
 * - testiclike is no reetntrant: it used palloc. And in PostgreSQL it is more than 3 times slower than 
 *   case sensitive matches, while in our implementation the diffirence is much smaller - 40%
 * - character elements of timeseries are expected to be ASCII.
 */
inline bool imcs_match_string(char const* str, size_t str_len, char const* pattern) 
{
    char const* wildcard = NULL;
    char const* strpos = NULL;
    char const* end  = str + str_len;
    bool value;
    while (true) {
        if (*pattern == MATCH_ANY_SUBSTRING) {
            wildcard = ++pattern;
            strpos = str;
        } else if (str == end || *str == '\0') {
            value = (*pattern == '\0');
            break;
        } else if (*pattern == ESCAPE_CHAR && pattern[1] == *str) {
            str += 1;
            pattern += 2;
        } else if (*pattern != ESCAPE_CHAR
                   && (*str == *pattern || *pattern == MATCH_ANY_ONE_CHAR))
        {
            str += 1;
            pattern += 1;
        } else if (wildcard) {
            str = ++strpos;
            pattern = wildcard;
        } else {
            value = false;
            break;
        }
    }
    return value;
}

static bool imcs_like_next(imcs_iterator_h iterator)       
{                                                                       
    size_t i, tile_size;                                                
    int elem_size = iterator->opd[0]->elem_size;
    imcs_like_context_t* ctx = (imcs_like_context_t*)iterator->context; 
    char* txt;
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    
        return false;                                                   
    } 
    txt = iterator->opd[0]->tile.arr_char;                                                                
    tile_size = iterator->opd[0]->tile_size;                            
    for (i = 0; i < tile_size; i++, txt += elem_size) {     
        iterator->tile.arr_int8[i] = imcs_match_string(txt, elem_size, ctx->pattern);
    }                                                                   
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                    
    return true;                                                        
}                                                                       
 
imcs_iterator_h imcs_like(imcs_iterator_h input, char const* pattern)
{
    imcs_iterator_h result = imcs_new_iterator(sizeof(int8), sizeof(imcs_like_context_t)); 
    imcs_like_context_t* ctx = (imcs_like_context_t*)result->context;   
    IMCS_CHECK_TYPE(input->elem_type, TID_char);                  
    result->elem_type = TID_int8;                                 
    result->opd[0] = imcs_operand(input);                               
    result->next = imcs_like_next;            
    result->flags = FLAG_CONTEXT_FREE;                                  
    ctx->pattern = pattern;
    return result;                                                      
}


inline bool imcs_match_string_ignore_case(char const* s, size_t slen, char const* p) 
{
    unsigned char const* str = (unsigned char const*)s;
    unsigned char const* pattern = (unsigned char const*)p;
    unsigned char const* wildcard = NULL;
    unsigned char const* strpos = NULL;
    unsigned char const* end  = str + slen;
    bool value;
    while (true) {
        if (*pattern == MATCH_ANY_SUBSTRING) {
            wildcard = ++pattern;
            strpos = str;
        } else if (str == end || *str == '\0') {
            value = (*pattern == '\0');
            break;
        } else if (*pattern == ESCAPE_CHAR && pattern[1] == tolower(*str)) {
            str += 1;
            pattern += 2;
        } else if (*pattern != ESCAPE_CHAR
                   && (tolower(*str) == *pattern || *pattern == MATCH_ANY_ONE_CHAR))
        {
            str += 1;
            pattern += 1;
        } else if (wildcard) {
            str = ++strpos;
            pattern = wildcard;
        } else {
            value = false;
            break;
        }
    }
    return value;
}

static bool imcs_ilike_next(imcs_iterator_h iterator)       
{                                                                       
    size_t i, tile_size;                                                
    int elem_size = iterator->opd[0]->elem_size;
    imcs_like_context_t* ctx = (imcs_like_context_t*)iterator->context; 
    char* txt;
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    
        return false;                                                   
    } 
    txt = iterator->opd[0]->tile.arr_char;                                                                
    tile_size = iterator->opd[0]->tile_size;                            
    for (i = 0; i < tile_size; i++, txt += elem_size) {     
        iterator->tile.arr_int8[i] = imcs_match_string_ignore_case(txt, elem_size, ctx->pattern);
    }                                                                   
    iterator->tile_size = tile_size;                                    
    iterator->next_pos += tile_size;                                    
    return true;                                                        
}                                                                       
 
imcs_iterator_h imcs_ilike(imcs_iterator_h input, char const* pattern)
{
    imcs_iterator_h result = imcs_new_iterator(sizeof(int8), sizeof(imcs_like_context_t)); 
    imcs_like_context_t* ctx = (imcs_like_context_t*)result->context;   
    unsigned char* pat = (unsigned char*)pattern;
    IMCS_CHECK_TYPE(input->elem_type, TID_char);                  
    result->elem_type = TID_int8;                                 
    result->opd[0] = imcs_operand(input);                               
    result->next = imcs_ilike_next;            
    result->flags = FLAG_CONTEXT_FREE;                                  
    ctx->pattern = pattern;
    while (*pat != '\0') { 
        *pat = tolower(*pat);
        pat += 1;
    }
    return result;                                                      
}

#define IMCS_JOIN_TS_DEF(TYPE)                                          \
static bool imcs_join_unsorted_##TYPE##_next(imcs_iterator_h iterator)  \
{                                                                       \
    size_t i, tile_size;                                                \
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)iterator->context; \
    imcs_page_t* root_page = ctx->stack[0].page;                        \
    imcs_pos_t next_pos = iterator->next_pos;                           \
    if (!iterator->opd[0]->next(iterator->opd[0])) {                    \
        return false;                                                   \
    }                                                                   \
    tile_size = iterator->opd[0]->tile_size;                            \
    if (ctx->direction < 0) {                                           \
        for (i = 0; i < tile_size; i++) {                               \
            iterator->next_pos = 0;                                     \
            if (!imcs_search_page_##TYPE(root_page, iterator, iterator->opd[0]->tile.arr_##TYPE[i], BOUNDARY_EXCLUSIVE, 0) || iterator->next_pos == 0) { \
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no matching timestamp in timeseries"))); \
            }                                                           \
            iterator->tile.arr_int64[i] = iterator->next_pos-1;         \
        }                                                               \
    } else {                                                            \
        int boundary = ctx->direction == 0 ? BOUNDARY_EXACT : BOUNDARY_INCLUSIVE; \
        for (i = 0; i < tile_size; i++) {                               \
            iterator->next_pos = 0;                                     \
            if (!imcs_search_page_##TYPE(root_page, iterator, iterator->opd[0]->tile.arr_##TYPE[i], boundary, 0)) { \
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no matching timestamp in timeseries"))); \
            }                                                           \
            iterator->tile.arr_int64[i] = iterator->next_pos;           \
        }                                                               \
    }                                                                   \
    iterator->tile_size = tile_size;                                    \
    iterator->next_pos = next_pos + tile_size;                          \
    return true;                                                        \
}                                                                       \
                                                                        \
imcs_iterator_h imcs_join_unsorted_##TYPE(imcs_timeseries_t* ts, imcs_iterator_h input, int direction) \
{                                                                       \
    imcs_iterator_h result = imcs_new_iterator(sizeof(int64), sizeof(imcs_iterator_context_t)); \
    imcs_iterator_context_t* ctx = (imcs_iterator_context_t*)result->context; \
    IMCS_CHECK_TYPE(input->elem_type, TID_##TYPE);                      \
    result->elem_type = TID_int64;                                      \
    result->opd[0] = imcs_operand(input);                               \
    result->next = imcs_join_unsorted_##TYPE##_next;                    \
    result->flags = FLAG_CONTEXT_FREE;                                  \
    ctx->direction = direction;                                         \
    ctx->stack[0].page = ts->root_page;                                 \
    return result;                                                      \
}

IMCS_JOIN_TS_DEF(int8)
IMCS_JOIN_TS_DEF(int16)
IMCS_JOIN_TS_DEF(int32)
IMCS_JOIN_TS_DEF(int64)
IMCS_JOIN_TS_DEF(float)
IMCS_JOIN_TS_DEF(double)
