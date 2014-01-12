#ifndef __FILEIO_H__
#define __FILEIO_H__

#include <stddef.h>
#include <stdlib.h>

/**
 * File IO: access to file system
 * I can not use PostgreSQL fd.h, because I need reentrant read/write
 */

#ifdef _WIN32
typedef void* imcs_file_h;
#else
typedef int imcs_file_h;
#endif

imcs_file_h imcs_file_open(char const* path);
bool imcs_file_read(imcs_file_h file, void* buf, size_t size, off_t pos);
void imcs_file_write(imcs_file_h file, void const* buf, size_t size, off_t pos);
void imcs_file_close(imcs_file_h file);

#endif
