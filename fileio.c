#include "imcs.h"
#include "fileio.h"

#ifdef _WIN32

imcs_file_h imcs_file_open(char const* path)
{
    HANDLE h = CreateFile(path, GENERIC_READ | GENERIC_WRITE,
                          FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 0, NULL);

    if (h == INVALID_HANDLE_VALUE) { 
        imcs_ereport(ERRCODE_UNDEFINED_FILE, "Failed to open file '%s': %d", path, GetLastError());
    }
    return h;
}

bool imcs_file_read(imcs_file_h file, void* buf, size_t size, off_t pos)
{
    DWORD readBytes = 0;
    OVERLAPPED Overlapped;
    Overlapped.Offset = (DWORD)LO_32(pos);
    Overlapped.OffsetHigh = (DWORD)HI_32(pos);
    Overlapped.hEvent = NULL;
    if (ReadFile(file, buf, size, &readBytes, &Overlapped))
    {
        if (readBytes != size) { 
            imcs_ereport(ERRCODE_IO_ERROR, "Read %d bytes from file instead of %ld", readBytes, size);
        }
    }
    else
    {
        int rc = GetLastError();
        if (rc != ERROR_HANDLE_EOF) { 
            imcs_ereport(ERRCODE_IO_ERROR, "File read failed: %d", GetLastError());
        }
    }
    return readBytes != 0;
}

    
void imcs_file_write(imcs_file_h file, void const* buf, size_t size, off_t pos)
{
    DWORD writtenBytes;
    OVERLAPPED Overlapped;
    Overlapped.Offset = (DWORD)LO_32(pos);
    Overlapped.OffsetHigh = (DWORD)HI_32(pos);
    Overlapped.hEvent = NULL;
    if (!WriteFile(file, buf, size, &writtenBytes, &Overlapped))
    {
        imcs_ereport(ERRCODE_IO_ERROR, "File write failed: %d", GetLastError());
    }
    else if (writtenBytes != size)
    {
        imcs_ereport(ERRCODE_IO_ERROR, "Write %d bytes from file instead of %ld", writtenBytes, size);
    }
}

void   imcs_file_close(imcs_file_h file)
{
    if (!CloseHandle(file)) {
        imcs_ereport(ERRCODE_IO_ERROR, "File close failed: %d", GetLastError());
    }
}

#else

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

#ifndef O_LARGEFILE
    #define O_LARGEFILE 0
#endif

imcs_file_h imcs_file_open(char const* path)
{
    int rc = open(path, O_LARGEFILE | O_RDWR | O_CREAT, 0600);
    if (rc < 0) { 
        imcs_ereport(ERRCODE_UNDEFINED_FILE, "Failed to open file '%s': %d", path, errno);
    }
    return rc;
}

bool imcs_file_read(imcs_file_h file, void* buf, size_t size, off_t pos)
{
    ssize_t rc = pread(file, buf, size, pos);
    if (rc != 0 && rc != size) { 
        imcs_ereport(ERRCODE_IO_ERROR, "File read failed: %d", errno);
    }
    return rc != 0;
}

    
void imcs_file_write(imcs_file_h file, void const* buf, size_t size, off_t pos)
{
    ssize_t rc = pwrite(file, buf, size, pos);
    if (rc != size) { 
        imcs_ereport(ERRCODE_IO_ERROR, "File write failed: %d", errno);
    }
}

void imcs_file_close(imcs_file_h file)
{
    if (close(file) < 0) {
        imcs_ereport(ERRCODE_IO_ERROR, "File close failed: %d", errno);
    }
}

#endif
