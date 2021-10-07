#ifndef __FILE_H__
#define __FILE_H__
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <vector>

#include "page.h"

namespace FileIO
{
    extern std::vector<int> opened_files;
    int open(const char* filename);
    off_t size(int fd);
    void write(int fd, const void* src, int n, int offset);
    void read(int fd, void* dst, int n, int offset);
    void close(int fd);
}

// Open existing database file or create one if not existed.
int file_open_database_file(const char* path);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd);

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);

// Stop referencing the database file
void file_close_database_file();

#endif // __FILE_H__