#ifndef __FILE_H__
#define __FILE_H__
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <vector>

typedef uint64_t pagenum_t;

constexpr uint64_t INITIAL_SIZE = 10485760; // 10 MiB
constexpr uint64_t PAGE_SIZE = 4096;
constexpr pagenum_t INITIAL_FREE_PAGES = (INITIAL_SIZE / PAGE_SIZE) - 1;

constexpr uint64_t HEADER_FREE_OFFSET = 0;
constexpr uint64_t HEADER_NUMPAGE_OFFSET = 8;
constexpr uint64_t FREE_FREE_OFFSET = 0;

class page_t
{
private:
    template<class T>
    void offsetCheck(uint64_t offset);
protected:
    char data[PAGE_SIZE];
public:
    page_t();
    template<class T>
    void get_data(T* dest, uint64_t offset);
    template<class T>
    T get_data(uint64_t offset);
    template<class T>
    void set_data(const T src, uint64_t offset);
};

namespace PageIO {
    namespace HeaderPage {
        pagenum_t get_free_page_number(page_t* page);
        uint64_t get_num_pages(page_t* page);
        void set_free_page_number(page_t* page, pagenum_t free_page_number);
        void set_num_pages(page_t* page, uint64_t num_pages);
    }
    namespace FreePage {
        pagenum_t get_next_free_page_number(page_t* page);
        void set_next_free_page_number(page_t* page, pagenum_t next_free_page_number);
    }
}

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