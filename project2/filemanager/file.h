#ifndef __FILE_H__
#define __FILE_H__
#include <cstdint>
#include <fstream>
#include <iostream>

const int INITIAL_SIZE = 1048576;
const int PAGE_SIZE = 4096;
const int INITIAL_FREE_PAGES = (INITIAL_SIZE / PAGE_SIZE) - 1;

const int HEADER_PAGE_USED = 16;
const int HEADER_PAGE_RESERVED = PAGE_SIZE - HEADER_PAGE_USED;

const int FREE_PAGE_USED = 8;
const int FREE_PAGE_RESERVED = PAGE_SIZE - FREE_PAGE_USED;

typedef uint64_t pagenum_t;

class page_t
{
protected:
    page_t();
};

class header_page_t : public page_t
{
private:
    pagenum_t free_page_number;
    uint64_t num_pages;
    char reserved[HEADER_PAGE_RESERVED]; // Reserved for other DBMS layers

public:
    header_page_t(); //default constructor

    // getters
    pagenum_t get_free_page_number() const;
    uint64_t get_num_pages() const;

    // setters
    void set_free_page_number(pagenum_t free_page_number);
    void set_num_pages(uint64_t num_pages);
};

class free_page_t : public page_t
{
private:
    pagenum_t next_free_page_number;
    char reserved[FREE_PAGE_RESERVED]; // Reserved for DBMS

public:
    free_page_t(); //default constructor

    // getters
    pagenum_t get_next_free_page_number() const;

    // setters
    void set_next_free_page_number(pagenum_t next_free_page_number);
};

class FileIO
{
private:
    std::fstream file;
    void seek(int position);

public:
    FileIO();
    ~FileIO();

    int open(const char *src);
    void close();
    int is_open();
    int size();
    int get_file_descriptor();

    int file_read_page(pagenum_t pagenum, page_t *dest);
    int file_write_page(pagenum_t pagenum, const page_t *src);
};

extern FileIO File; //global variable for file io

// Open existing database file or create one if not existed.
int64_t file_open_database_file(char* path);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t *dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t *src);

// Stop referencing the database file
void file_close_database_file();

void file_read_header(page_t *header);
void file_write_header(const page_t *header);

#endif // __FILE_H__