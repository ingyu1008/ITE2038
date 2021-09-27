#include "file.h"

page_t::page_t(){};

header_page_t::header_page_t() : free_page_number(0), num_pages(0) { std::fill_n(reserved, HEADER_PAGE_RESERVED, '\0'); };
pagenum_t header_page_t::get_free_page_number() const { return this->free_page_number; }
uint64_t header_page_t::get_num_pages() const { return this->num_pages; }
void header_page_t::set_free_page_number(pagenum_t free_page_number) { this->free_page_number = free_page_number; }
void header_page_t::set_num_pages(uint64_t num_pages) { this->num_pages = num_pages; }

free_page_t::free_page_t() : next_free_page_number(0) { std::fill_n(reserved, FREE_PAGE_RESERVED, '\0'); };
pagenum_t free_page_t::get_next_free_page_number() const { return this->next_free_page_number; }
void free_page_t::set_next_free_page_number(pagenum_t next_free_page_number) { this->next_free_page_number = next_free_page_number; }

void FileIO::seek(int position) { file.seekp(position); }

FileIO::FileIO(){};
FileIO::~FileIO()
{
    if (is_open())
    {
        close();
    }
};
int FileIO::open(const char *src)
{
    if (is_open())
    {
        file.close();
    }
    file.open(src, std::ios::in | std::ios::out | std::ios::ate);
    if (!is_open())
    {
        file.open(src, std::ios::in | std::ios::out | std::ios::ate | std::ios::trunc);
    }
    return 0;
}
void FileIO::close()
{
    if (is_open())
    {
        file.close();
    }
}
int FileIO::is_open()
{
    if (file.is_open())
    {
        return 1;
    }
    return 0;
}
int FileIO::size()
{
    std::streampos pos = this->file.tellg();
    this->file.seekg(0, this->file.end);
    int size = this->file.tellg();
    this->file.seekg(pos);

    return size;
}

/*
 * returns 0 on success
 * returns 1 if file is not open
 */
int FileIO::file_read_page(pagenum_t pagenum, page_t *dest)
{
    if (!this->is_open())
    {
        std::cout << "[ERROR]: file not open" << std::endl;
        return 1;
    }
    file.sync();
    this->seek(pagenum * PAGE_SIZE);
    char buffer[PAGE_SIZE];
    file.read(buffer, PAGE_SIZE);
    std::copy(buffer, buffer + PAGE_SIZE, reinterpret_cast<char *>(dest));

    return 0;
}
/*
 * returns 0 on success
 * returns 1 if file is not open
 */
int FileIO::file_write_page(pagenum_t pagenum, const page_t *src)
{
    if (!this->is_open())
    {
        std::cout << "[ERROR]: file not open" << std::endl;
        return 1;
    }
    std::cout << "[INFO]: file_write_page(" << pagenum << ")" << std::endl;
    this->seek(pagenum * PAGE_SIZE);

    file.write(reinterpret_cast<const char *>(src), PAGE_SIZE);
    file.flush();
    return 0;
}

FileIO File; //global variable

// Open existing database file or create one if not existed.
int64_t file_open_database_file(char *path)
{
    File.open(path);
    if (File.size() == 0)
    {
        // defult size = 10MiB = 256 pages (including header)
        header_page_t header;
        header.set_free_page_number(INITIAL_FREE_PAGES);
        for (pagenum_t pagenum = INITIAL_FREE_PAGES; pagenum > 0; pagenum--)
        {
            free_page_t free_page;
            free_page.set_next_free_page_number(pagenum - 1);
            file_write_page(pagenum, &free_page);
        }
        header.set_num_pages(INITIAL_FREE_PAGES + 1);
        file_write_header(&header);
    }

    return File.get_file_descriptor();
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page()
{
    header_page_t header_page;
    file_read_header(&header_page);

    pagenum_t free_page_number = header_page.get_free_page_number();
    if (free_page_number == 0)
    {
        pagenum_t next_num = header_page.get_num_pages();
        pagenum_t next_size = next_num * 2;
        while (next_num < next_size)
        {
            free_page_t free_page;
            free_page.set_next_free_page_number(free_page_number);
            free_page_number = next_num;
            file_write_page(next_num, &free_page);
            next_num++;
        }
        header_page.set_free_page_number(free_page_number);
        header_page.set_num_pages(next_size);
    }
    free_page_t free_page;
    file_read_page(free_page_number, &free_page);

    header_page.set_free_page_number(free_page.get_next_free_page_number());
    file_write_header(&header_page);
    return free_page_number;
}

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum)
{
    header_page_t header_page;
    file_read_header(&header_page);

    free_page_t free_page;
    free_page.set_next_free_page_number(header_page.get_free_page_number());
    file_write_page(pagenum, &free_page);

    header_page.set_free_page_number(pagenum);
    file_write_header(&header_page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t *dest)
{
    int code = File.file_read_page(pagenum, dest);
    if (code != 0)
    {
        std::cout << "[ERROR]: file_read_page failed" << std::endl;
        std::cout << "[ERROR]: the error code is " << code << std::endl;
    }
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t *src)
{
    int code = File.file_write_page(pagenum, src);
    if (code != 0)
    {
        std::cout << "[ERROR]: file_write_page failed" << std::endl;
        std::cout << "[ERROR]: the error code is " << code << std::endl;
    }
}

// Stop referencing the database file
void file_close_database_file()
{
    File.close();
}

void file_read_header(page_t *header) { file_read_page(0, header); }
void file_write_header(const page_t *header) { file_write_page(0, header); }