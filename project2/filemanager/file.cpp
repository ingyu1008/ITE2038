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

// ----------------------------------------------------------------
// This Part is deprecated. Now Using linux system calls for file io.
// ----------------------------------------------------------------
// void FileIO::seek(int position) { file.seekp(position); }
//
// FileIO::FileIO(){};
// FileIO::~FileIO()
// {
//     if (is_open())
//     {
//         close();
//     }
// };
// int FileIO::open(const char *src)
// {
//     if (is_open())
//     {
//         file.close();
//     }
//     file.open(src, std::ios::in | std::ios::out | std::ios::ate);
//     if (!is_open())
//     {
//         file.open(src, std::ios::in | std::ios::out | std::ios::ate | std::ios::trunc);
//     }
//     return 0;
// }
// void FileIO::close()
// {
//     if (is_open())
//     {
//         file.close();
//     }
// }
// int FileIO::is_open()
// {
//     if (file.is_open())
//     {
//         return 1;
//     }
//     return 0;
// }
// int FileIO::size()
// {
//     std::streampos pos = this->file.tellg();
//     this->file.seekg(0, this->file.end);
//     int size = this->file.tellg();
//     this->file.seekg(pos);
//
//     return size;
// }
//
// /*
//  * returns 0 on success
//  * returns 1 if file is not open
//  */
// int FileIO::file_read_page(pagenum_t pagenum, page_t *dest)
// {
//     if (!this->is_open())
//     {
//         std::cout << "[ERROR]: file not open" << std::endl;
//         return 1;
//     }
//     file.sync();
//     this->seek(pagenum * PAGE_SIZE);
//     char buffer[PAGE_SIZE];
//     file.read(buffer, PAGE_SIZE);
//     std::copy(buffer, buffer + PAGE_SIZE, reinterpret_cast<char *>(dest));
//
//     return 0;
// }
// /*
//  * returns 0 on success
//  * returns 1 if file is not open
//  */
// int FileIO::file_write_page(pagenum_t pagenum, const page_t *src)
// {
//     if (!this->is_open())
//     {
//         std::cout << "[ERROR]: file not open" << std::endl;
//         return 1;
//     }
//     std::cout << "[INFO]: file_write_page(" << pagenum << ")" << std::endl;
//     this->seek(pagenum * PAGE_SIZE);
//
//     file.write(reinterpret_cast<const char *>(src), PAGE_SIZE);
//     file.flush();
//     return 0;
// }
// ----------------------------------------------------------------

namespace FileIO
{
    std::vector<int> opened_files;

    int open(const char *filename)
    {
        int fd = ::open(filename, O_RDWR | O_CREAT | O_SYNC, 0644);
        std::cout << "[INFO]: open(" << filename << ")" << std::endl;
        opened_files.push_back(fd);
        return fd;
    }

    off_t size(int fd)
    {
        off_t offset = lseek(fd, 0, SEEK_CUR); // find the current offset
        off_t sz = lseek(fd, 0, SEEK_END);     // get the size of the file
        lseek(fd, offset, SEEK_SET);           // seek back to where it was
        return sz;
    }

    void write(int fd, const void *src, int n, int offset)
    {
        std::cout << "[INFO]: write(" << fd << "," << n << "," << offset << ")" << std::endl;
        pwrite(fd, src, n, offset);
    }

    void read(int fd, void *dst, int n, int offset)
    {
        pread(fd, dst, n, offset);
    }

    void close(int fd)
    {
        ::close(fd);
    }
}

// Open existing database file or create one if not existed.
int file_open_database_file(const char *path)
{
    int fd = FileIO::open(path);
    if (FileIO::size(fd) == 0)
    {
        // defult size = 10MiB = 256 pages (including header)
        header_page_t header;
        header.set_free_page_number(INITIAL_FREE_PAGES);
        for (pagenum_t pagenum = INITIAL_FREE_PAGES; pagenum > 0; pagenum--)
        {
            free_page_t free_page;
            free_page.set_next_free_page_number(pagenum - 1);
            FileIO::write(fd, &free_page, PAGE_SIZE, pagenum * PAGE_SIZE);
        }
        header.set_num_pages(INITIAL_FREE_PAGES + 1);
        FileIO::write(fd, &header, PAGE_SIZE, 0);
        FileIO::read(fd, &header, PAGE_SIZE, 0);
    }

    return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd)
{
    header_page_t header_page;
    FileIO::read(fd, &header_page, PAGE_SIZE, 0);

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
            FileIO::write(fd, &free_page, PAGE_SIZE, next_num * PAGE_SIZE);
            next_num++;
        }
        header_page.set_free_page_number(free_page_number);
        header_page.set_num_pages(next_size);
    }
    free_page_t free_page;
    FileIO::read(fd, &free_page, PAGE_SIZE, free_page_number * PAGE_SIZE);

    header_page.set_free_page_number(free_page.get_next_free_page_number());
    FileIO::write(fd, &header_page, PAGE_SIZE, 0);
    return free_page_number;
}

// Free an on-disk page to the free page list
// WARNING: page at pagenum should already have been allocated
void file_free_page(int fd, pagenum_t pagenum)
{
    header_page_t header_page;
    FileIO::read(fd, &header_page, PAGE_SIZE, 0);

    free_page_t free_page;
    free_page.set_next_free_page_number(header_page.get_free_page_number());
    FileIO::write(fd, &free_page, PAGE_SIZE, pagenum * PAGE_SIZE);

    header_page.set_free_page_number(pagenum);
    FileIO::write(fd, &header_page, PAGE_SIZE, 0);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t *dest)
{
    FileIO::read(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t *src)
{
    FileIO::write(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);
}

// Stop referencing the database file
void file_close_database_file()
{
    for (auto const &fd : FileIO::opened_files)
    {
        FileIO::close(fd);
    }
    FileIO::opened_files.clear();
}

// void file_read_header(page_t *header) { file_read_page(0, header); }
// void file_write_header(const page_t *header) { file_write_page(0, header); }F