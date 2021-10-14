#include "file.h"
#include "page.h"

std::vector<int> FileIO::opened_files;
int FileIO::open(const char* filename)
{
    int fd = ::open(filename, O_RDWR | O_CREAT | O_SYNC, 0644);
    std::cout << "[INFO] open(" << filename << ")" << std::endl;
    opened_files.push_back(fd);
    return fd;
}
off_t FileIO::size(int fd)
{
    off_t offset = lseek(fd, 0, SEEK_CUR); // find the current offset
    off_t sz = lseek(fd, 0, SEEK_END);     // get the size of the file
    lseek(fd, offset, SEEK_SET);           // seek back to where it was
    return sz;
}
void FileIO::write(int fd, const void* src, int n, int offset)
{
    pwrite(fd, src, n, offset);
    sync();
}
void FileIO::read(int fd, void* dst, int n, int offset)
{
    pread(fd, dst, n, offset);
}
void FileIO::close(int fd)
{
    ::close(fd);
}

// Open existing database file or create one if not existed.
int file_open_database_file(const char* path)
{
    int fd = FileIO::open(path);
    if (FileIO::size(fd) == 0)
    {
        // defult size = 10MiB = 2560 pages (including header)
        page_t header;
        for (pagenum_t pagenum = INITIAL_FREE_PAGES; pagenum > 0; pagenum--)
        {
            page_t free_page;
            PageIO::FreePage::set_next_free_pagenum(&free_page, pagenum - 1);
            FileIO::write(fd, &free_page, PAGE_SIZE, pagenum * PAGE_SIZE);
        }
        PageIO::HeaderPage::set_num_pages(&header, INITIAL_FREE_PAGES + 1);
        PageIO::HeaderPage::set_free_pagenum(&header, INITIAL_FREE_PAGES);
        FileIO::write(fd, &header, PAGE_SIZE, 0);
    }

    return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd)
{
    page_t header_page;
    FileIO::read(fd, &header_page, PAGE_SIZE, 0);

    pagenum_t free_pagenum = PageIO::HeaderPage::get_free_pagenum(&header_page);
    if (free_pagenum == 0)
    {
        pagenum_t next_num = PageIO::HeaderPage::get_num_pages(&header_page);
        pagenum_t next_size = next_num * 2;
        while (next_num < next_size)
        {
            page_t free_page;
            PageIO::FreePage::set_next_free_pagenum(&free_page, free_pagenum);
            free_pagenum = next_num;
            FileIO::write(fd, &free_page, PAGE_SIZE, next_num * PAGE_SIZE);
            next_num++;
        }
        PageIO::HeaderPage::set_free_pagenum(&header_page, free_pagenum);
        PageIO::HeaderPage::set_num_pages(&header_page, next_size);
    }
    page_t free_page;
    FileIO::read(fd, &free_page, PAGE_SIZE, free_pagenum * PAGE_SIZE);

    PageIO::HeaderPage::set_free_pagenum(&header_page, PageIO::FreePage::get_next_free_pagenum(&free_page));
    FileIO::write(fd, &header_page, PAGE_SIZE, 0);

    // std::cout << "[DEBUG] Allocated " << free_pagenum << std::endl;
    // FileIO::read(fd, &header_page, PAGE_SIZE, 0);
    // std::cout << "[DEBUG] Saved next free " << PageIO::HeaderPage::get_free_pagenum(&header_page) << std::endl;

    return free_pagenum;
}

// Free an on-disk page to the free page list
// WARNING: page at pagenum should already have been allocated
void file_free_page(int fd, pagenum_t pagenum)
{
    page_t header_page;
    FileIO::read(fd, &header_page, PAGE_SIZE, 0);

    page_t free_page;
    PageIO::FreePage::set_next_free_pagenum(&free_page, PageIO::HeaderPage::get_free_pagenum(&header_page));
    FileIO::write(fd, &free_page, PAGE_SIZE, pagenum * PAGE_SIZE);

    PageIO::HeaderPage::set_free_pagenum(&header_page, pagenum);
    FileIO::write(fd, &header_page, PAGE_SIZE, 0);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest)
{
    FileIO::read(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src)
{
    // std::cout << "file_write_page(" << pagenum << ")" << std::endl;
    FileIO::write(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);
}

// Stop referencing the database file
void file_close_database_file()
{
    for (auto const& fd : FileIO::opened_files)
    {
        FileIO::close(fd);
    }
    FileIO::opened_files.clear();
}