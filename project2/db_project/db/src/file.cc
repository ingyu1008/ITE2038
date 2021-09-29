#include "file.h"

template<class T>
void page_t::offsetCheck(uint64_t offset) {
    if (offset + sizeof(T) > PAGE_SIZE) {
        std::cout << "[ERROR]: Invalid Offset at get_data()" << std::endl;
        throw std::invalid_argument("Offset is out of range.");
    }
}

page_t::page_t() { std::fill_n(data, PAGE_SIZE, '\0'); };

template<class T>
T page_t::get_data(uint64_t offset) {
    offsetCheck<T>(offset);
    T ret;
    std::memcpy(&ret, data + offset, sizeof(T));
    return ret;
}

template<class T>
void page_t::get_data(T* dest, uint64_t offset) {
    offsetCheck<T>(offset);
    std::memcpy(dest, data + offset, sizeof(T));
}

template<class T>
void page_t::set_data(const T src, uint64_t offset) {
    offsetCheck<T>(offset);
    std::memcpy(data + offset, &src, sizeof(T));
}

// header_page_t::header_page_t() {};
// pagenum_t header_page_t::get_free_page_number() const {
//     pagenum_t free_page_number;
//     memcpy(&free_page_number, data + HEADER_FREE_OFFSET, sizeof(pagenum_t));
//     return free_page_number;
// };
// uint64_t header_page_t::get_num_pages() const {
//     uint64_t num_pages;
//     memcpy(&num_pages, data + HEADER_NUMPAGE_OFFSET, sizeof(uint64_t));
//     return num_pages;
// }
// void header_page_t::set_free_page_number(pagenum_t free_page_number) {
//     memcpy(data + HEADER_FREE_OFFSET, &free_page_number, sizeof(pagenum_t));
// }
// void header_page_t::set_num_pages(uint64_t num_pages) {
//     memcpy(data + HEADER_NUMPAGE_OFFSET, &num_pages, sizeof(uint64_t));
// }

// free_page_t::free_page_t() {};
// pagenum_t free_page_t::get_next_free_page_number() const {
//     pagenum_t next_free_page_number;
//     memcpy(&next_free_page_number, data + FREE_FREE_OFFSET, sizeof(pagenum_t));
//     return next_free_page_number;
// }
// void free_page_t::set_next_free_page_number(pagenum_t next_free_page_number) {
//     memcpy(data + FREE_FREE_OFFSET, &next_free_page_number, sizeof(pagenum_t));
// }

pagenum_t PageIO::HeaderPage::get_free_page_number(page_t* page) {
    return page->get_data<pagenum_t>(HEADER_FREE_OFFSET);
}
uint64_t PageIO::HeaderPage::get_num_pages(page_t* page) {
    return page->get_data<uint64_t>(HEADER_NUMPAGE_OFFSET);
}
void PageIO::HeaderPage::set_free_page_number(page_t* page, pagenum_t free_page_number) {
    page->set_data(free_page_number, HEADER_FREE_OFFSET);
}
void PageIO::HeaderPage::set_num_pages(page_t* page, uint64_t num_pages) {
    page->set_data(num_pages, HEADER_NUMPAGE_OFFSET);
}
pagenum_t PageIO::FreePage::get_next_free_page_number(page_t* page) {
    return page->get_data<pagenum_t>(FREE_FREE_OFFSET);
}
void PageIO::FreePage::set_next_free_page_number(page_t* page, pagenum_t next_free_page_number) {
    page->set_data(next_free_page_number, FREE_FREE_OFFSET);
}

std::vector<int> FileIO::opened_files;
int FileIO::open(const char* filename)
{
    int fd = ::open(filename, O_RDWR | O_CREAT | O_SYNC, 0644);
    std::cout << "[INFO]: open(" << filename << ")" << std::endl;
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
    // std::cout << "[INFO]: write(" << fd << "," << n << "," << offset << ")" << std::endl;
    pwrite(fd, src, n, offset);
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
            PageIO::FreePage::set_next_free_page_number(&free_page, pagenum - 1);
            FileIO::write(fd, &free_page, PAGE_SIZE, pagenum * PAGE_SIZE);
        }
        PageIO::HeaderPage::set_num_pages(&header, INITIAL_FREE_PAGES + 1);
        PageIO::HeaderPage::set_free_page_number(&header, INITIAL_FREE_PAGES);
        FileIO::write(fd, &header, PAGE_SIZE, 0);
    }

    return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd)
{
    page_t header_page;
    FileIO::read(fd, &header_page, PAGE_SIZE, 0);

    pagenum_t free_page_number = PageIO::HeaderPage::get_free_page_number(&header_page);
    if (free_page_number == 0)
    {
        pagenum_t next_num = PageIO::HeaderPage::get_num_pages(&header_page);
        pagenum_t next_size = next_num * 2;
        while (next_num < next_size)
        {
            page_t free_page;
            PageIO::FreePage::set_next_free_page_number(&free_page, free_page_number);
            free_page_number = next_num;
            FileIO::write(fd, &free_page, PAGE_SIZE, next_num * PAGE_SIZE);
            next_num++;
        }
        PageIO::HeaderPage::set_free_page_number(&header_page, free_page_number);
        PageIO::HeaderPage::set_num_pages(&header_page, next_size);
    }
    page_t free_page;
    FileIO::read(fd, &free_page, PAGE_SIZE, free_page_number * PAGE_SIZE);

    PageIO::HeaderPage::set_free_page_number(&header_page, PageIO::FreePage::get_next_free_page_number(&free_page));
    FileIO::write(fd, &header_page, PAGE_SIZE, 0);
    return free_page_number;
}

// Free an on-disk page to the free page list
// WARNING: page at pagenum should already have been allocated
void file_free_page(int fd, pagenum_t pagenum)
{
    page_t header_page;
    FileIO::read(fd, &header_page, PAGE_SIZE, 0);

    page_t free_page;
    PageIO::FreePage::set_next_free_page_number(&free_page, PageIO::HeaderPage::get_free_page_number(&header_page));
    FileIO::write(fd, &free_page, PAGE_SIZE, pagenum * PAGE_SIZE);

    PageIO::HeaderPage::set_free_page_number(&header_page, pagenum);
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

// void file_read_header(page_t *header) { file_read_page(0, header); }
// void file_write_header(const page_t *header) { file_write_page(0, header); }F