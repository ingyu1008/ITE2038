#include "file.h"

#include <gtest/gtest.h>

#include <string>

//File Initialization
TEST(FileManager, FileInitializationTest)
{
    if (std::remove("testdb") == 0)
    {
        std::cout << "[INFO] File 'testdb' already exists. Deleting it." << std::endl;
    }
    int64_t fd = file_open_table_file("testdb");
    page_t header;
    file_read_page(fd, 0, &header);
    if (fd < 0 || fd >= 1024)
    {
        FAIL(); // File Descriptor Should be in randge of [0, 1023]
    }

    EXPECT_EQ(FileIO::size(fd), INITIAL_SIZE); // File size should equal to what it should be.
    EXPECT_EQ(PageIO::HeaderPage::get_num_pages(&header), INITIAL_FREE_PAGES + 1);

    file_close_database_file();
}

// Page Management - Simple Alloc/Free
TEST(FileManager, SimpleAllocFree)
{
    int64_t fd = file_open_table_file("testdb");

    page_t header;

    file_read_page(fd, 0, &header);
    pagenum_t firstnum = PageIO::HeaderPage::get_free_pagenum(&header);
    pagenum_t first = file_alloc_page(fd);
    EXPECT_EQ(first, firstnum);

    file_read_page(fd, 0, &header);
    pagenum_t secondnum = PageIO::HeaderPage::get_free_pagenum(&header);
    pagenum_t second = file_alloc_page(fd);
    EXPECT_EQ(second, secondnum);

    file_read_page(fd, 0, &header);
    EXPECT_NE(second, PageIO::HeaderPage::get_free_pagenum(&header));

    file_free_page(fd, first);
    file_read_page(fd, 0, &header);
    page_t page;
    bool flag = false;
    for (pagenum_t i = PageIO::HeaderPage::get_free_pagenum(&header); i != 0; i = PageIO::FreePage::get_next_free_pagenum(&page))
    {
        file_read_page(fd, i, &page);
        EXPECT_NE(i, second);
        flag |= (i == first);
    }

    file_free_page(fd, second);

    file_close_database_file();
}

// Page Management - Double Page Size Check
TEST(FileManager, AdvancedAllocation)
{
    int64_t fd = file_open_table_file("testdb");

    std::vector<pagenum_t> allocated;

    for (int i = 0; i < INITIAL_FREE_PAGES; i++)
    {
        pagenum_t x = file_alloc_page(fd);
        allocated.push_back(x);
        EXPECT_NE(x, 0);
    }

    page_t header;
    file_read_page(fd, 0, &header);
    EXPECT_EQ(FileIO::size(fd), INITIAL_SIZE); // File size should equal to what it should be.
    EXPECT_EQ(PageIO::HeaderPage::get_num_pages(&header), INITIAL_FREE_PAGES + 1);

    allocated.push_back(file_alloc_page(fd));

    file_read_page(fd, 0, &header);
    EXPECT_EQ(FileIO::size(fd), INITIAL_SIZE * 2); // File size should equal to what it should be.
    EXPECT_EQ(PageIO::HeaderPage::get_num_pages(&header), (INITIAL_FREE_PAGES + 1) * 2);

    for(auto pagenum : allocated){
        file_free_page(fd, pagenum);
    }

    file_close_database_file();
}

// Page IO
TEST(FileManager, PageIO)
{
    char buffer[PAGE_SIZE] = "Hello World! abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    int64_t fd = file_open_table_file("testdb");

    pagenum_t p = file_alloc_page(fd);

    file_write_page(fd, p, buffer);

    char dest[PAGE_SIZE] = "";
    file_read_page(fd, p, dest);

    for (int i = 0; i < PAGE_SIZE; i++)
    {
        EXPECT_EQ(buffer[i], dest[i]);
    }

    file_free_page(fd, p);
    file_close_database_file();
}
