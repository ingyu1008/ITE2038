#include "filemanager/file.h"
#include <sstream>

int main(int argc, char const *argv[])
{
    if (argc < 2)
    {
        std::cout << "Usage: " << argv[0] << " <filename>" << std::endl;
        return 1;
    }

    int fd = file_open_database_file(argv[1]);

    while (true)
    {
        std::cout << "Now Reading Header Page" << std::endl;
        header_page_t header;
        file_read_page(fd, 0, &header);
        std::cout << "================================================" << std::endl;
        std::cout << "This Part Is Written for Testing Purpose" << std::endl;
        std::cout << "Entire File Size = " << FileIO::size(fd) << std::endl;
        std::cout << "Number of Pages = " << header.get_num_pages() << std::endl;
        std::cout << "Next Free Page Number = " << header.get_free_page_number() << std::endl;
        std::cout << "================================================" << std::endl;
        std::cout << "Operations" << std::endl;
        std::cout << "a: Allocate a Page" << std::endl;
        std::cout << "f x: Free the Page x" << std::endl;
        std::cout << "q: Exit Program" << std::endl;
        std::cout << "================================================" << std::endl;
        std::cout << ">> ";
        std::string input;
        std::getline(std::cin, input);
        std::stringstream ss(input);
        ss >> input;
        if (input == "q")
            break;
        else if (input == "a")
        {
            pagenum_t page = file_alloc_page(fd);
            std::cout << "[INFO]: allocated page " << page << std::endl;
        }
        else if (input == "f")
        {
            ss >> input;
            pagenum_t page = std::stoll(input);
            file_free_page(fd, page);
            std::cout << "[INFO]: freed page " << page << std::endl;
        }
    }

    file_close_database_file();

    return 0;
}
