cmake -S . -B build
cmake --build build
cd build
ctest
cd bin
./db_test
cd ../..
