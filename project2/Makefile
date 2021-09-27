CC = g++
FLAGS = -std=c++11 -Wall
INCS = filemanager
SRCS = $(wildcard */*.cpp)
OBJS = $(SRCS:.cpp=.o)
TARGET = main

all: $(TARGET)

$(TARGET): $(TARGET).o $(OBJS)
	$(CC) $(FLAGS) -o $@ $^ -I $(INCS)

%.o: %.cpp
	$(CC) $(FLAGS) -o $@ -c $<

clean:
	rm $(target) $(TARGET).o $(OBJS)