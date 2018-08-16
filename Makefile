CC= g++
CFLAGS = -Wall -Wextra -std=c++11 -pthread -g

# All Target
all: Search libMapReduceFramework.a

MapReduceFramework.a: libMaprReduceFramework.a

libMapReduceFramework.a: Search.o MapReduceFramework.o
	ar rcs MapReduceFramework.a $^
	
Search: Search.o MapReduceFramework.o
	$(CC) $(CFLAGS) $^ -o $@
	
%.o: %.cpp MapReduceClient.h MapReduceFramework.h
	$(CC) $(CFLAGS) -c $<
	
tar:
	tar cvf ex3.tar README Makefile Search.cpp MapReduceFramework.cpp

clean:
	rm -f Search.o MapReduceFramework.a ex3.tar

.PHONY: tar clean

