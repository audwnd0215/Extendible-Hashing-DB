SRCS=bucket.cc index_page.cc hash_index.cc ehdb.cc
OBJS=$(SRCS:.cc=.o)

CXX?=g++
CFLAGS?=-Wall

.PHONY: clean

ehdb: $(OBJS)
	$(CXX) -o $@ $^

bucket.o:

index_page.o:

hash_index.o:

ehdb.o:

cut_lines:
	$(CXX) -o $@ cut_lines.cc

clean:
	-rm ehdb *.o cut_lines
