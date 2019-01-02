OBJS=src/main.o src/functions.o 
SOURCE=src/main.c src/functions.c
HEADER= header-files/functions.h
OUT=myexe
OBJS2=src/unit_testing.o src/functions.o
SOURCE2=src/unit_testing.c src/functions.c
OUT2=unitest
CC=gcc
FLAGS=-g -c

all:$(OBJS) $(OBJS2)
	$(CC) -g $(SOURCE) -o $(OUT) -lm
	$(CC) -g $(SOURCE2) -o $(OUT2) -lcunit -lm

src/main.o: src/main.c
	$(CC) $(FLAGS) src/main.c

src/unit_testing.o: src/unit_testing.c
	$(CC) $(FLAGS) src/unit_testing.c

src/functions.o : src/functions.c 
	$(CC) $(FLAGS) src/functions.c

clean: 
	rm -f *.o  $(OUT) $(OUT2)

count:
	wc $(SOURCE) $(HEADER)
