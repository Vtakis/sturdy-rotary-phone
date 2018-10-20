OBJS=src/main.o src/functions.o 
SOURCE=src/main.c src/functions.c
HEADER= header-files/functions.h
OUT=myexe
CC=gcc
FLAGS=-g -c -Wall

all:$(OBJS)
	$(CC) -g $(SOURCE) -o $(OUT) 

src/main.o: src/main.c
	$(CC) $(FLAGS) src/main.c

src/functions.o : src/functions.c 
	$(CC) $(FLAGS) src/functions.c

clean: 
	rm -f *.o  $(OUT)

count:
	wc $(SOURCE) $(HEADER)
