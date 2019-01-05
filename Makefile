OBJS=src/main.o src/functions.o src/Job_Scheduler.o
SOURCE=src/main.c src/functions.c src/Job_Scheduler.c
HEADER= header-files/functions.h header-files/Job_Scheduler.h
OUT=myexe
OBJS2=src/unit_testing.o src/functions.o src/Job_Scheduler.o
SOURCE2=src/unit_testing.c src/functions.c src/Job_Scheduler.c
OUT2=unitest
SOURCE3=harness.cpp
OUT3=harness
CC=gcc
CCX=g++
FLAGS=-c -g 

all:$(OBJS) $(OBJS2)
	$(CC) -g $(SOURCE) -o $(OUT) -lpthread -lm
	$(CC) -g $(SOURCE2) -o $(OUT2) -lcunit -lpthread -lm
	$(CCX) -g $(SOURCE3) -o $(OUT3) 

src/main.o: src/main.c
	$(CC) $(FLAGS) src/main.c

src/unit_testing.o: src/unit_testing.c
	$(CC) $(FLAGS) src/unit_testing.c

src/Job_Scheduler.o: src/Job_Scheduler.c
	$(CC) $(FLAGS) src/Job_Scheduler.c

src/functions.o : src/functions.c 
	$(CC) $(FLAGS) src/functions.c


clean: 
	rm -f *.o  $(OUT) $(OUT2) $(OUT3)

count:
	wc $(SOURCE) $(HEADER)
