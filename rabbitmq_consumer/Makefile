B1;2802;0cINCPATH=/usr/include
CC=gcc
CFLAGS=-c -Wall -g -Iinih -I$(INCPATH) -I$(INCPATH)/mysql
LDFLAGS=-lrabbitmq -lmysqlclient
SRCS= inih/ini.c consumer.c
OBJ=$(SRCS:.c=.o)
all:$(OBJ)
	$(CC) $(LDFLAGS) $(OBJ) -o consumer
%.o:%.c
	$(CC) $(CFLAGS)  $< -o $@

clean:
	-rm *.o
	-rm *~
