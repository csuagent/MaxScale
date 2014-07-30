CC=cc
CFLAGS=-c -Wall -g -Iinih
LDFLAGS= -lrabbitmq
SRCS=consumer.c inih/ini.c
OBJ=$(SRCS:.c=.o)
all:$(OBJ)
	$(CC) $(LDFLAGS) $(OBJ) -o consumer
%.o:%.c
	$(CC) $(CFLAGS) $< -o $@
