# CampusCompute Makefile (PES1UG25AM440, PES1UG25AM488)
CC      = gcc
CFLAGS  = -Wall -Wextra -std=gnu11 -O2 -pthread
LDLIBS  = -lrt -pthread
TARGET  = campuscompute

all: $(TARGET)

$(TARGET): campuscompute.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

run: $(TARGET)
	./$(TARGET) demo

# Removes only build output and this program's log; never touches other files or system-wide IPC.
clean:
	rm -f $(TARGET) campuscompute.log

.PHONY: all run clean
