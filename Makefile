CC = clang
CFLAGS = -Wall -pedantic -Werror -Wextra
LDFLAGS = -pthread

DEPS = asgn4_helper_funcs.a connection.h debug.h request.h response.h queue.h rwlock.h

all: httpserver

httpserver: httpserver.o asgn4_helper_funcs.a
	$(CC) -o httpserver httpserver.o asgn4_helper_funcs.a $(LDFLAGS)

httpserver.o: httpserver.c $(DEPS)
	$(CC) $(CFLAGS) -c httpserver.c

clean:
	rm -f httpserver *.o

format:
	clang-format -i -style=file *.[ch]

