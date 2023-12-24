#include <sys/socket.h>
#include <sys/stat.h>
#include <limits.h>
#include <sys/fcntl.h>
#include <stdbool.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include "connection.h"
#include "debug.h"
#include "response.h"
#include "request.h"
#include "queue.h"
#include "rwlock.h"
#include "asgn2_helper_funcs.h"

#define BUFFER_SIZE 2048

pthread_mutex_t mutex;

// Node struct
typedef struct rwlockNodeObj *rwlockNode;

typedef struct rwlockNodeObj {
    char *uri;
    rwlock_t *rwlock;
    rwlockNode next;
} rwlockNodeObj;

// Hash table that holds Node
typedef struct rwlockHTObj *rwlockHT;

typedef struct rwlockHTObj {
    rwlockNode head;
    int length;
} rwlockHTObj;

// Thread that holds Hash table
typedef struct ThreadObj *Thread;

typedef struct ThreadObj {
    pthread_t thread;
    int id;
    rwlockHT *rwlockHT;
    queue_t *queue;
} ThreadObj;

void handle_connection(int, rwlockHT);
void handle_get(conn_t *, rwlockHT);
void handle_put(conn_t *, rwlockHT);
void handle_unsupported(conn_t *);

rwlockNode newRWNode(char *uri, rwlock_t *rwlock) {
    rwlockNode node = malloc(sizeof(rwlockNodeObj));

    node->uri = strdup(uri);
    node->rwlock = rwlock;
    node->next = NULL;

    return node;
}

void appendNode(rwlockHT hashTable, char *uri, rwlock_t *lock) {
    if (hashTable->length == 0) {
        rwlockNodeObj *newNode = newRWNode(uri, lock);
        hashTable->head = newNode;
        hashTable->length++;
    } else {
        rwlockNodeObj *currNode = hashTable->head;

        while (currNode->next != NULL) {
            currNode = currNode->next;
        }

        rwlockNodeObj *newNode = newRWNode(uri, lock);
        currNode->next = newNode;
        hashTable->length++;
    }
}

rwlockHT newlockHT(void) {
    rwlockHT lockNode = malloc(sizeof(rwlockHTObj));
    lockNode->length = 0;
    lockNode->head = NULL;

    return lockNode;
}

void freeNode(rwlockHT *lockNode) {
    rwlockNode node = (*lockNode)->head;
    for (int i = 0; i < (*lockNode)->length; i++) {
        rwlockNode nextNode = node->next;
        free(node);
        node = nextNode;
    }

    free(lockNode);
}

// from Mitchell's section
rwlock_t *rwlock_ht_ll_lookup(rwlockNode head, char *uri) {

    if (head == NULL)
        return NULL;

    rwlockNode node = head;

    while (node != NULL) {
        if (strcmp(node->uri, uri) == 0) {
            return node->rwlock;
        }

        node = node->next;
    }

    return NULL; // no match
}

int verifyMethod(const char *str) {
    if (strncmp(str, "GET ", 4) == 0)
        return 1;
    else if (strncmp(str, "PUT ", 4) == 0)
        return 2;
    else
        return 0;
}

int verifyVersion(const char *str) {
    if (strncmp(str, "HTTP/", 5) == 0)
        return 1;
    else
        return 0;
}

int verifyVersionNum(const char *str) {
    if (!(str[0] >= '0' && str[0] <= '9' && str[2] >= '0' && str[2] <= '9'))
        return 0;

    if (str[1] != '.')
        return 0;

    if (str[3] != '\r')
        return 0;

    if (str[4] != '\n')
        return 0;

    return 1;
}

bool isAlphabetic(const char *str) {
    while (*str) {
        if ((*str >= 'a' && *str <= 'z') || (*str >= 'A' && *str <= 'Z') || *str == ' ')
            str++;
        else
            return false;
    }
    return true;
}

bool isAlphaPlus(const char *str) {
    while (*str) {
        char current_char = *str;
        if ((current_char >= 'a' && current_char <= 'z')
            || (current_char >= 'A' && current_char <= 'Z')
            || (current_char >= '0' && current_char <= '9')
            || (current_char == '.' || current_char == '-') || (current_char == ' ')
            || (current_char == ':')) {
            str++;
        } else
            return false;
    }
    return true;
}

// from Mitchell's section
void worker_thread(Thread thread) {
    queue_t *queue = thread->queue;

    while (1) {
        uintptr_t connfd = 0;
        queue_pop(queue, (void **) &connfd);
        handle_connection((int) connfd, *thread->rwlockHT);
        close((int) connfd);
    }
}

int main(int argc, char **argv) {

    char *endptr = NULL;
    long port = strtol(argv[1], &endptr, 10);
    int t = 4;
    int opt;
    int manualThread = 0;

    while ((opt = getopt(argc, argv, "t:")) != -1) {
        switch (opt) {
        case 't':
            t = atoi(optarg);
            manualThread = 1;
            break;
        default: break;
        }
    }

    if (argc < 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Get port
    if (manualThread)
        port = strtol(argv[3], &endptr, 10);
    else
        port = strtol(argv[1], &endptr, 10);

    if (port < 1 || port > 65535 || (endptr && *endptr != '\0')) {
        fprintf(stderr, "Invalid Port\n");
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket sock;
    listener_init(&sock, (int) port);

    Thread threads[t];
    rwlockHT rwlock_ht = newlockHT();
    queue_t *queue = queue_new(t);

    for (int i = 0; i < t; i++) {
        threads[i] = malloc(sizeof(ThreadObj));
        threads[i]->id = i;
        threads[i]->rwlockHT = &rwlock_ht;
        threads[i]->queue = queue;

        pthread_create(&threads[i]->thread, NULL, (void *(*) (void *) ) worker_thread, threads[i]);
    }

    // dispatcher thread
    while (1) {
        uintptr_t connfd = listener_accept(&sock);
        queue_push(queue, (void *) connfd);
    }

    return EXIT_SUCCESS;
}

void handle_connection(int connfd, rwlockHT rwlock_HT) {

    conn_t *conn = conn_new(connfd);

    const Response_t *res = conn_parse(conn);

    if (res != NULL) {
        conn_send_response(conn, res);
    } else {
        debug("%s", conn_str(conn));
        const Request_t *req = conn_get_request(conn);
        if (req == &REQUEST_GET) {
            handle_get(conn, rwlock_HT);
        } else if (req == &REQUEST_PUT) {
            handle_put(conn, rwlock_HT);
        } else {
            handle_unsupported(conn);
        }
    }

    conn_delete(&conn);
}

void handle_get(conn_t *conn, rwlockHT rwlock_HT) {

    char *uri = conn_get_uri(conn);

    const Response_t *res = NULL;

    pthread_mutex_lock(&mutex);

    rwlock_t *lock = rwlock_ht_ll_lookup(rwlock_HT->head, uri);
    if (lock == NULL) {
        lock = rwlock_new(N_WAY, 1);
        appendNode(rwlock_HT, uri, lock);
    }
    pthread_mutex_unlock(&mutex);

    reader_lock(lock);

    if (isAlphaPlus(uri) == false) {
        res = &RESPONSE_BAD_REQUEST;
        char *req = conn_get_header(conn, "Request-Id");
        if (req == NULL)
            req = "0";
        fprintf(stderr, "GET,/%s,400,%s\n", uri, req);
        goto out;
    }

    int fd = open(uri, O_RDONLY);

    if (fd < 0) {
        if (errno == ENOENT) {
            res = &RESPONSE_NOT_FOUND;
            char *req = conn_get_header(conn, "Request-Id");
            if (req == NULL)
                req = "0";
            fprintf(stderr, "GET,/%s,404,%s\n", uri, req);
            goto out;
        }
        if (errno == EACCES || errno == EISDIR) {
            res = &RESPONSE_FORBIDDEN;
            char *req = conn_get_header(conn, "Request-Id");
            if (req == NULL)
                req = "0";
            fprintf(stderr, "GET,/%s,403,%s\n", uri, req);
            goto out;
        } else {
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
            char *req = conn_get_header(conn, "Request-Id");
            if (req == NULL)
                req = "0";
            fprintf(stderr, "GET,/%s,500,%s\n", uri, req);
            goto out;
        }
    }

    struct stat fileStat;
    fstat(fd, &fileStat);

    if (S_ISDIR(fileStat.st_mode)) {
        res = &RESPONSE_FORBIDDEN;
        char *req = conn_get_header(conn, "Request-Id");
        if (req == NULL)
            req = "0";
        fprintf(stderr, "GET,/%s,403,%s\n", uri, req);
        goto out;
    }

    int fileSize = (int) fileStat.st_size;

    res = conn_send_file(conn, fd, fileSize);

    if (res == NULL) {
        res = &RESPONSE_OK;
        char *req = conn_get_header(conn, "Request-Id");
        if (req == NULL)
            req = "0";
        fprintf(stderr, "GET,/%s,200,%s\n", uri, req);
    }

    reader_unlock(lock);

    close(fd);
    return;

out:
    conn_send_response(conn, res);
    reader_unlock(lock);
}

// don't change
void handle_unsupported(conn_t *conn) {
    debug("handling unsupported request");

    // send responses
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
}

void handle_put(conn_t *conn, rwlockHT rwlock_HT) {

    char *uri = conn_get_uri(conn);
    const Response_t *res = NULL;
    debug("handling put request for %s", uri);

    // Check if file already exists before opening it.
    bool existed = access(uri, F_OK) == 0;
    debug("%s existed? %d", uri, existed);

    pthread_mutex_lock(&mutex);

    rwlock_t *lock = rwlock_ht_ll_lookup(rwlock_HT->head, uri);
    if (lock == NULL) {
        lock = rwlock_new(N_WAY, 1);
        appendNode(rwlock_HT, uri, lock);
    }
    pthread_mutex_unlock(&mutex);

    writer_lock(lock);

    // Open the file..
    int fd = open(uri, O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd < 0) {
        debug("%s: %d", uri, errno);
        if (errno == EACCES || errno == EISDIR || errno == ENOENT) {
            res = &RESPONSE_FORBIDDEN;
            char *req = conn_get_header(conn, "Request-Id");
            if (req == NULL)
                req = "0";
            fprintf(stderr, "PUT,/%s,403,%s\n", uri, req);
            goto out;
        } else {
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
            char *req = conn_get_header(conn, "Request-Id");
            if (req == NULL)
                req = "0";
            fprintf(stderr, "PUT,/%s,500,%s\n", uri, req);
            goto out;
        }
    }

    res = conn_recv_file(conn, fd);

    if (res == NULL && existed) {
        res = &RESPONSE_OK;
        char *req = conn_get_header(conn, "Request-Id");
        if (req == NULL)
            req = "0";
        fprintf(stderr, "PUT,/%s,200,%s\n", uri, req);
    } else if (res == NULL && !existed) {
        res = &RESPONSE_CREATED;
        char *req = conn_get_header(conn, "Request-Id");
        if (req == NULL)
            req = "0";
        fprintf(stderr, "PUT,/%s,201,%s\n", uri, req);
    }

    writer_unlock(lock);
    close(fd);

out:
    conn_send_response(conn, res);
}
