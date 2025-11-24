#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <sys/socket.h>
#include <errno.h>

#include "client_registry.h"
#include "debug.h"
#include <sys/syscall.h>

/*
 * Debug macro with thread ID format (matching demo_server)
 */
#define debug_thread(S, ...) \
    do { \
        fprintf(stderr, KMAG "DEBUG: %lu: " KNRM S NL, (unsigned long)syscall(SYS_gettid), ##__VA_ARGS__); \
    } while (0)

#define MAX_CLIENTS 1024

struct client_registry {
    int *fds;                    // Array of file descriptors
    int count;                    // Number of registered clients
    int capacity;                 // Capacity of the array
    pthread_mutex_t mutex;       // Mutex protecting the registry
    sem_t empty_sem;             // Semaphore signaled when count reaches 0
};

/*
 * Initialize a new client registry.
 */
CLIENT_REGISTRY *creg_init() {
    CLIENT_REGISTRY *cr = malloc(sizeof(CLIENT_REGISTRY));
    if (cr == NULL) {
        return NULL;
    }
    
    cr->capacity = MAX_CLIENTS;
    cr->count = 0;
    cr->fds = malloc(sizeof(int) * cr->capacity);
    if (cr->fds == NULL) {
        free(cr);
        return NULL;
    }
    
    if (pthread_mutex_init(&cr->mutex, NULL) != 0) {
        free(cr->fds);
        free(cr);
        return NULL;
    }
    
    if (sem_init(&cr->empty_sem, 0, 0) != 0) {
        pthread_mutex_destroy(&cr->mutex);
        free(cr->fds);
        free(cr);
        return NULL;
    }
    
    return cr;
}

/*
 * Finalize a client registry, freeing all associated resources.
 */
void creg_fini(CLIENT_REGISTRY *cr) {
    if (cr == NULL) {
        return;
    }
    
    pthread_mutex_destroy(&cr->mutex);
    sem_destroy(&cr->empty_sem);
    free(cr->fds);
    free(cr);
}

/*
 * Register a client file descriptor.
 */
int creg_register(CLIENT_REGISTRY *cr, int fd) {
    if (cr == NULL || fd < 0) {
        return -1;
    }
    
    pthread_mutex_lock(&cr->mutex);
    
    if (cr->count >= cr->capacity) {
        pthread_mutex_unlock(&cr->mutex);
        return -1;
    }
    
    // Add fd to array
    cr->fds[cr->count] = fd;
    cr->count++;
    
    debug_thread("Register client fd %d (total connected: %d)", fd, cr->count);
    
    pthread_mutex_unlock(&cr->mutex);
    return 0;
}

/*
 * Unregister a client file descriptor, alerting anybody waiting
 * for the registered set to become empty.
 */
int creg_unregister(CLIENT_REGISTRY *cr, int fd) {
    if (cr == NULL || fd < 0) {
        return -1;
    }
    
    pthread_mutex_lock(&cr->mutex);
    
    // Find and remove fd from array
    int found = 0;
    for (int i = 0; i < cr->count; i++) {
        if (cr->fds[i] == fd) {
            // Move last element to this position
            cr->fds[i] = cr->fds[cr->count - 1];
            cr->count--;
            found = 1;
            break;
        }
    }
    
    if (!found) {
        pthread_mutex_unlock(&cr->mutex);
        return -1;
    }
    
    debug_thread("Unregistered client fd %d (count: %d)", fd, cr->count);
    
    // If count reached zero, signal the semaphore
    if (cr->count == 0) {
        sem_post(&cr->empty_sem);
    }
    
    pthread_mutex_unlock(&cr->mutex);
    return 0;
}

/*
 * A thread calling this function will block in the call until
 * the number of registered clients has reached zero.
 */
void creg_wait_for_empty(CLIENT_REGISTRY *cr) {
    if (cr == NULL) {
        return;
    }
    
    pthread_mutex_lock(&cr->mutex);
    int current_count = cr->count;
    pthread_mutex_unlock(&cr->mutex);
    
    // If already empty, return immediately
    if (current_count == 0) {
        return;
    }
    
    // Wait for semaphore (will be posted when count reaches 0)
    sem_wait(&cr->empty_sem);
}

/*
 * Shut down all the currently registered client file descriptors.
 */
void creg_shutdown_all(CLIENT_REGISTRY *cr) {
    if (cr == NULL) {
        return;
    }
    
    pthread_mutex_lock(&cr->mutex);
    
    debug_thread("Shutting down %d client connections", cr->count);
    
    // Shutdown all registered fds
    for (int i = 0; i < cr->count; i++) {
        shutdown(cr->fds[i], SHUT_RD);
    }
    
    pthread_mutex_unlock(&cr->mutex);
}

