#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <sys/syscall.h>

#include "client_registry.h"
#include "exchange.h"
#include "account.h"
#include "trader.h"
#include "server.h"
#include "debug.h"

extern EXCHANGE *exchange;
extern CLIENT_REGISTRY *client_registry;

static volatile sig_atomic_t shutdown_flag = 0;
static int listen_fd = -1;

static void terminate(int status);
static void sighup_handler(int sig);

/*
 * "Bourse" exchange server.
 *
 * Usage: bourse -p <port>
 */
int main(int argc, char* argv[]){
    int port = 0;
    int opt;
    
    // Parse command-line arguments
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
            case 'p':
                port = atoi(optarg);
                if (port <= 0 || port > 65535) {
                    fprintf(stderr, "Invalid port number: %s\n", optarg);
                    fprintf(stderr, "Usage: %s -p <port>\n", argv[0]);
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                fprintf(stderr, "Usage: %s -p <port>\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
    
    if (port == 0) {
        fprintf(stderr, "Port number is required\n");
        fprintf(stderr, "Usage: %s -p <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // Install SIGHUP handler
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sighup_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGHUP, &sa, NULL) == -1) {
        error("Failed to install SIGHUP handler");
        exit(EXIT_FAILURE);
    }

    // Perform required initializations
    debug("%lu: Initialize client registry", (unsigned long)syscall(SYS_gettid));
    client_registry = creg_init();
    if (client_registry == NULL) {
        error("Failed to initialize client registry");
        exit(EXIT_FAILURE);
    }
    
    debug("%lu: Initialize accounts module", (unsigned long)syscall(SYS_gettid));
    if (accounts_init() != 0) {
        error("Failed to initialize accounts");
        terminate(EXIT_FAILURE);
    }
    
    debug("%lu: Initialize trader module", (unsigned long)syscall(SYS_gettid));
    if (traders_init() != 0) {
        error("Failed to initialize traders");
        terminate(EXIT_FAILURE);
    }
    
    exchange = exchange_init();
    if (exchange == NULL) {
        error("Failed to initialize exchange");
        terminate(EXIT_FAILURE);
    }
    debug("Initialized exchange %p", exchange);

    // Create listening socket
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        error("Failed to create socket");
        terminate(EXIT_FAILURE);
    }

    // Set socket options to allow reuse of address
    int optval = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
        error("Failed to set socket options");
        close(listen_fd);
        terminate(EXIT_FAILURE);
    }

    // Bind socket to port
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        error("Failed to bind socket to port %d: %s", port, strerror(errno));
        close(listen_fd);
        terminate(EXIT_FAILURE);
    }

    // Listen for connections
    if (listen(listen_fd, 10) == -1) {
        error("Failed to listen on socket");
        close(listen_fd);
        terminate(EXIT_FAILURE);
    }

    info("Bourse server listening on port %d", port);

    // Accept loop
    while (!shutdown_flag) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd == -1) {
            if (shutdown_flag) {
                // Accept was interrupted by shutdown
                break;
            }
            error("Failed to accept connection");
            continue;
        }

        // Allocate memory for file descriptor to pass to thread
        int *fd_ptr = malloc(sizeof(int));
        if (fd_ptr == NULL) {
            error("Failed to allocate memory for client fd");
            close(client_fd);
            continue;
        }
        *fd_ptr = client_fd;

        // Create thread for client
        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, brs_client_service, fd_ptr) != 0) {
            error("Failed to create client thread");
            free(fd_ptr);
            close(client_fd);
            continue;
        }
        
        // Detach thread so we don't need to join it
        pthread_detach(thread_id);
    }

    // Close listening socket
    if (listen_fd != -1) {
        close(listen_fd);
        listen_fd = -1;
    }

    terminate(EXIT_SUCCESS);
    return 0;
}

/*
 * SIGHUP handler - triggers clean shutdown
 */
static void sighup_handler(int sig) {
    (void)sig; // Suppress unused parameter warning
    shutdown_flag = 1;
    if (listen_fd != -1) {
        close(listen_fd);
        listen_fd = -1;
    }
}

/*
 * Function called to cleanly shut down the server.
 */
static void terminate(int status) {
    // Shutdown all client connections.
    // This will trigger the eventual termination of service threads.
    creg_shutdown_all(client_registry);
    
    debug("Waiting for service threads to terminate...");
    creg_wait_for_empty(client_registry);
    debug("All service threads terminated.");

    // Finalize modules.
    creg_fini(client_registry);
    exchange_fini(exchange);
    traders_fini();
    accounts_fini();

    debug("Bourse server terminating");
    exit(status);
}
