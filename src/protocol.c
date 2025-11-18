#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "debug.h"

/*
 * Helper function to write all bytes, looping until complete or error
 */
static ssize_t full_write(int fd, const void *buf, size_t count) {
    size_t total = 0;
    const char *ptr = (const char *)buf;
    
    while (total < count) {
        ssize_t n = write(fd, ptr + total, count - total);
        if (n == -1) {
            if (errno == EINTR) {
                continue; // Interrupted, retry
            }
            return -1; // Error
        }
        if (n == 0) {
            return total; // EOF
        }
        total += n;
    }
    return total;
}

/*
 * Helper function to read all bytes, looping until complete or error
 */
static ssize_t full_read(int fd, void *buf, size_t count) {
    size_t total = 0;
    char *ptr = (char *)buf;
    
    while (total < count) {
        ssize_t n = read(fd, ptr + total, count - total);
        if (n == -1) {
            if (errno == EINTR) {
                continue; // Interrupted, retry
            }
            return -1; // Error
        }
        if (n == 0) {
            return total; // EOF
        }
        total += n;
    }
    return total;
}

/*
 * Send a packet, which consists of a fixed-size header followed by an
 * optional associated data payload.
 */
int proto_send_packet(int fd, BRS_PACKET_HEADER *hdr, void *payload) {
    if (hdr == NULL) {
        errno = EINVAL;
        return -1;
    }
    
    // Write header
    ssize_t n = full_write(fd, hdr, sizeof(BRS_PACKET_HEADER));
    if (n != sizeof(BRS_PACKET_HEADER)) {
        if (n == 0) {
            errno = EPIPE; // Connection closed
        }
        return -1;
    }
    
    // Write payload if present
    uint16_t payload_size = ntohs(hdr->size);
    if (payload_size > 0) {
        if (payload == NULL) {
            errno = EINVAL;
            return -1;
        }
        
        n = full_write(fd, payload, payload_size);
        if (n != payload_size) {
            if (n == 0) {
                errno = EPIPE; // Connection closed
            }
            return -1;
        }
    }
    
    return 0;
}

/*
 * Receive a packet, blocking until one is available.
 */
int proto_recv_packet(int fd, BRS_PACKET_HEADER *hdr, void **payloadp) {
    if (hdr == NULL || payloadp == NULL) {
        errno = EINVAL;
        return -1;
    }
    
    // Initialize payload pointer
    *payloadp = NULL;
    
    // Read header
    ssize_t n = full_read(fd, hdr, sizeof(BRS_PACKET_HEADER));
    if (n == 0) {
        // EOF before reading any bytes - connection closed cleanly
        errno = 0; // Indicate EOF (clean close)
        return -1;
    }
    if (n != sizeof(BRS_PACKET_HEADER)) {
        if (n == -1) {
            return -1; // Error already set
        }
        errno = EPIPE; // Incomplete header
        return -1;
    }
    
    // Get payload size (convert from network byte order)
    uint16_t payload_size = ntohs(hdr->size);
    
    // Read payload if present
    if (payload_size > 0) {
        void *payload = malloc(payload_size);
        if (payload == NULL) {
            errno = ENOMEM;
            return -1;
        }
        
        n = full_read(fd, payload, payload_size);
        if (n != payload_size) {
            free(payload);
            if (n == 0) {
                errno = EPIPE; // EOF before reading full payload
            }
            return -1;
        }
        
        *payloadp = payload;
    }
    
    return 0;
}

