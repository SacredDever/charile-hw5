#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>

#include "trader.h"
#include "protocol.h"
#include "debug.h"

struct trader {
    int fd;
    char *name;
    ACCOUNT *account;
    pthread_mutex_t mutex;  // Must be recursive
    int refcount;
};

// Global trader map
static struct trader_map_entry {
    char *name;
    TRADER *trader;
} trader_map[MAX_TRADERS];

static int trader_count = 0;
static pthread_mutex_t trader_map_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Initialize the traders module.
 */
int traders_init(void) {
    trader_count = 0;
    memset(trader_map, 0, sizeof(trader_map));
    return 0;
}

/*
 * Finalize the traders module, freeing all associated resources.
 */
void traders_fini(void) {
    pthread_mutex_lock(&trader_map_mutex);
    
    for (int i = 0; i < trader_count; i++) {
        if (trader_map[i].trader != NULL) {
            TRADER *trader = trader_map[i].trader;
            pthread_mutex_lock(&trader->mutex);
            trader->refcount = 1; // Set to 1 so unref will free it
            pthread_mutex_unlock(&trader->mutex);
            trader_unref(trader, "fini");
        }
        free(trader_map[i].name);
    }
    
    trader_count = 0;
    pthread_mutex_unlock(&trader_map_mutex);
}

/*
 * Attempt to log in a trader with a specified user name.
 */
TRADER *trader_login(int fd, char *name) {
    if (fd < 0 || name == NULL) {
        return NULL;
    }
    
    pthread_mutex_lock(&trader_map_mutex);
    
    // Check if max traders reached
    if (trader_count >= MAX_TRADERS) {
        pthread_mutex_unlock(&trader_map_mutex);
        return NULL;
    }
    
    // Create new trader
    TRADER *trader = malloc(sizeof(TRADER));
    if (trader == NULL) {
        pthread_mutex_unlock(&trader_map_mutex);
        return NULL;
    }
    
    trader->fd = fd;
    trader->refcount = 1;
    
    // Copy name
    trader->name = malloc(strlen(name) + 1);
    if (trader->name == NULL) {
        free(trader);
        pthread_mutex_unlock(&trader_map_mutex);
        return NULL;
    }
    strcpy(trader->name, name);
    
    // Get account
    trader->account = account_lookup(name);
    if (trader->account == NULL) {
        free(trader->name);
        free(trader);
        pthread_mutex_unlock(&trader_map_mutex);
        return NULL;
    }
    
    // Initialize recursive mutex
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    if (pthread_mutex_init(&trader->mutex, &attr) != 0) {
        pthread_mutexattr_destroy(&attr);
        free(trader->name);
        free(trader);
        pthread_mutex_unlock(&trader_map_mutex);
        return NULL;
    }
    pthread_mutexattr_destroy(&attr);
    
    // Add to map
    char *name_copy = malloc(strlen(name) + 1);
    if (name_copy == NULL) {
        pthread_mutex_destroy(&trader->mutex);
        free(trader->name);
        free(trader);
        pthread_mutex_unlock(&trader_map_mutex);
        return NULL;
    }
    strcpy(name_copy, name);
    
    trader_map[trader_count].name = name_copy;
    trader_map[trader_count].trader = trader;
    trader_count++;
    
    debug("Trader logged in: %s (fd: %d)", name, fd);
    
    pthread_mutex_unlock(&trader_map_mutex);
    return trader;
}

/*
 * Log out a trader.
 */
void trader_logout(TRADER *trader) {
    if (trader == NULL) {
        return;
    }
    
    pthread_mutex_lock(&trader_map_mutex);
    
    // Remove from map
    for (int i = 0; i < trader_count; i++) {
        if (trader_map[i].trader == trader) {
            free(trader_map[i].name);
            // Move last element to this position
            trader_map[i] = trader_map[trader_count - 1];
            trader_count--;
            break;
        }
    }
    
    pthread_mutex_unlock(&trader_map_mutex);
    
    // Unref the trader (consumes one reference)
    trader_unref(trader, "logout");
}

/*
 * Increase the reference count on a trader by one.
 */
TRADER *trader_ref(TRADER *trader, char *why) {
    if (trader == NULL) {
        return NULL;
    }
    
    pthread_mutex_lock(&trader->mutex);
    trader->refcount++;
    debug("trader_ref: %s (refcount: %d) - %s", trader->name, trader->refcount, why);
    pthread_mutex_unlock(&trader->mutex);
    
    return trader;
}

/*
 * Decrease the reference count on a trader by one.
 */
void trader_unref(TRADER *trader, char *why) {
    if (trader == NULL) {
        return;
    }
    
    pthread_mutex_lock(&trader->mutex);
    
    trader->refcount--;
    debug("trader_unref: %s (refcount: %d) - %s", trader->name, trader->refcount, why);
    
    if (trader->refcount < 0) {
        error("trader_unref: refcount went negative for %s", trader->name);
        abort();
    }
    
    if (trader->refcount == 0) {
        // Free resources
        if (trader->fd >= 0) {
            close(trader->fd);
        }
        free(trader->name);
        pthread_mutex_unlock(&trader->mutex);
        pthread_mutex_destroy(&trader->mutex);
        free(trader);
        return;
    }
    
    pthread_mutex_unlock(&trader->mutex);
}

/*
 * Get the account associated with a trader.
 */
ACCOUNT *trader_get_account(TRADER *trader) {
    if (trader == NULL) {
        return NULL;
    }
    return trader->account;
}

/*
 * Send a packet to the client for a trader.
 */
int trader_send_packet(TRADER *trader, BRS_PACKET_HEADER *pkt, void *data) {
    if (trader == NULL || pkt == NULL) {
        return -1;
    }
    
    pthread_mutex_lock(&trader->mutex);
    int result = proto_send_packet(trader->fd, pkt, data);
    pthread_mutex_unlock(&trader->mutex);
    
    return result;
}

/*
 * Broadcast a packet to all currently logged-in traders.
 */
int trader_broadcast_packet(BRS_PACKET_HEADER *pkt, void *data) {
    if (pkt == NULL) {
        return -1;
    }
    
    // Create a copy of the packet for each trader
    // We need to copy the payload if present
    void *payload_copy = NULL;
    uint16_t payload_size = ntohs(pkt->size);
    if (payload_size > 0 && data != NULL) {
        payload_copy = malloc(payload_size);
        if (payload_copy == NULL) {
            return -1;
        }
        memcpy(payload_copy, data, payload_size);
    }
    
    pthread_mutex_lock(&trader_map_mutex);
    
    // Create array of traders to broadcast to
    TRADER **traders = malloc(sizeof(TRADER *) * trader_count);
    if (traders == NULL) {
        pthread_mutex_unlock(&trader_map_mutex);
        if (payload_copy != NULL) {
            free(payload_copy);
        }
        return -1;
    }
    
    int count = 0;
    for (int i = 0; i < trader_count; i++) {
        if (trader_map[i].trader != NULL) {
            traders[count] = trader_ref(trader_map[i].trader, "broadcast");
            count++;
        }
    }
    
    pthread_mutex_unlock(&trader_map_mutex);
    
    // Send to all traders
    int result = 0;
    for (int i = 0; i < count; i++) {
        // Create a copy of the header for each send
        BRS_PACKET_HEADER hdr_copy = *pkt;
        void *data_copy = NULL;
        
        if (payload_size > 0 && payload_copy != NULL) {
            data_copy = malloc(payload_size);
            if (data_copy != NULL) {
                memcpy(data_copy, payload_copy, payload_size);
            }
        }
        
        if (trader_send_packet(traders[i], &hdr_copy, data_copy) != 0) {
            result = -1;
        }
        
        if (data_copy != NULL) {
            free(data_copy);
        }
        
        trader_unref(traders[i], "broadcast");
    }
    
    free(traders);
    if (payload_copy != NULL) {
        free(payload_copy);
    }
    
    return result;
}

/*
 * Send an ACK packet to the client for a trader.
 */
int trader_send_ack(TRADER *trader, BRS_STATUS_INFO *info) {
    if (trader == NULL) {
        return -1;
    }
    
    BRS_PACKET_HEADER hdr;
    hdr.type = BRS_ACK_PKT;
    
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    hdr.timestamp_sec = htonl(ts.tv_sec);
    hdr.timestamp_nsec = htonl(ts.tv_nsec);
    
    if (info != NULL) {
        // Copy info and ensure network byte order
        BRS_STATUS_INFO info_copy = *info;
        hdr.size = htons(sizeof(BRS_STATUS_INFO));
        return trader_send_packet(trader, &hdr, &info_copy);
    } else {
        hdr.size = 0;
        return trader_send_packet(trader, &hdr, NULL);
    }
}

/*
 * Send an NACK packet to the client for a trader.
 */
int trader_send_nack(TRADER *trader) {
    if (trader == NULL) {
        return -1;
    }
    
    BRS_PACKET_HEADER hdr;
    hdr.type = BRS_NACK_PKT;
    hdr.size = 0;
    
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    hdr.timestamp_sec = htonl(ts.tv_sec);
    hdr.timestamp_nsec = htonl(ts.tv_nsec);
    
    return trader_send_packet(trader, &hdr, NULL);
}

