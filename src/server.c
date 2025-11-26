#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <inttypes.h>

#include "server.h"
#include "protocol.h"
#include "trader.h"
#include "account.h"
#include "exchange.h"
#include "debug.h"

extern EXCHANGE *exchange;
extern CLIENT_REGISTRY *client_registry;

/*
 * Get current thread ID
 */
static inline unsigned long get_thread_id(void) {
    return syscall(SYS_gettid);
}

/*
 * Debug macro with thread ID format (matching demo_server)
 */
#define debug_thread(S, ...) \
    do { \
        fprintf(stderr, KMAG "DEBUG: %015lu: " KNRM S NL, get_thread_id(), ##__VA_ARGS__); \
    } while (0)

/*
 * Format timestamp from packet header
 */
static double format_timestamp(uint32_t sec, uint32_t nsec) {
    return (double)sec + (double)nsec / 1000000000.0;
}

/*
 * Format packet type name
 */
static const char *packet_type_name(BRS_PACKET_TYPE type) {
    switch (type) {
        case BRS_LOGIN_PKT: return "LOGIN";
        case BRS_STATUS_PKT: return "STATUS";
        case BRS_DEPOSIT_PKT: return "DEPOSIT";
        case BRS_WITHDRAW_PKT: return "WITHDRAW";
        case BRS_ESCROW_PKT: return "ESCROW";
        case BRS_RELEASE_PKT: return "RELEASE";
        case BRS_BUY_PKT: return "BUY";
        case BRS_SELL_PKT: return "SELL";
        case BRS_CANCEL_PKT: return "CANCEL";
        case BRS_ACK_PKT: return "ACK";
        case BRS_NACK_PKT: return "NACK";
        case BRS_BOUGHT_PKT: return "BOUGHT";
        case BRS_SOLD_PKT: return "SOLD";
        case BRS_POSTED_PKT: return "POSTED";
        case BRS_CANCELED_PKT: return "CANCELED";
        case BRS_TRADED_PKT: return "TRADED";
        default: return "UNKNOWN";
    }
}

/*
 * Thread function for the thread that handles a particular client.
 */
void *brs_client_service(void *arg) {
    int fd = *(int *)arg;
    free(arg);
    
    // Detach thread
    pthread_detach(pthread_self());
    
    // Register client
    creg_register(client_registry, fd);
    
    TRADER *trader = NULL;
    int logged_in = 0;
    char *trader_username = NULL;  // Store username for debug messages
    
    debug_thread("[%d] Starting client service", fd);
    
    // Main service loop
    while (1) {
        BRS_PACKET_HEADER hdr;
        void *payload = NULL;
        
        int result = proto_recv_packet(fd, &hdr, &payload);
        
        if (result == -1) {
            // Error or EOF
            if (errno == 0) {
                // EOF (clean connection close)
                debug("EOF received from client fd %d", fd);
            } else {
                error("Error receiving packet from client fd %d: %s", fd, strerror(errno));
            }
            break;
        }
        
        // result == 0 means success
        
        // Convert packet type and size from network byte order
        BRS_PACKET_TYPE type = (BRS_PACKET_TYPE)hdr.type;
        uint16_t payload_size = ntohs(hdr.size);
        double timestamp = format_timestamp(ntohl(hdr.timestamp_sec), ntohl(hdr.timestamp_nsec));
        
        // Log incoming packet
        if (type == BRS_LOGIN_PKT && payload != NULL && payload_size > 0) {
            char *username = malloc(payload_size + 1);
            memcpy(username, payload, payload_size);
            username[payload_size] = '\0';
            debug_thread("<= %.9f: type=%s, size=%d, user: '%s'", timestamp, packet_type_name(type), payload_size, username);
            free(username);
        } else if (type == BRS_DEPOSIT_PKT && payload != NULL && payload_size == sizeof(BRS_FUNDS_INFO)) {
            BRS_FUNDS_INFO *info = (BRS_FUNDS_INFO *)payload;
            debug_thread("<= %.9f: type=%s, size=%d, amount: %u", timestamp, packet_type_name(type), payload_size, ntohl(info->amount));
        } else if (type == BRS_WITHDRAW_PKT && payload != NULL && payload_size == sizeof(BRS_FUNDS_INFO)) {
            BRS_FUNDS_INFO *info = (BRS_FUNDS_INFO *)payload;
            debug_thread("<= %.9f: type=%s, size=%d, amount: %u", timestamp, packet_type_name(type), payload_size, ntohl(info->amount));
        } else if (type == BRS_ESCROW_PKT && payload != NULL && payload_size == sizeof(BRS_ESCROW_INFO)) {
            BRS_ESCROW_INFO *info = (BRS_ESCROW_INFO *)payload;
            debug_thread("<= %.9f: type=%s, size=%d, quantity: %u", timestamp, packet_type_name(type), payload_size, ntohl(info->quantity));
        } else if (type == BRS_RELEASE_PKT && payload != NULL && payload_size == sizeof(BRS_ESCROW_INFO)) {
            BRS_ESCROW_INFO *info = (BRS_ESCROW_INFO *)payload;
            debug_thread("<= %.9f: type=%s, size=%d, quantity: %u", timestamp, packet_type_name(type), payload_size, ntohl(info->quantity));
        } else if (type == BRS_BUY_PKT && payload != NULL && payload_size == sizeof(BRS_ORDER_INFO)) {
            BRS_ORDER_INFO *info = (BRS_ORDER_INFO *)payload;
            debug_thread("<= %.9f: type=%s, size=%d, quantity: %u, price: %u", timestamp, packet_type_name(type), payload_size, ntohl(info->quantity), ntohl(info->price));
        } else if (type == BRS_SELL_PKT && payload != NULL && payload_size == sizeof(BRS_ORDER_INFO)) {
            BRS_ORDER_INFO *info = (BRS_ORDER_INFO *)payload;
            debug_thread("<= %.9f: type=%s, size=%d, quantity: %u, price: %u", timestamp, packet_type_name(type), payload_size, ntohl(info->quantity), ntohl(info->price));
        } else if (type == BRS_CANCEL_PKT && payload != NULL && payload_size == sizeof(BRS_CANCEL_INFO)) {
            BRS_CANCEL_INFO *info = (BRS_CANCEL_INFO *)payload;
            debug_thread("<= %.9f: type=%s, size=%d, order: %u", timestamp, packet_type_name(type), payload_size, ntohl(info->order));
        } else if (type == BRS_STATUS_PKT) {
            debug_thread("<= %.9f: type=%s, size=%d (no payload)", timestamp, packet_type_name(type), payload_size);
        } else {
            debug_thread("<= %.9f: type=%s, size=%d", timestamp, packet_type_name(type), payload_size);
        }
        
        debug_thread("[%d] %s packet received", fd, packet_type_name(type));
        
        // Handle LOGIN before login
        if (!logged_in) {
            if (type == BRS_LOGIN_PKT) {
                // Extract username from payload
                if (payload_size == 0 || payload == NULL) {
                    // Send NACK
                    BRS_PACKET_HEADER nack_hdr;
                    nack_hdr.type = BRS_NACK_PKT;
                    nack_hdr.size = 0;
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    nack_hdr.timestamp_sec = htonl(ts.tv_sec);
                    nack_hdr.timestamp_nsec = htonl(ts.tv_nsec);
                    proto_send_packet(fd, &nack_hdr, NULL);
                    free(payload);
                    continue;
                }
                
                // Username is not null-terminated, so we need to add null terminator
                char *username = malloc(payload_size + 1);
                if (username == NULL) {
                    free(payload);
                    break;
                }
                memcpy(username, payload, payload_size);
                username[payload_size] = '\0';
                
                // Try to login
                debug_thread("[%d] Login '%s'", fd, username);
                trader = trader_login(fd, username);
                
                if (trader != NULL) {
                    logged_in = 1;
                    // Store username for debug messages
                    trader_username = malloc(strlen(username) + 1);
                    if (trader_username != NULL) {
                        strcpy(trader_username, username);
                    }
                }
                free(username);
                
                if (trader != NULL) {
                    // Send ACK
                    trader_send_ack(trader, NULL);
                } else {
                    // Send NACK
                    BRS_PACKET_HEADER nack_hdr;
                    nack_hdr.type = BRS_NACK_PKT;
                    nack_hdr.size = 0;
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    nack_hdr.timestamp_sec = htonl(ts.tv_sec);
                    nack_hdr.timestamp_nsec = htonl(ts.tv_nsec);
                    proto_send_packet(fd, &nack_hdr, NULL);
                }
                
                free(payload);
                continue;
            } else {
                // Not logged in and not LOGIN packet - send NACK
                BRS_PACKET_HEADER nack_hdr;
                nack_hdr.type = BRS_NACK_PKT;
                nack_hdr.size = 0;
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                nack_hdr.timestamp_sec = htonl(ts.tv_sec);
                nack_hdr.timestamp_nsec = htonl(ts.tv_nsec);
                proto_send_packet(fd, &nack_hdr, NULL);
                free(payload);
                continue;
            }
        }
        
        // After login, handle other commands
        switch (type) {
            case BRS_LOGIN_PKT:
                // Already logged in - send NACK
                trader_send_nack(trader);
                free(payload);
                break;
                
            case BRS_STATUS_PKT: {
                debug_thread("Get status of exchange %p", exchange);
                BRS_STATUS_INFO info;
                exchange_get_status(exchange, trader_get_account(trader), &info);
                trader_send_ack(trader, &info);
                free(payload);
                break;
            }
            
            case BRS_DEPOSIT_PKT: {
                if (payload_size != sizeof(BRS_FUNDS_INFO) || payload == NULL) {
                    trader_send_nack(trader);
                    free(payload);
                    break;
                }
                
                BRS_FUNDS_INFO *funds_info = (BRS_FUNDS_INFO *)payload;
                funds_t amount = ntohl(funds_info->amount);
                
                ACCOUNT *account = trader_get_account(trader);
                account_increase_balance(account, amount);
                
                debug_thread("Get status of exchange %p", exchange);
                
                BRS_STATUS_INFO info;
                exchange_get_status(exchange, account, &info);
                trader_send_ack(trader, &info);
                free(payload);
                break;
            }
            
            case BRS_WITHDRAW_PKT: {
                if (payload_size != sizeof(BRS_FUNDS_INFO) || payload == NULL) {
                    trader_send_nack(trader);
                    free(payload);
                    break;
                }
                
                BRS_FUNDS_INFO *funds_info = (BRS_FUNDS_INFO *)payload;
                funds_t amount = ntohl(funds_info->amount);
                
                ACCOUNT *account = trader_get_account(trader);
                // Get current balance before withdraw
                BRS_STATUS_INFO temp_info;
                account_get_status(account, &temp_info);
                funds_t old_balance = ntohl(temp_info.balance);
                
                if (account_decrease_balance(account, amount) != 0) {
                    debug_thread("Account '%s' balance %u is less than debit amount %u", trader_username ? trader_username : "unknown", old_balance, amount);
                    trader_send_nack(trader);
                } else {
                    debug_thread("Account '%s': decrease balance (%u -> %u)", trader_username ? trader_username : "unknown", old_balance, old_balance - amount);
                    debug_thread("Get status of exchange %p", exchange);
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, account, &info);
                    trader_send_ack(trader, &info);
                }
                free(payload);
                break;
            }
            
            case BRS_ESCROW_PKT: {
                if (payload_size != sizeof(BRS_ESCROW_INFO) || payload == NULL) {
                    trader_send_nack(trader);
                    free(payload);
                    break;
                }
                
                BRS_ESCROW_INFO *escrow_info = (BRS_ESCROW_INFO *)payload;
                quantity_t quantity = ntohl(escrow_info->quantity);
                
                ACCOUNT *account = trader_get_account(trader);
                account_increase_inventory(account, quantity);
                
                debug_thread("Get status of exchange %p", exchange);
                
                BRS_STATUS_INFO info;
                exchange_get_status(exchange, account, &info);
                trader_send_ack(trader, &info);
                free(payload);
                break;
            }
            
            case BRS_RELEASE_PKT: {
                if (payload_size != sizeof(BRS_ESCROW_INFO) || payload == NULL) {
                    trader_send_nack(trader);
                    free(payload);
                    break;
                }
                
                BRS_ESCROW_INFO *escrow_info = (BRS_ESCROW_INFO *)payload;
                quantity_t quantity = ntohl(escrow_info->quantity);
                
                ACCOUNT *account = trader_get_account(trader);
                // Get current inventory before release
                BRS_STATUS_INFO temp_info;
                account_get_status(account, &temp_info);
                quantity_t old_inventory = ntohl(temp_info.inventory);
                
                if (account_decrease_inventory(account, quantity) != 0) {
                    debug_thread("Account '%s' inventory %u is less than quantity %u to decrease by", trader_username ? trader_username : "unknown", old_inventory, quantity);
                    trader_send_nack(trader);
                } else {
                    debug_thread("Get status of exchange %p", exchange);
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, account, &info);
                    trader_send_ack(trader, &info);
                }
                free(payload);
                break;
            }
            
            case BRS_BUY_PKT: {
                if (payload_size != sizeof(BRS_ORDER_INFO) || payload == NULL) {
                    trader_send_nack(trader);
                    free(payload);
                    break;
                }
                
                BRS_ORDER_INFO *order_info = (BRS_ORDER_INFO *)payload;
                quantity_t quantity = ntohl(order_info->quantity);
                funds_t price = ntohl(order_info->price);
                
                debug_thread("brs buy: quantity: %u, limit: %u", quantity, price);
                
                ACCOUNT *account = trader_get_account(trader);
                orderid_t order_id = exchange_post_buy(exchange, trader, quantity, price);
                
                if (order_id == 0) {
                    trader_send_nack(trader);
                } else {
                    // Broadcast POSTED notification before ACK
                    debug_thread("Attempt to broadcast");
                    BRS_PACKET_HEADER hdr;
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    hdr.timestamp_sec = htonl(ts.tv_sec);
                    hdr.timestamp_nsec = htonl(ts.tv_nsec);
                    hdr.type = BRS_POSTED_PKT;
                    hdr.size = htons(sizeof(BRS_NOTIFY_INFO));
                    BRS_NOTIFY_INFO notify;
                    notify.buyer = htonl(order_id);
                    notify.seller = 0;
                    notify.quantity = htonl(quantity);
                    notify.price = htonl(price);
                    trader_broadcast_packet(&hdr, &notify);
                    
                    debug_thread("Get status of exchange %p", exchange);
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, account, &info);
                    info.orderid = htonl(order_id);
                    trader_send_ack(trader, &info);
                }
                free(payload);
                break;
            }
            
            case BRS_SELL_PKT: {
                if (payload_size != sizeof(BRS_ORDER_INFO) || payload == NULL) {
                    trader_send_nack(trader);
                    free(payload);
                    break;
                }
                
                BRS_ORDER_INFO *order_info = (BRS_ORDER_INFO *)payload;
                quantity_t quantity = ntohl(order_info->quantity);
                funds_t price = ntohl(order_info->price);
                
                debug_thread("brs_sell: quantity: %u, limit: %u", quantity, price);
                
                ACCOUNT *account = trader_get_account(trader);
                // Check inventory before posting
                BRS_STATUS_INFO temp_info;
                account_get_status(account, &temp_info);
                quantity_t inventory = ntohl(temp_info.inventory);
                
                orderid_t order_id = exchange_post_sell(exchange, trader, quantity, price);
                
                if (order_id == 0) {
                    debug_thread("Account '%s' inventory %u is less than quantity %u to decrease by", trader_username ? trader_username : "unknown", inventory, quantity);
                    trader_send_nack(trader);
                } else {
                    debug_thread("Get status of exchange %p", exchange);
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, account, &info);
                    info.orderid = htonl(order_id);
                    trader_send_ack(trader, &info);
                    
                    // Broadcast POSTED notification after ACK
                    BRS_PACKET_HEADER hdr;
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    hdr.timestamp_sec = htonl(ts.tv_sec);
                    hdr.timestamp_nsec = htonl(ts.tv_nsec);
                    hdr.type = BRS_POSTED_PKT;
                    hdr.size = htons(sizeof(BRS_NOTIFY_INFO));
                    BRS_NOTIFY_INFO notify;
                    notify.buyer = 0;
                    notify.seller = htonl(order_id);
                    notify.quantity = htonl(quantity);
                    notify.price = htonl(price);
                    trader_broadcast_packet(&hdr, &notify);
                }
                free(payload);
                break;
            }
            
            case BRS_CANCEL_PKT: {
                if (payload_size != sizeof(BRS_CANCEL_INFO) || payload == NULL) {
                    trader_send_nack(trader);
                    free(payload);
                    break;
                }
                
                BRS_CANCEL_INFO *cancel_info = (BRS_CANCEL_INFO *)payload;
                orderid_t order = ntohl(cancel_info->order);
                quantity_t quantity;
                
                debug_thread("brs_cancel: order: %u", order);
                debug_thread("Exchange %p trying to cancel order %u", exchange, order);
                
                if (exchange_cancel(exchange, trader, order, &quantity) != 0) {
                    debug_thread("Order to be canceled does not exist in exchange");
                    trader_send_nack(trader);
                } else {
                    debug_thread("Get status of exchange %p", exchange);
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, trader_get_account(trader), &info);
                    info.orderid = htonl(order);
                    info.quantity = htonl(quantity);
                    trader_send_ack(trader, &info);
                }
                free(payload);
                break;
            }
            
            default:
                // Unknown packet type - send NACK
                trader_send_nack(trader);
                free(payload);
                break;
        }
    }
    
    // Cleanup
    if (trader != NULL) {
        trader_logout(trader);
        trader_unref(trader, "client disconnect");
    }
    
    if (trader_username != NULL) {
        free(trader_username);
    }
    
    creg_unregister(client_registry, fd);
    close(fd);
    
    debug_thread("Client service thread terminating for fd %d", fd);
    return NULL;
}

