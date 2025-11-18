#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <arpa/inet.h>

#include "server.h"
#include "protocol.h"
#include "trader.h"
#include "account.h"
#include "exchange.h"
#include "debug.h"

extern EXCHANGE *exchange;
extern CLIENT_REGISTRY *client_registry;

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
    
    debug("Client service thread started for fd %d", fd);
    
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
        
        debug("Received packet type %d from fd %d", type, fd);
        
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
                trader = trader_login(fd, username);
                free(username);
                
                if (trader != NULL) {
                    logged_in = 1;
                    // Send ACK
                    BRS_PACKET_HEADER ack_hdr;
                    ack_hdr.type = BRS_ACK_PKT;
                    ack_hdr.size = 0;
                    struct timespec ts;
                    clock_gettime(CLOCK_REALTIME, &ts);
                    ack_hdr.timestamp_sec = htonl(ts.tv_sec);
                    ack_hdr.timestamp_nsec = htonl(ts.tv_nsec);
                    proto_send_packet(fd, &ack_hdr, NULL);
                    debug("Client fd %d logged in successfully", fd);
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
                
                account_increase_balance(trader_get_account(trader), amount);
                
                BRS_STATUS_INFO info;
                exchange_get_status(exchange, trader_get_account(trader), &info);
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
                
                if (account_decrease_balance(trader_get_account(trader), amount) != 0) {
                    trader_send_nack(trader);
                } else {
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, trader_get_account(trader), &info);
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
                
                account_increase_inventory(trader_get_account(trader), quantity);
                
                BRS_STATUS_INFO info;
                exchange_get_status(exchange, trader_get_account(trader), &info);
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
                
                if (account_decrease_inventory(trader_get_account(trader), quantity) != 0) {
                    trader_send_nack(trader);
                } else {
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, trader_get_account(trader), &info);
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
                
                orderid_t order_id = exchange_post_buy(exchange, trader, quantity, price);
                
                if (order_id == 0) {
                    trader_send_nack(trader);
                } else {
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, trader_get_account(trader), &info);
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
                    notify.buyer = htonl(order_id);
                    notify.seller = 0;
                    notify.quantity = htonl(quantity);
                    notify.price = htonl(price);
                    trader_broadcast_packet(&hdr, &notify);
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
                
                orderid_t order_id = exchange_post_sell(exchange, trader, quantity, price);
                
                if (order_id == 0) {
                    trader_send_nack(trader);
                } else {
                    BRS_STATUS_INFO info;
                    exchange_get_status(exchange, trader_get_account(trader), &info);
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
                
                if (exchange_cancel(exchange, trader, order, &quantity) != 0) {
                    trader_send_nack(trader);
                } else {
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
    
    creg_unregister(client_registry, fd);
    close(fd);
    
    debug("Client service thread terminating for fd %d", fd);
    return NULL;
}

