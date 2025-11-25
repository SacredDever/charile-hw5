#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <time.h>

#include "exchange.h"
#include "protocol.h"
#include "debug.h"
#include <unistd.h>
#include <sys/syscall.h>

/*
 * Debug macro with thread ID format (matching demo_server)
 */
#define debug_thread(S, ...) \
    do { \
        fprintf(stderr, KMAG "DEBUG: %015lu: " KNRM S NL, (unsigned long)syscall(SYS_gettid), ##__VA_ARGS__); \
    } while (0)

#define MAX_ORDERS 1024

typedef enum {
    ORDER_BUY,
    ORDER_SELL
} order_type_t;

struct order {
    orderid_t id;
    TRADER *trader;
    order_type_t type;
    quantity_t quantity;
    funds_t price;  // max price for buy, min price for sell
    struct order *next;
};

struct exchange {
    struct order *buy_orders;      // List of buy orders
    struct order *sell_orders;      // List of sell orders
    pthread_mutex_t mutex;
    sem_t matchmaker_sem;           // Semaphore to wake matchmaker
    funds_t last_trade_price;
    orderid_t next_order_id;
    pthread_t matchmaker_thread;
    volatile int running;
};

static void *matchmaker_thread_func(void *arg);

/*
 * Initialize a new exchange.
 */
EXCHANGE *exchange_init() {
    EXCHANGE *xchg = malloc(sizeof(EXCHANGE));
    if (xchg == NULL) {
        return NULL;
    }
    
    xchg->buy_orders = NULL;
    xchg->sell_orders = NULL;
    xchg->last_trade_price = 0;
    xchg->next_order_id = 1;
    xchg->running = 1;
    
    if (pthread_mutex_init(&xchg->mutex, NULL) != 0) {
        free(xchg);
        return NULL;
    }
    
    if (sem_init(&xchg->matchmaker_sem, 0, 0) != 0) {
        pthread_mutex_destroy(&xchg->mutex);
        free(xchg);
        return NULL;
    }
    
    // Start matchmaker thread
    if (pthread_create(&xchg->matchmaker_thread, NULL, matchmaker_thread_func, xchg) != 0) {
        sem_destroy(&xchg->matchmaker_sem);
        pthread_mutex_destroy(&xchg->mutex);
        free(xchg);
        return NULL;
    }
    
    debug_thread("Matchmaker for exchange %p starting", xchg);
    debug_thread("Matchmaker for exchange %p sleeping", xchg);
    
    return xchg;
}

/*
 * Finalize an exchange, freeing all associated resources.
 */
void exchange_fini(EXCHANGE *xchg) {
    if (xchg == NULL) {
        return;
    }
    
    // Stop matchmaker thread
    xchg->running = 0;
    sem_post(&xchg->matchmaker_sem);
    pthread_join(xchg->matchmaker_thread, NULL);
    
    pthread_mutex_lock(&xchg->mutex);
    
    // Cancel all remaining orders and unencumber funds/inventory
    struct order *order = xchg->buy_orders;
    while (order != NULL) {
        struct order *next = order->next;
        
        // Refund encumbered funds
        ACCOUNT *account = trader_get_account(order->trader);
        account_increase_balance(account, order->quantity * order->price);
        
        trader_unref(order->trader, "exchange_fini");
        free(order);
        order = next;
    }
    
    order = xchg->sell_orders;
    while (order != NULL) {
        struct order *next = order->next;
        
        // Release encumbered inventory
        ACCOUNT *account = trader_get_account(order->trader);
        account_increase_inventory(account, order->quantity);
        
        trader_unref(order->trader, "exchange_fini");
        free(order);
        order = next;
    }
    
    pthread_mutex_unlock(&xchg->mutex);
    
    sem_destroy(&xchg->matchmaker_sem);
    pthread_mutex_destroy(&xchg->mutex);
    free(xchg);
}

/*
 * Find best buy order (highest price)
 */
static struct order *find_best_buy(EXCHANGE *xchg) {
    struct order *best = NULL;
    struct order *order = xchg->buy_orders;
    
    while (order != NULL) {
        if (best == NULL || order->price > best->price) {
            best = order;
        }
        order = order->next;
    }
    
    return best;
}

/*
 * Find best sell order (lowest price)
 */
static struct order *find_best_sell(EXCHANGE *xchg) {
    struct order *best = NULL;
    struct order *order = xchg->sell_orders;
    
    while (order != NULL) {
        if (best == NULL || order->price < best->price) {
            best = order;
        }
        order = order->next;
    }
    
    return best;
}

/*
 * Remove order from list
 */
static void remove_order(struct order **list, struct order *order) {
    if (*list == order) {
        *list = order->next;
    } else {
        struct order *prev = *list;
        while (prev != NULL && prev->next != order) {
            prev = prev->next;
        }
        if (prev != NULL) {
            prev->next = order->next;
        }
    }
}

/*
 * Matchmaker thread function
 */
static void *matchmaker_thread_func(void *arg) {
    EXCHANGE *xchg = (EXCHANGE *)arg;
    
    while (xchg->running) {
        // Wait for orders to change
        sem_wait(&xchg->matchmaker_sem);
        
        if (!xchg->running) {
            break;
        }
        
        pthread_mutex_lock(&xchg->mutex);
        
        // Match orders until no more matches
        while (1) {
            struct order *buy_order = find_best_buy(xchg);
            struct order *sell_order = find_best_sell(xchg);
            
            // Check if orders match
            if (buy_order == NULL || sell_order == NULL) {
                break;
            }
            
            if (buy_order->price < sell_order->price) {
                break; // No match
            }
            
            // Determine trade price (closest to last trade price within overlap)
            funds_t trade_price;
            funds_t min_price = sell_order->price;
            funds_t max_price = buy_order->price;
            
            if (xchg->last_trade_price == 0) {
                // No previous trade, use midpoint
                trade_price = (min_price + max_price) / 2;
            } else if (xchg->last_trade_price >= min_price && xchg->last_trade_price <= max_price) {
                // Last trade price is within overlap
                trade_price = xchg->last_trade_price;
            } else if (xchg->last_trade_price < min_price) {
                trade_price = min_price;
            } else {
                trade_price = max_price;
            }
            
            // Determine trade quantity
            quantity_t trade_qty = buy_order->quantity;
            if (sell_order->quantity < trade_qty) {
                trade_qty = sell_order->quantity;
            }
            
            // Store max price before updating order
            funds_t buy_max_price = buy_order->price;
            
            // Update orders
            buy_order->quantity -= trade_qty;
            sell_order->quantity -= trade_qty;
            
            // Update last trade price
            xchg->last_trade_price = trade_price;
            
            // Update accounts
            ACCOUNT *buyer_account = trader_get_account(buy_order->trader);
            ACCOUNT *seller_account = trader_get_account(sell_order->trader);
            
            // Seller gets proceeds
            account_increase_balance(seller_account, trade_price * trade_qty);
            
            // Buyer gets inventory
            account_increase_inventory(buyer_account, trade_qty);
            
            // Buyer refund calculation:
            // Originally encumbered: original_qty * buy_max_price
            // Amount actually used: trade_qty * trade_price
            // Should remain encumbered: buy_order->quantity * buy_max_price (after update)
            // Refund = original_encumbered - used - remaining_encumbered
            //        = (buy_order->quantity + trade_qty) * buy_max_price - trade_qty * trade_price - buy_order->quantity * buy_max_price
            //        = trade_qty * buy_max_price - trade_qty * trade_price
            //        = trade_qty * (buy_max_price - trade_price)
            funds_t refund = trade_qty * (buy_max_price - trade_price);
            if (refund > 0) {
                account_increase_balance(buyer_account, refund);
            }
            
            // Remove orders with zero quantity
            if (buy_order->quantity == 0) {
                remove_order(&xchg->buy_orders, buy_order);
            }
            if (sell_order->quantity == 0) {
                remove_order(&xchg->sell_orders, sell_order);
            }
            
            // Send notifications
            BRS_PACKET_HEADER hdr;
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            hdr.timestamp_sec = htonl(ts.tv_sec);
            hdr.timestamp_nsec = htonl(ts.tv_nsec);
            
            // BOUGHT notification to buyer
            if (buy_order->quantity == 0 || trade_qty > 0) {
                hdr.type = BRS_BOUGHT_PKT;
                hdr.size = htons(sizeof(BRS_NOTIFY_INFO));
                BRS_NOTIFY_INFO notify;
                notify.buyer = htonl(buy_order->id);
                notify.seller = htonl(sell_order->id);
                notify.quantity = htonl(trade_qty);
                notify.price = htonl(trade_price);
                trader_send_packet(buy_order->trader, &hdr, &notify);
            }
            
            // SOLD notification to seller
            if (sell_order->quantity == 0 || trade_qty > 0) {
                hdr.type = BRS_SOLD_PKT;
                hdr.size = htons(sizeof(BRS_NOTIFY_INFO));
                BRS_NOTIFY_INFO notify;
                notify.buyer = htonl(buy_order->id);
                notify.seller = htonl(sell_order->id);
                notify.quantity = htonl(trade_qty);
                notify.price = htonl(trade_price);
                trader_send_packet(sell_order->trader, &hdr, &notify);
            }
            
            // TRADED broadcast
            hdr.type = BRS_TRADED_PKT;
            hdr.size = htons(sizeof(BRS_NOTIFY_INFO));
            BRS_NOTIFY_INFO notify;
            notify.buyer = htonl(buy_order->id);
            notify.seller = htonl(sell_order->id);
            notify.quantity = htonl(trade_qty);
            notify.price = htonl(trade_price);
            trader_broadcast_packet(&hdr, &notify);
            
            // Free orders that were removed
            if (buy_order->quantity == 0) {
                trader_unref(buy_order->trader, "trade complete");
                free(buy_order);
            }
            if (sell_order->quantity == 0) {
                trader_unref(sell_order->trader, "trade complete");
                free(sell_order);
            }
        }
        
        pthread_mutex_unlock(&xchg->mutex);
    }
    
    return NULL;
}

/*
 * Get the current status of the exchange.
 */
void exchange_get_status(EXCHANGE *xchg, ACCOUNT *account, BRS_STATUS_INFO *infop) {
    if (xchg == NULL || infop == NULL) {
        return;
    }
    
    pthread_mutex_lock(&xchg->mutex);
    
    // Get account status if provided
    if (account != NULL) {
        account_get_status(account, infop);
    } else {
        memset(infop, 0, sizeof(BRS_STATUS_INFO));
    }
    
    // Find best bid and ask
    struct order *best_buy = find_best_buy(xchg);
    struct order *best_sell = find_best_sell(xchg);
    
    infop->bid = best_buy != NULL ? htonl(best_buy->price) : 0;
    infop->ask = best_sell != NULL ? htonl(best_sell->price) : 0;
    infop->last = htonl(xchg->last_trade_price);
    
    pthread_mutex_unlock(&xchg->mutex);
}

/*
 * Post a buy order on the exchange.
 */
orderid_t exchange_post_buy(EXCHANGE *xchg, TRADER *trader, quantity_t quantity, funds_t price) {
    if (xchg == NULL || trader == NULL || quantity == 0 || price == 0) {
        return 0;
    }
    
    ACCOUNT *account = trader_get_account(trader);
    if (account == NULL) {
        return 0;
    }
    
    // Check if trader has enough funds
    funds_t max_cost = quantity * price;
    if (account_decrease_balance(account, max_cost) != 0) {
        return 0; // Insufficient funds
    }
    
    pthread_mutex_lock(&xchg->mutex);
    
    // Create order
    struct order *order = malloc(sizeof(struct order));
    if (order == NULL) {
        // Refund the funds
        account_increase_balance(account, max_cost);
        pthread_mutex_unlock(&xchg->mutex);
        return 0;
    }
    
    order->id = xchg->next_order_id++;
    order->trader = trader_ref(trader, "buy order");
    order->type = ORDER_BUY;
    order->quantity = quantity;
    order->price = price;
    order->next = xchg->buy_orders;
    xchg->buy_orders = order;
    
    orderid_t order_id = order->id;
    
    pthread_mutex_unlock(&xchg->mutex);
    
    // Wake matchmaker
    sem_post(&xchg->matchmaker_sem);
    
    return order_id;
}

/*
 * Post a sell order on the exchange.
 */
orderid_t exchange_post_sell(EXCHANGE *xchg, TRADER *trader, quantity_t quantity, funds_t price) {
    if (xchg == NULL || trader == NULL || quantity == 0 || price == 0) {
        return 0;
    }
    
    ACCOUNT *account = trader_get_account(trader);
    if (account == NULL) {
        return 0;
    }
    
    // Check if trader has enough inventory
    if (account_decrease_inventory(account, quantity) != 0) {
        return 0; // Insufficient inventory
    }
    
    pthread_mutex_lock(&xchg->mutex);
    
    // Create order
    struct order *order = malloc(sizeof(struct order));
    if (order == NULL) {
        // Refund the inventory
        account_increase_inventory(account, quantity);
        pthread_mutex_unlock(&xchg->mutex);
        return 0;
    }
    
    order->id = xchg->next_order_id++;
    order->trader = trader_ref(trader, "sell order");
    order->type = ORDER_SELL;
    order->quantity = quantity;
    order->price = price;
    order->next = xchg->sell_orders;
    xchg->sell_orders = order;
    
    orderid_t order_id = order->id;
    
    pthread_mutex_unlock(&xchg->mutex);
    
    // Wake matchmaker
    sem_post(&xchg->matchmaker_sem);
    
    return order_id;
}

/*
 * Attempt to cancel a pending order.
 */
int exchange_cancel(EXCHANGE *xchg, TRADER *trader, orderid_t order, quantity_t *quantity) {
    if (xchg == NULL || trader == NULL || quantity == NULL) {
        return -1;
    }
    
    pthread_mutex_lock(&xchg->mutex);
    
    // Search for order in buy orders
    struct order **list = &xchg->buy_orders;
    struct order *found = NULL;
    
    for (struct order **p = list; *p != NULL; p = &(*p)->next) {
        if ((*p)->id == order) {
            // Verify trader matches
            if ((*p)->trader != trader) {
                pthread_mutex_unlock(&xchg->mutex);
                return -1;
            }
            
            found = *p;
            *quantity = found->quantity;
            
            // Remove from list
            *p = (*p)->next;
            
            // Refund encumbered funds
            ACCOUNT *account = trader_get_account(trader);
            account_increase_balance(account, found->quantity * found->price);
            
            trader_unref(found->trader, "cancel");
            free(found);
            
            pthread_mutex_unlock(&xchg->mutex);
            
            // Broadcast CANCELED notification
            BRS_PACKET_HEADER hdr;
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            hdr.timestamp_sec = htonl(ts.tv_sec);
            hdr.timestamp_nsec = htonl(ts.tv_nsec);
            hdr.type = BRS_CANCELED_PKT;
            hdr.size = htons(sizeof(BRS_NOTIFY_INFO));
            BRS_NOTIFY_INFO notify;
            notify.buyer = htonl(order);
            notify.seller = 0;
            notify.quantity = htonl(*quantity);
            notify.price = 0;
            trader_broadcast_packet(&hdr, &notify);
            
            return 0;
        }
    }
    
    // Search in sell orders
    list = &xchg->sell_orders;
    for (struct order **p = list; *p != NULL; p = &(*p)->next) {
        if ((*p)->id == order) {
            // Verify trader matches
            if ((*p)->trader != trader) {
                pthread_mutex_unlock(&xchg->mutex);
                return -1;
            }
            
            found = *p;
            *quantity = found->quantity;
            
            // Remove from list
            *p = (*p)->next;
            
            // Refund encumbered inventory
            ACCOUNT *account = trader_get_account(trader);
            account_increase_inventory(account, found->quantity);
            
            trader_unref(found->trader, "cancel");
            free(found);
            
            pthread_mutex_unlock(&xchg->mutex);
            
            // Broadcast CANCELED notification
            BRS_PACKET_HEADER hdr;
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            hdr.timestamp_sec = htonl(ts.tv_sec);
            hdr.timestamp_nsec = htonl(ts.tv_nsec);
            hdr.type = BRS_CANCELED_PKT;
            hdr.size = htons(sizeof(BRS_NOTIFY_INFO));
            BRS_NOTIFY_INFO notify;
            notify.buyer = 0;
            notify.seller = htonl(order);
            notify.quantity = htonl(*quantity);
            notify.price = 0;
            trader_broadcast_packet(&hdr, &notify);
            
            return 0;
        }
    }
    
    pthread_mutex_unlock(&xchg->mutex);
    return -1; // Order not found
}

