#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <arpa/inet.h>

#include "account.h"
#include "debug.h"

struct account {
    funds_t balance;
    quantity_t inventory;
    pthread_mutex_t mutex;
};

// Global account map
static struct account_map_entry {
    char *name;
    ACCOUNT *account;
} account_map[MAX_ACCOUNTS];

static int account_count = 0;
static pthread_mutex_t account_map_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Initialize the accounts module.
 */
int accounts_init(void) {
    // Map mutex is already initialized statically
    account_count = 0;
    memset(account_map, 0, sizeof(account_map));
    return 0;
}

/*
 * Finalize the accounts module, freeing all associated resources.
 */
void accounts_fini(void) {
    pthread_mutex_lock(&account_map_mutex);
    
    for (int i = 0; i < account_count; i++) {
        if (account_map[i].account != NULL) {
            pthread_mutex_destroy(&account_map[i].account->mutex);
            free(account_map[i].account);
            free(account_map[i].name);
        }
    }
    
    account_count = 0;
    pthread_mutex_unlock(&account_map_mutex);
}

/*
 * Look up an account for a specified user name.
 */
ACCOUNT *account_lookup(char *name) {
    if (name == NULL) {
        return NULL;
    }
    
    pthread_mutex_lock(&account_map_mutex);
    
    // Search for existing account
    for (int i = 0; i < account_count; i++) {
        if (account_map[i].name != NULL && strcmp(account_map[i].name, name) == 0) {
            ACCOUNT *account = account_map[i].account;
            pthread_mutex_unlock(&account_map_mutex);
            return account;
        }
    }
    
    // Account doesn't exist - check if we can create one
    if (account_count >= MAX_ACCOUNTS) {
        pthread_mutex_unlock(&account_map_mutex);
        return NULL;
    }
    
    // Create new account
    ACCOUNT *account = malloc(sizeof(ACCOUNT));
    if (account == NULL) {
        pthread_mutex_unlock(&account_map_mutex);
        return NULL;
    }
    
    account->balance = 0;
    account->inventory = 0;
    if (pthread_mutex_init(&account->mutex, NULL) != 0) {
        free(account);
        pthread_mutex_unlock(&account_map_mutex);
        return NULL;
    }
    
    // Copy name
    char *name_copy = malloc(strlen(name) + 1);
    if (name_copy == NULL) {
        pthread_mutex_destroy(&account->mutex);
        free(account);
        pthread_mutex_unlock(&account_map_mutex);
        return NULL;
    }
    strcpy(name_copy, name);
    
    // Add to map
    account_map[account_count].name = name_copy;
    account_map[account_count].account = account;
    account_count++;
    
    debug("Create new account %p [%s]", account, name);
    
    pthread_mutex_unlock(&account_map_mutex);
    return account;
}

/*
 * Increase the balance for an account.
 */
void account_increase_balance(ACCOUNT *account, funds_t amount) {
    if (account == NULL) {
        return;
    }
    
    pthread_mutex_lock(&account->mutex);
    funds_t old_balance = account->balance;
    account->balance += amount;
    pthread_mutex_unlock(&account->mutex);
    
    // Find account name for debug
    pthread_mutex_lock(&account_map_mutex);
    const char *name = "unknown";
    for (int i = 0; i < account_count; i++) {
        if (account_map[i].account == account) {
            name = account_map[i].name;
            break;
        }
    }
    debug("Increase balance of account '%s' (%u -> %u)", name, old_balance, account->balance);
    pthread_mutex_unlock(&account_map_mutex);
}

/*
 * Attempt to decrease the balance for an account.
 */
int account_decrease_balance(ACCOUNT *account, funds_t amount) {
    if (account == NULL) {
        return -1;
    }
    
    pthread_mutex_lock(&account->mutex);
    
    if (account->balance >= amount) {
        account->balance -= amount;
        pthread_mutex_unlock(&account->mutex);
        return 0;
    }
    
    pthread_mutex_unlock(&account->mutex);
    return -1;
}

/*
 * Increase the inventory of an account by a specified quantity.
 */
void account_increase_inventory(ACCOUNT *account, quantity_t quantity) {
    if (account == NULL) {
        return;
    }
    
    pthread_mutex_lock(&account->mutex);
    quantity_t old_inventory = account->inventory;
    account->inventory += quantity;
    pthread_mutex_unlock(&account->mutex);
    
    // Find account name for debug
    pthread_mutex_lock(&account_map_mutex);
    const char *name = "unknown";
    for (int i = 0; i < account_count; i++) {
        if (account_map[i].account == account) {
            name = account_map[i].name;
            break;
        }
    }
    debug("Increase inventory of account '%s' (%u -> %u)", name, old_inventory, account->inventory);
    pthread_mutex_unlock(&account_map_mutex);
}

/*
 * Attempt to decrease the inventory for an account by a specified quantity.
 */
int account_decrease_inventory(ACCOUNT *account, quantity_t quantity) {
    if (account == NULL) {
        return -1;
    }
    
    pthread_mutex_lock(&account->mutex);
    
    if (account->inventory >= quantity) {
        account->inventory -= quantity;
        pthread_mutex_unlock(&account->mutex);
        return 0;
    }
    
    pthread_mutex_unlock(&account->mutex);
    return -1;
}

/*
 * Get the current balance and inventory of a specified account.
 */
void account_get_status(ACCOUNT *account, BRS_STATUS_INFO *infop) {
    if (account == NULL || infop == NULL) {
        return;
    }
    
    pthread_mutex_lock(&account->mutex);
    
    // Copy values and convert to network byte order
    infop->balance = htonl(account->balance);
    infop->inventory = htonl(account->inventory);
    
    // Other fields are set by exchange_get_status
    infop->bid = 0;
    infop->ask = 0;
    infop->last = 0;
    infop->orderid = 0;
    infop->quantity = 0;
    
    pthread_mutex_unlock(&account->mutex);
}