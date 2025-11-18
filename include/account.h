/**
 * === DO NOT MODIFY THIS FILE ===
 * If you need some other prototypes or constants in a header, please put them
 * in another header file.
 *
 * When we grade, we will be replacing this file with our own copy.
 * You have been warned.
 * === DO NOT MODIFY THIS FILE ===
 */
#ifndef ACCOUNT_H
#define ACCOUNT_H

#include "protocol.h"

/*
 * The maximum number of accounts supported by the exchange.
 */
#define MAX_ACCOUNTS 64

/*
 * Structure that defines the state of an account in the Bourse server.
 *
 * You will have to give a complete structure definition in accounts.c.
 * The precise contents are up to you.  Be sure that all the operations
 * that might be called concurrently are thread-safe.
 */
typedef struct account ACCOUNT;

/*
 * Initialize the accounts module.
 * This must be called before doing calling any other functions in this
 * module.
 *
 * @return 0 if initialization succeeds, -1 otherwise.
 */
int accounts_init(void);

/*
 * Finalize the accounts module, freeing all associated resources.
 * This should be called when the accounts module is no longer required.
 */
void accounts_fini(void);

/*
 * Look up an account for a specified user name.  If an account with
 * the specified name already exists, that account is returned, otherwise
 * a new account with zero balance and inventory is created.
 * An account, once created for a particular user name, persists until
 * the server is shut down.
 *
 * @param name  The user name, which is copied by this function.
 * @return A pointer to an ACCOUNT object, in case of success, otherwise NULL.
 */
ACCOUNT *account_lookup(char *name);

/*
 * Increase the balance for an account.
 *
 * @param account  The account whose balance is to be increased.
 * @param amount  The amount by which the balance is to be increased.
 */
void account_increase_balance(ACCOUNT *account, funds_t amount);

/*
 * Attempt to decrease the balance for an account.
 *
 * @param account  The account whose balance is to be decreased.
 * @param amount  The amount by which the balance is to be decreased.
 * @return 0 if the original balance is at least as great as the
 * amount of decrease, -1 otherwise.  In case -1 is returned, there
 * is no change to the account balance.
 */
int account_decrease_balance(ACCOUNT *account, funds_t amount);

/*
 * Increase the inventory of an account by a specified quantity.
 *
 * @param account  The account whose inventory is to be increased.
 * @param amount  The amount by which the inventory is to be increased.
 */
void account_increase_inventory(ACCOUNT *account, quantity_t quantity);

/*
 * Attempt to decrease the inventory for an account by a specified quantity.
 *
 * @param account  The account whose inventory is to be decreased.
 * @param amount  The amount by which the inventory is to be decreased.
 * @return 0 if the original inventory is at least as great as the
 * amount of decrease, -1 otherwise.  In case -1 is returned, there
 * is no change to the account balance.
 */
int account_decrease_inventory(ACCOUNT *account, quantity_t quantity);

/*
 * Get the current balance and inventory of a specified account.  The values
 * returned are guaranteed to be a consistent snapshot of the state of the
 * account at a single point in time, although the state may change as soon
 * as this function has returned.
 *
 * @param account  the account whose balance and inventory is to be queried.
 * @param infop  pointer to structure to receive the status information.
 * As this structure is designed to be sent in a packet, multibyte fields will be
 * stored in network byte order.
 */
void account_get_status(ACCOUNT *account, BRS_STATUS_INFO *infop);

#endif
