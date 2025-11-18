/**
 * === DO NOT MODIFY THIS FILE ===
 * If you need some other prototypes or constants in a header, please put them
 * in another header file.
 *
 * When we grade, we will be replacing this file with our own copy.
 * You have been warned.
 * === DO NOT MODIFY THIS FILE ===
 */
#ifndef TRADER_H
#define TRADER_H

#include "protocol.h"
#include "account.h"

/*
 * A trader object maintains state information about a logged-in trader,
 * including a reference to the trader's account and the means of sending
 * a message to the associated client.
 */

/*
 * The maximum number of traders supported by the exchange.
 */
#define MAX_TRADERS 64

/*
 * Structure that defines the state of a trader in the Bourse server.
 *
 * You will have to give a complete structure definition in trader.c.
 * The precise contents are up to you.  Be sure that all the operations
 * that might be called concurrently are thread-safe.
 */
typedef struct trader TRADER;

/*
 * Initialize the traders module.
 * This must be called before doing calling any other functions in this
 * module.
 *
 * @return 0 if initialization succeeds, -1 otherwise.
 */
int traders_init(void);

/*
 * Finalize the traders module, freeing all associated resources.
 * This should be called when the trader module is no longer required.
 */
void traders_fini(void);

/*
 * Attempt to log in a trader with a specified user name.
 *
 * @param clientfd  The file descriptor of the connection to the client.
 * @param name  The trader's user name, which is copied by this function.
 * @return A pointer to a TRADER object, in case of success, otherwise NULL.
 *
 * The login can fail if no account for that user name already exists and it
 * was not possible to create a new account.  If the login succeeds, then a
 * pointer to a TRADER object is returned.  The returned TRADER object has a
 * reference count of one.  This reference should be "owned" by the thread that
 * is servicing this client.  It will ultimately be released upon disconnection
 * of the client, by a call to trader_logout.
 *
 * It is possible for multiple traders to be logged in at once with the same
 * user name (and therefore using the same account).
 */
TRADER *trader_login(int fd, char *name);

/*
 * Log out a trader.
 *
 * @param trader  The trader to be logged out.
 *
 * This function "consumes" one reference to the TRADER object by calling
 * trader_unref().
 */
void trader_logout(TRADER *trader);

/*
 * Increase the reference count on a trader by one.
 *
 * @param trader  The TRADER whose reference count is to be increased.
 * @param why  A string describing the reason why the reference count is
 * being increased.  This is used for debugging printout, to help trace
 * the reference counting.
 * @return  The same TRADER object that was passed as a parameter.
 */
TRADER *trader_ref(TRADER *trader, char *why);

/*
 * Decrease the reference count on a trader by one.
 *
 * @param trader  The TRADER whose reference count is to be decreased.
 * @param why  A string describing the reason why the reference count is
 * being increased.  This is used for debugging printout, to help trace
 * the reference counting.
 *
 * If after decrementing, the reference count has reached zero, then the
 * trader and its contents are freed.
 */
void trader_unref(TRADER *trader, char *why);

/*
 * Get the account associated with a trader.
 *
 * @param trader  The TRADER object from which to get the account.
 * @return  The account associated with the trader.
 */
ACCOUNT *trader_get_account(TRADER *trader);

/*
 * Send a packet to the client for a trader.
 *
 * @param trader  The TRADER object for the client who should receive
 * the packet.
 * @param pkt  The packet to be sent.
 * @param data  Data payload to be sent, or NULL if none.
 * @return 0 if transmission succeeds, -1 otherwise.
 *
 * Once a client has connected and successfully logged in, this function
 * should be used to send packets to the client, as opposed to the lower-level
 * proto_send_packet() function.  The reason for this is that the present
 * function will obtain exclusive access to the trader before calling
 * proto_send_packet().  The fact that exclusive access is obtained before
 * sending means that multiple threads can safely call this function to send
 * to the client, and these calls will be properly serialized.
 */
int trader_send_packet(TRADER *trader, BRS_PACKET_HEADER *pkt, void *data);

/*
 * Broadcast a packet to all currently logged-in traders.
 *
 * @param pkt  The packet to be sent.
 * @param data  Data payload to be sent, or NULL if none.
 * @return 0 if broadcast succeeds, -1 otherwise.
 */
int trader_broadcast_packet(BRS_PACKET_HEADER *pkt, void *data);

/*
 * Send an ACK packet to the client for a trader.
 *
 * @param trader  The TRADER object for the client who should receive
 * the packet.
 * @param infop  Pointer to the optional data payload for this packet,
 * or NULL if there is to be no payload.
 * @return 0 if transmission succeeds, -1 otherwise.
 */
int trader_send_ack(TRADER *trader, BRS_STATUS_INFO *info);

/*
 * Send an NACK packet to the client for a trader.
 *
 * @param trader  The TRADER object for the client who should receive
 * the packet.
 * @return 0 if transmission succeeds, -1 otherwise.
 */
int trader_send_nack(TRADER *trader);

#endif
