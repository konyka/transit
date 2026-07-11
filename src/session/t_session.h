#ifndef T_SESSION_H
#define T_SESSION_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef enum t_sstate {
    T_SESSION_DISCONNECTED = 0,
    T_SESSION_CONNECTING,
    T_SESSION_CONNECTED,
    T_SESSION_CLOSING
} t_sstate;

typedef struct t_session t_session;

typedef void (*t_session_event_cb)(t_session *sess, int event, void *ud);

t_session *t_session_create(uint64_t session_id);
int        t_session_destroy(t_session *sess);
void       t_session_retain(t_session *sess);
void       t_session_release(t_session *sess);
uint64_t   t_session_id(const t_session *sess);
t_sstate   t_session_get_state(const t_session *sess);
int        t_session_is_active(const t_session *sess);
void       t_session_set_user_data(t_session *sess, void *ud);
void      *t_session_get_user_data(const t_session *sess);
int        t_session_connect(t_session *sess);
int        t_session_disconnect(t_session *sess);
uint64_t   t_session_last_activity_ns(const t_session *sess);
void       t_session_update_activity(t_session *sess);
int        t_session_check_timeout(t_session *sess, int64_t timeout_ns);
size_t     t_session_msgs_sent(const t_session *sess);
size_t     t_session_msgs_received(const t_session *sess);
void       t_session_record_send(t_session *sess);
void       t_session_record_recv(t_session *sess);

#endif /* T_SESSION_H */
