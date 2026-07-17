#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "t_session.h"
#include "t_time.h"

/* Internal session implementation */
struct t_session {
    uint64_t id;
    t_sstate state;
    uint64_t last_activity_ns;
    size_t msgs_sent;
    size_t msgs_received;
    size_t refs; /* optional retain/release */
    size_t pins; /* destroy-guard: dispatch register holds a pin */
    void *user_data;
    t_session_event_cb event_cb;
    void *event_ud;
};

static uint64_t _now_ns(void) {
    return t_time_now_ns();
}

/* Public API */
t_session *t_session_create(uint64_t session_id) {
    t_session *sess = (t_session *)calloc(1, sizeof(t_session));
    if (!sess) {
        return NULL;
    }
    sess->id = session_id;
    sess->state = T_SESSION_DISCONNECTED;
    sess->last_activity_ns = 0;
    sess->msgs_sent = 0;
    sess->msgs_received = 0;
    sess->refs = 0;
    sess->pins = 0;
    sess->user_data = NULL;
    sess->event_cb = NULL;
    sess->event_ud = NULL;
    return sess;
}

int t_session_destroy(t_session *sess) {
    if (!sess) return 0;
    /* Refuse free while retained or still pinned by dispatch. */
    if (sess->refs > 0 || sess->pins > 0) return -1;
    free(sess);
    return 0;
}

void t_session_retain(t_session *sess) {
    if (sess) sess->refs++;
}

void t_session_release(t_session *sess) {
    if (sess && sess->refs > 0) sess->refs--;
}

void t_session_pin(t_session *sess) {
    if (sess) sess->pins++;
}

void t_session_unpin(t_session *sess) {
    if (sess && sess->pins > 0) sess->pins--;
}

uint64_t t_session_id(const t_session *sess) {
    return sess ? sess->id : 0;
}

t_sstate t_session_get_state(const t_session *sess) {
    return sess ? sess->state : T_SESSION_DISCONNECTED;
}

int t_session_is_active(const t_session *sess) {
    return sess && sess->state == T_SESSION_CONNECTED;
}

void t_session_set_user_data(t_session *sess, void *ud) {
    if (sess) sess->user_data = ud;
}

void *t_session_get_user_data(const t_session *sess) {
    return sess ? sess->user_data : NULL;
}

int t_session_connect(t_session *sess) {
    if (!sess) return -1;
    sess->state = T_SESSION_CONNECTED;
    sess->last_activity_ns = _now_ns();
    return 0;
}

int t_session_disconnect(t_session *sess) {
    if (!sess) return -1;
    sess->state = T_SESSION_DISCONNECTED;
    sess->last_activity_ns = _now_ns();
    return 0;
}

uint64_t t_session_last_activity_ns(const t_session *sess) {
    return sess ? sess->last_activity_ns : 0;
}

void t_session_update_activity(t_session *sess) {
    if (sess) sess->last_activity_ns = _now_ns();
}

int t_session_check_timeout(t_session *sess, int64_t timeout_ns) {
    if (!sess) return -1;
    if (timeout_ns <= 0) return 0;
    uint64_t now = _now_ns();
    uint64_t last = sess->last_activity_ns;
    if (last == 0) return 0;
    if (now < last) return 0; /* clock went backwards */
    return (now - last >= (uint64_t)timeout_ns) ? 1 : 0;
}

size_t t_session_msgs_sent(const t_session *sess) {
    return sess ? sess->msgs_sent : 0;
}

size_t t_session_msgs_received(const t_session *sess) {
    return sess ? sess->msgs_received : 0;
}

void t_session_record_send(t_session *sess) {
    if (sess) sess->msgs_sent++;
}

void t_session_record_recv(t_session *sess) {
    if (sess) sess->msgs_received++;
}
