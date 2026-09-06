#include "t_test.h"
#include "t_wal.h"
#include "t_queue.h"
#include "t_broker.h"
#include "t_domain.h"
#include "t_file.h"
#include "t_compiler.h"
#include <string.h>
#include <stdio.h>
#if T_PLATFORM_WINDOWS
#include <direct.h>
static void rm_dir(const char *p) { (void)_rmdir(p); }
#else
#include <unistd.h>
static void rm_dir(const char *p) { (void)rmdir(p); }
#endif

static int g_n;
static int g_puts;
static int g_dels;
static uint64_t g_id;
static uint8_t g_pri;
static char g_buf[16];

static void replay_one(const t_wal_rec *r, void *ud) {
    (void)ud;
    g_n++;
    g_id = r->msg_id;
    g_pri = r->priority;
    if (r->data && r->data_len > 0) {
        size_t n = r->data_len < sizeof(g_buf) - 1 ? r->data_len : sizeof(g_buf) - 1;
        memcpy(g_buf, r->data, n);
        g_buf[n] = 0;
    }
}

static void replay_count(const t_wal_rec *r, void *ud) {
    (void)ud;
    if (r->op == T_WAL_PUT) g_puts++;
    if (r->op == T_WAL_DEL) g_dels++;
}

static void replay_n(const t_wal_rec *r, void *ud) {
    (void)r;
    (void)ud;
    g_n++;
}

static void exclusive_nop(const t_msg *m, void *ud) {
    (void)m;
    (void)ud;
}

static void rm(const char *p) { t_file_unlink(p); }

T_TEST(wal_rejects_bad_path) {
    T_ASSERT_NULL(t_wal_open(NULL, 1));
    T_ASSERT_NULL(t_wal_open("", 1));
}

T_TEST(wal_put_replay) {
    const char *path = "test_transit_wal_put.bin";
    rm(path);
    t_wal *w = t_wal_open(path, 1);
    T_ASSERT_NOT_NULL(w);
    T_ASSERT_EQ(t_wal_append(w, T_WAL_PUT, 3, 1, (const uint8_t *)"ab", 2), 0);
    t_wal_close(w);

    w = t_wal_open(path, 1);
    T_ASSERT_NOT_NULL(w);
    g_n = 0;
    g_buf[0] = 0;
    T_ASSERT_EQ(t_wal_replay(w, replay_one, NULL), 0);
    T_ASSERT_EQ(g_n, 1);
    T_ASSERT_EQ((long long)g_id, 1);
    T_ASSERT_EQ((int)g_pri, 3);
    T_ASSERT_STR_EQ(g_buf, "ab");
    t_wal_close(w);
    rm(path);
}

T_TEST(wal_put_then_del) {
    const char *path = "test_transit_wal_del.bin";
    rm(path);
    t_wal *w = t_wal_open(path, 1);
    T_ASSERT_EQ(t_wal_append(w, T_WAL_PUT, 0, 9, (const uint8_t *)"x", 1), 0);
    T_ASSERT_EQ(t_wal_append(w, T_WAL_DEL, 0, 9, NULL, 0), 0);
    t_wal_close(w);
    w = t_wal_open(path, 1);
    g_puts = 0;
    g_dels = 0;
    T_ASSERT_EQ(t_wal_replay(w, replay_count, NULL), 0);
    T_ASSERT_EQ(g_puts, 1);
    T_ASSERT_EQ(g_dels, 1);
    t_wal_close(w);
    rm(path);
}

T_TEST(wal_torn_tail_ok) {
    const char *path = "test_transit_wal_torn.bin";
    rm(path);
    t_wal *w = t_wal_open(path, 1);
    T_ASSERT_EQ(t_wal_append(w, T_WAL_PUT, 0, 1, (const uint8_t *)"ok", 2), 0);
    t_wal_close(w);
    FILE *f = fopen(path, "ab");
    T_ASSERT_NOT_NULL(f);
    fputc(0xAA, f);
    fclose(f);
    w = t_wal_open(path, 1);
    g_n = 0;
    T_ASSERT_EQ(t_wal_replay(w, replay_n, NULL), 0);
    T_ASSERT_EQ(g_n, 1);
    t_wal_close(w);
    rm(path);
}

T_TEST(wal_rejects_oversize_record) {
    const char *path = "test_transit_wal_big.bin";
    rm(path);
    t_wal *w = t_wal_open(path, 0);
    T_ASSERT_NOT_NULL(w);
    T_ASSERT(t_wal_append(w, T_WAL_PUT, 0, 1, (const uint8_t *)"x",
                          (16u * 1024u * 1024u) + 1u) != 0);
    t_wal_close(w);
    rm(path);
}

T_TEST(queue_durable_survives_reopen) {
    const char *path = "test_transit_q_durable.wal";
    rm(path);
    t_queue *q = t_queue_create("orders", T_QUEUE_FIFO, T_QUEUE_FLAG_DURABLE);
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ(t_queue_open_wal(q, path, 1), 0);
    T_ASSERT_EQ(t_queue_post(q, (const uint8_t *)"one", 3, 0), 0);
    T_ASSERT_EQ(t_queue_post(q, (const uint8_t *)"two", 3, 1), 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 2);
    t_queue_destroy(q);

    q = t_queue_create("orders", T_QUEUE_FIFO, T_QUEUE_FLAG_DURABLE);
    T_ASSERT_EQ(t_queue_open_wal(q, path, 1), 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 2);
    t_msg m;
    T_ASSERT_EQ(t_queue_consume(q, &m), 0);
    T_ASSERT_MEM_EQ(m.data, "one", 3);
    T_ASSERT_EQ(t_queue_ack(q, m.msg_id), 0);
    t_queue_destroy(q);

    q = t_queue_create("orders", T_QUEUE_FIFO, T_QUEUE_FLAG_DURABLE);
    T_ASSERT_EQ(t_queue_open_wal(q, path, 1), 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    T_ASSERT_EQ(t_queue_consume(q, &m), 0);
    T_ASSERT_MEM_EQ(m.data, "two", 3);
    t_queue_destroy(q);
    rm(path);
}

T_TEST(wal_join_roundtrip) {
    const char *path = "test_transit_wal_join.bin";
    rm(path);
    t_wal *w = t_wal_open(path, 1);
    T_ASSERT_NOT_NULL(w);
    T_ASSERT_EQ(t_wal_append(w, T_WAL_JOIN, 0, 0, (const uint8_t *)"workers", 7), 0);
    T_ASSERT_EQ(t_wal_append(w, T_WAL_JOIN, 0, 0, NULL, 0), -1);
    t_wal_close(w);

    w = t_wal_open(path, 1);
    T_ASSERT_NOT_NULL(w);
    g_n = 0;
    g_buf[0] = 0;
    T_ASSERT_EQ(t_wal_replay(w, replay_one, NULL), 0);
    T_ASSERT_EQ(g_n, 1);
    T_ASSERT_STR_EQ(g_buf, "workers");
    t_wal_close(w);
    rm(path);
}

T_TEST(queue_durable_group_survives_reopen) {
    const char *path = "test_transit_q_group.wal";
    rm(path);
    t_queue *q = t_queue_create("orders", T_QUEUE_FIFO, T_QUEUE_FLAG_DURABLE);
    T_ASSERT_EQ(t_queue_open_wal(q, path, 1), 0);
    T_ASSERT_EQ(t_queue_set_group(q, "workers"), 0);
    T_ASSERT_EQ(t_queue_post(q, (const uint8_t *)"one", 3, 0), 0);
    t_queue_destroy(q);

    q = t_queue_create("orders", T_QUEUE_FIFO, T_QUEUE_FLAG_DURABLE);
    T_ASSERT_EQ(t_queue_open_wal(q, path, 1), 0);
    T_ASSERT(strcmp(t_queue_group(q), "workers") == 0);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    T_ASSERT_EQ(t_queue_set_group(q, "other"), -1);
    t_queue_destroy(q);
    rm(path);
}

T_TEST(queue_durable_rejected_for_broadcast) {
    t_queue *q = t_queue_create("bc", T_QUEUE_BROADCAST, T_QUEUE_FLAG_DURABLE);
    T_ASSERT_NULL(q);
}

T_TEST(queue_exclusive_second_consumer) {
    t_queue *q = t_queue_create("ex", T_QUEUE_FIFO, T_QUEUE_FLAG_EXCLUSIVE);
    T_ASSERT_NOT_NULL(q);
    T_ASSERT(t_queue_add_consumer(q, exclusive_nop, NULL) != 0);
    T_ASSERT_EQ((unsigned long long)t_queue_add_consumer(q, exclusive_nop, q), 0ULL);
    T_ASSERT_EQ((int)t_queue_consumer_count(q), 1);
    t_queue_destroy(q);
}

T_TEST(broker_durable_roundtrip) {
    const char *dir = "test_transit_broker_wal";
    const char *wal = "test_transit_broker_wal/default.jobs.wal";
    rm(wal);
    rm_dir(dir);
    t_broker *b = t_broker_create("n0");
    T_ASSERT_EQ(t_broker_set_datadir(b, dir), 0);
    T_ASSERT_EQ(t_broker_set_wal_sync_every(b, 1), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", T_QUEUE_FIFO,
                                      T_QUEUE_FLAG_DURABLE), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"hi", 2, 0), 0);
    t_broker_destroy(b);

    b = t_broker_create("n0");
    T_ASSERT_EQ(t_broker_set_datadir(b, dir), 0);
    T_ASSERT_EQ(t_broker_set_wal_sync_every(b, 1), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", T_QUEUE_FIFO,
                                      T_QUEUE_FLAG_DURABLE), 0);
    t_domain *d = t_broker_get_domain(b, "default");
    t_queue *q = (t_queue *)t_domain_get_queue(d, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT_EQ((int)t_queue_pending_count(q), 1);
    t_broker_destroy(b);
    rm(wal);
    rm_dir(dir);
}

T_TEST(broker_durable_group_roundtrip) {
    const char *dir = "test_transit_broker_wal_g";
    const char *wal = "test_transit_broker_wal_g/default.jobs.wal";
    rm(wal);
    rm_dir(dir);
    t_broker *b = t_broker_create("n0");
    T_ASSERT_EQ(t_broker_set_datadir(b, dir), 0);
    T_ASSERT_EQ(t_broker_set_wal_sync_every(b, 1), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", T_QUEUE_FIFO,
                                      T_QUEUE_FLAG_DURABLE), 0);
    T_ASSERT_EQ(t_broker_join_group(b, "jobs", "workers"), 0);
    T_ASSERT_EQ(t_broker_publish(b, "jobs", (const uint8_t *)"hi", 2, 0), 0);
    t_broker_destroy(b);

    b = t_broker_create("n0");
    T_ASSERT_EQ(t_broker_set_datadir(b, dir), 0);
    T_ASSERT_EQ(t_broker_set_wal_sync_every(b, 1), 0);
    T_ASSERT_EQ(t_broker_start(b), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", T_QUEUE_FIFO,
                                      T_QUEUE_FLAG_DURABLE), 0);
    t_domain *d = t_broker_get_domain(b, "default");
    t_queue *q = (t_queue *)t_domain_get_queue(d, "jobs");
    T_ASSERT_NOT_NULL(q);
    T_ASSERT(strcmp(t_queue_group(q), "workers") == 0);
    T_ASSERT_EQ(t_broker_join_group(b, "jobs", "other"), -1);
    t_broker_destroy(b);
    rm(wal);
    rm_dir(dir);
}

T_TEST(broker_durable_requires_datadir) {
    t_broker *b = t_broker_create("n0");
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", T_QUEUE_FIFO,
                                      T_QUEUE_FLAG_DURABLE), -1);
    t_broker_destroy(b);
}

T_TEST(broker_delete_unlinks_wal) {
    const char *dir = "test_transit_broker_wal_rm";
    const char *wal = "test_transit_broker_wal_rm/default.jobs.wal";
    rm(wal);
    rm_dir(dir);
    t_broker *b = t_broker_create("n0");
    T_ASSERT_EQ(t_broker_set_datadir(b, dir), 0);
    T_ASSERT_EQ(t_broker_create_queue(b, "default", "jobs", T_QUEUE_FIFO,
                                      T_QUEUE_FLAG_DURABLE), 0);
    T_ASSERT_EQ(t_broker_delete_queue(b, "default", "jobs"), 0);
    {
        t_file f;
        t_file_init(&f);
        T_ASSERT(t_file_open(&f, wal, T_FILE_READ) != 0);
    }
    t_broker_destroy(b);
    rm_dir(dir);
}

int main(void) {
    return t_run_all_tests();
}
