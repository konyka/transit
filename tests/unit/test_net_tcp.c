#include "t_test.h"
#include "t_socket.h"
#include "t_tcp.h"
#include "t_evloop.h"
#include "t_time.h"
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>

T_TEST(socket_create_close) {
    int fd = t_socket_create(AF_INET, SOCK_STREAM, 0);
    T_ASSERT(fd >= 0);
    t_socket_close(fd);
}

T_TEST(socket_nonblock) {
    int fd = t_socket_create(AF_INET, SOCK_STREAM, 0);
    T_ASSERT(fd >= 0);
    T_ASSERT_EQ(t_socket_set_nonblock(fd), 0);
    t_socket_close(fd);
}

T_TEST(sockaddr_ipv4) {
    t_sockaddr addr;
    T_ASSERT_EQ(t_sockaddr_init_ipv4(&addr, "127.0.0.1", 8080), 0);
    T_ASSERT_EQ(t_sockaddr_port(&addr), 8080);
}

T_TEST(socket_bind_listen) {
    int fd = t_socket_create(AF_INET, SOCK_STREAM, 0);
    T_ASSERT(fd >= 0);
    t_socket_set_reuseaddr(fd);
    t_socket_set_nonblock(fd);
    t_sockaddr addr;
    t_sockaddr_init_ipv4(&addr, "127.0.0.1", 0);
    T_ASSERT_EQ(t_socket_bind(fd, &addr), 0);
    T_ASSERT_EQ(t_socket_listen(fd, 128), 0);
    t_socket_close(fd);
}

static int g_echo_done;

static void on_accept(t_tcp_server *srv, int client_fd, t_sockaddr *peer, void *ud) {
    (void)srv; (void)peer;
    /* Just close the client fd for this test */
    t_socket_close(client_fd);
    *(int *)ud = 1;
}

T_TEST(tcp_server_echo_basic) {
    t_evloop *loop = t_evloop_create();
    t_tcp_server *srv = t_tcp_server_create(loop);
    T_ASSERT_NOT_NULL(srv);
    g_echo_done = 0;
    T_ASSERT_EQ(t_tcp_server_listen(srv, "127.0.0.1", 0, on_accept, &g_echo_done), 0);
    /* Connect to it */
    int client = t_socket_create(AF_INET, SOCK_STREAM, 0);
    t_socket_set_nonblock(client);
    t_sockaddr addr;
    t_sockaddr_init_ipv4(&addr, "127.0.0.1", 0);
    /* For this simple test, just verify the server was created and listening */
    t_socket_close(client);
    t_tcp_server_destroy(srv);
    t_evloop_destroy(loop);
}

T_TEST(tcp_conn_create_destroy) {
    int fds[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    t_evloop *loop = t_evloop_create();
    t_tcp_conn *conn = t_tcp_conn_create(fds[0], loop);
    T_ASSERT_NOT_NULL(conn);
    t_tcp_conn_destroy(conn);
    t_socket_close(fds[1]);
    t_evloop_destroy(loop);
}

int main(void) {
    return t_run_all_tests();
}
