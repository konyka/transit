#include "t_thread.h"
#include <stdlib.h>

#if T_PLATFORM_WINDOWS
#include <windows.h>

typedef struct t_thread_start {
    void *(*fn)(void *);
    void *arg;
} t_thread_start;

static DWORD WINAPI t_thread_win_main(void *p) {
    t_thread_start s = *(t_thread_start *)p;
    free(p);
    (void)s.fn(s.arg);
    return 0;
}
#else
#include <sched.h>
#endif

int t_thread_spawn(t_thread *th, void *(*fn)(void *), void *arg) {
    if (!th || !fn) return -1;
#if T_PLATFORM_WINDOWS
    {
        t_thread_start *s;
        HANDLE h;
        DWORD tid = 0;
        s = (t_thread_start *)malloc(sizeof(*s));
        if (!s) return -1;
        s->fn = fn;
        s->arg = arg;
        h = CreateThread(NULL, 0, t_thread_win_main, s, 0, &tid);
        if (!h) {
            free(s);
            return -1;
        }
        th->os = (void *)h;
        th->tid = (unsigned)tid;
        return 0;
    }
#else
    return pthread_create(&th->pt, NULL, fn, arg) == 0 ? 0 : -1;
#endif
}

int t_thread_join(t_thread *th) {
    if (!th) return -1;
#if T_PLATFORM_WINDOWS
    if (!th->os) return -1;
    if (WaitForSingleObject((HANDLE)th->os, INFINITE) != WAIT_OBJECT_0) return -1;
    CloseHandle((HANDLE)th->os);
    th->os = NULL;
    return 0;
#else
    return pthread_join(th->pt, NULL) == 0 ? 0 : -1;
#endif
}

void t_thread_yield(void) {
#if T_PLATFORM_WINDOWS
    (void)SwitchToThread();
#else
    (void)sched_yield();
#endif
}
