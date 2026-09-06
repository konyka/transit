#include "t_test.h"
#include "t_config.h"
#include "t_file.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Helper to build a C string with length */
static void run_parse_string(t_config *cfg, const char *s) {
    t_config_parse_string(cfg, s, strlen(s));
}

T_TEST(config_parse_string_basic) {
    t_config *cfg = t_config_create();
    const char *data = "host=localhost\nport=4222\n";
    run_parse_string(cfg, data);
    T_ASSERT(strcmp(t_config_get(cfg, "", "host"), "localhost") == 0);
    T_ASSERT_EQ(t_config_get_int(cfg, "", "port", 0), 4222);
    t_config_destroy(cfg);
}

T_TEST(config_parse_sections) {
    t_config *cfg = t_config_create();
    const char *data = "[server]\nhost=0.0.0.0\n[storage]\npath=/tmp\n";
    run_parse_string(cfg, data);
    /*Global section is created by default, so order is: "" , "server", "storage" */
    T_ASSERT(t_config_section_count(cfg) >= 3);
    T_ASSERT(strcmp(t_config_section_name(cfg, 1), "server") == 0);
    T_ASSERT(strcmp(t_config_section_name(cfg, 2), "storage") == 0);
    T_ASSERT(strcmp(t_config_get(cfg, "server", "host"), "0.0.0.0") == 0);
    T_ASSERT(strcmp(t_config_get(cfg, "storage", "path"), "/tmp") == 0);
    t_config_destroy(cfg);
}

T_TEST(config_comments_and_whitespace) {
    t_config *cfg = t_config_create();
    const char *data = "# a comment\n;another comment\n\n[sec]  \nhost = example.com  \n";
    run_parse_string(cfg, data);
    T_ASSERT(strcmp(t_config_get(cfg, "sec", "host"), "example.com") == 0);
    t_config_destroy(cfg);
}

T_TEST(config_empty_values) {
    t_config *cfg = t_config_create();
    const char *data = "key=\nkey2=\n";
    run_parse_string(cfg, data);
    T_ASSERT(strcmp(t_config_get(cfg, "", "key"), "") == 0);
    T_ASSERT(strcmp(t_config_get(cfg, "", "key2"), "") == 0);
    t_config_destroy(cfg);
}

T_TEST(config_overwrite) {
    t_config *cfg = t_config_create();
    const char *data = "k=1\nk=2\n";
    run_parse_string(cfg, data);
    T_ASSERT(strcmp(t_config_get(cfg, "", "k"), "2") == 0);
    t_config_destroy(cfg);
}

T_TEST(config_get_defaults) {
    t_config *cfg = t_config_create();
    const char *data = "";
    run_parse_string(cfg, data);
    T_ASSERT_EQ(t_config_get_int(cfg, "", "missing", 7), 7);
    t_config_destroy(cfg);
}

T_TEST(config_section_iteration) {
    t_config *cfg = t_config_create();
    const char *data = "[a]\nx=1\n[y]\nz=2\n";
    run_parse_string(cfg, data);
    /* global section at index 0, others follow */
    T_ASSERT(strcmp(t_config_section_name(cfg, 1), "a") == 0);
    T_ASSERT(strcmp(t_config_section_name(cfg, 2), "y") == 0);
    T_ASSERT(strcmp(t_config_key_name(cfg, "a", 0), "x") == 0);
    T_ASSERT(strcmp(t_config_key_name(cfg, "y", 0), "z") == 0);
    t_config_destroy(cfg);
}

T_TEST(config_parse_file) {
    t_config *cfg = t_config_create();
    const char *path = "test_transit_config.ini";
    const char *content = "[srv]\nhost=127.0.0.1\nport=1234\n";
    t_file f;
    t_file_unlink(path);
    t_file_init(&f);
    T_ASSERT_EQ(t_file_open(&f, path, T_FILE_WRITE | T_FILE_CREAT | T_FILE_TRUNC), 0);
    T_ASSERT_EQ(t_file_write(&f, content, strlen(content)), 0);
    t_file_close(&f);
    int r = t_config_parse_file(cfg, path);
    T_ASSERT_EQ(r, 0);
    T_ASSERT(strcmp(t_config_get(cfg, "srv", "host"), "127.0.0.1") == 0);
    T_ASSERT_EQ(t_config_get_int(cfg, "srv", "port", 0), 1234);
    t_file_unlink(path);
    t_config_destroy(cfg);
}

T_TEST(config_parse_file_missing_fails_closed) {
    t_config *cfg = t_config_create();
    T_ASSERT_EQ(t_config_parse_file(cfg, "test_transit_config_missing.ini"), -1);
    t_config_destroy(cfg);
}

int main(void) {
    return t_run_all_tests();
}
