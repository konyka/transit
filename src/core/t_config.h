#ifndef T_CONFIG_H
#define T_CONFIG_H

#include <stddef.h>

typedef struct t_config t_config;

t_config *t_config_create(void);
void      t_config_destroy(t_config *cfg);

int  t_config_parse_file(t_config *cfg, const char *path);
int  t_config_parse_string(t_config *cfg, const char *data, size_t len);

const char *t_config_get(t_config *cfg, const char *section, const char *key);
int         t_config_get_int(t_config *cfg, const char *section, const char *key, int default_val);
double      t_config_get_double(t_config *cfg, const char *section, const char *key, double default_val);
int         t_config_has(t_config *cfg, const char *section, const char *key);

size_t t_config_section_count(t_config *cfg);
const char *t_config_section_name(t_config *cfg, size_t index);

size_t t_config_key_count(t_config *cfg, const char *section);
const char *t_config_key_name(t_config *cfg, const char *section, size_t index);

#endif
