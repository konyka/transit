#include "t_config.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Internal simple data structures: sections containing key/value pairs */
typedef struct {
    char *key;
    char *value;
} t_config_kv;

typedef struct {
    char *name;      /* section name, empty string for global */
    t_config_kv *kv; /* array of key/value pairs */
    size_t kv_count;
    size_t kv_cap;
} t_config_section;

struct t_config {
    t_config_section *sections;
    size_t section_count;
    size_t section_cap;
};

static t_config_section *cfg_find_section(t_config *cfg, const char *name, int create_if_missing) {
    if (!cfg) return NULL;
    for (size_t i = 0; i < cfg->section_count; ++i) {
        if (strcmp(cfg->sections[i].name, name) == 0) {
            return &cfg->sections[i];
        }
    }
    if (!create_if_missing) return NULL;
    if (cfg->section_count == cfg->section_cap) {
        size_t new_cap = cfg->section_cap ? cfg->section_cap * 2 : 4;
        t_config_section *tmp = (t_config_section*)realloc(cfg->sections, new_cap * sizeof(t_config_section));
        if (!tmp) return NULL;
        cfg->sections = tmp;
        cfg->section_cap = new_cap;
    }
    char *sec_name = strdup(name);
    if (!sec_name) return NULL;
    t_config_section *sec = &cfg->sections[cfg->section_count];
    sec->name = sec_name;
    sec->kv = NULL;
    sec->kv_count = 0;
    sec->kv_cap = 0;
    cfg->section_count++;
    return sec;
}

static t_config_kv* cfg_find_kv(t_config_section *sec, const char *key) {
    for (size_t i = 0; i < sec->kv_count; ++i) {
        if (strcmp(sec->kv[i].key, key) == 0) return &sec->kv[i];
    }
    return NULL;
}

static int cfg_kv_append(t_config_section *sec, const char *key, const char *value) {
    if (sec->kv_count == sec->kv_cap) {
        size_t new_cap = sec->kv_cap ? sec->kv_cap * 2 : 4;
        t_config_kv *tmp = (t_config_kv*)realloc(sec->kv, new_cap * sizeof(t_config_kv));
        if (!tmp) return -1;
        sec->kv = tmp;
        sec->kv_cap = new_cap;
    }
    char *k = strdup(key);
    char *v = strdup(value);
    if (!k || !v) {
        free(k);
        free(v);
        return -1;
    }
    sec->kv[sec->kv_count].key = k;
    sec->kv[sec->kv_count].value = v;
    sec->kv_count++;
    return 0;
}

static int cfg_set_kv(t_config_section *sec, const char *key, const char *value) {
    t_config_kv *existing = cfg_find_kv(sec, key);
    if (existing) {
        char *nv = strdup(value);
        if (!nv) return -1;
        free(existing->value);
        existing->value = nv;
        return 0;
    }
    return cfg_kv_append(sec, key, value);
}

/* trim helpers: return newly allocated trimmed copy of s [start, end) */
static char *trim_copy(const char *start, const char *end) {
    while (start < end && (*start == ' ' || *start == '\t' || *start == '\r')) start++;
    while (end > start && (*(end-1) == ' ' || *(end-1) == '\t' || *(end-1) == '\r')) end--;
    size_t len = end - start;
    char *out = (char*)malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
}

static int ensure_empty_section(t_config *cfg) {
    return cfg_find_section(cfg, "", 1) != NULL ? 0 : -1;
}

static int ensure_initialized(t_config *cfg) {
    if (!cfg) return -1;
    if (cfg->section_count == 0) {
        return ensure_empty_section(cfg);
    }
    return 0;
}

/* Public API */
t_config *t_config_create(void) {
    t_config *cfg = (t_config*)calloc(1, sizeof(t_config));
    if (!cfg) return NULL;
    cfg->section_cap = 4;
    cfg->sections = (t_config_section*)calloc(cfg->section_cap, sizeof(t_config_section));
    if (!cfg->sections) {
        free(cfg);
        return NULL;
    }
    cfg->section_count = 0;
    return cfg;
}

void t_config_destroy(t_config *cfg) {
    if (!cfg) return;
    for (size_t i = 0; i < cfg->section_count; ++i) {
        t_config_section *sec = &cfg->sections[i];
        if (sec->name) free(sec->name);
        if (sec->kv) {
            for (size_t j = 0; j < sec->kv_count; ++j) {
                free(sec->kv[j].key);
                free(sec->kv[j].value);
            }
            free(sec->kv);
        }
    }
    free(cfg->sections);
    free(cfg);
}

/* Parse file by reading it into memory and delegating to string parser */
int t_config_parse_file(t_config *cfg, const char *path) {
    if (!cfg || !path) return -1;
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) sz = 0;
    char *data = (char*)malloc((size_t)sz + 1);
    if (!data) { fclose(f); return -1; }
    size_t rd = fread(data, 1, (size_t)sz, f);
    data[rd] = '\0';
    fclose(f);
    int res = t_config_parse_string(cfg, data, rd);
    free(data);
    return res;
}

/* Core INI parser: supports sections, key=value, comments (# or ;) */
int t_config_parse_string(t_config *cfg, const char *data, size_t len) {
    if (!cfg) return -1;
    if (ensure_initialized(cfg) != 0) return -1;
    const char *p = data;
    const char *end = data + len;
    const char *line_start = p;
    t_config_section *cur_sec = NULL;
    cur_sec = cfg_find_section(cfg, "", 1);
    if (!cur_sec) return -1;
    while (p < end) {
        const char *line_end = p;
        while (line_end < end && *line_end != '\n' && *line_end != '\r') line_end++;
        size_t line_len = line_end - line_start;

        const char *s = line_start;
        while (s < line_end && (*s == ' ' || *s == '\t')) s++;
        int is_comment = (s < line_end && (*s == '#' || *s == ';'));
        if (line_len > 0 && !is_comment) {
            if (*s == '[') {
                const char *name_start = s + 1;
                const char *name_end = line_end;
                while (name_end > name_start && *(name_end-1) != ']') name_end--;
                if (name_end > name_start) name_end--;
                while (name_end > name_start && (*(name_end-1) == ' ' || *(name_end-1) == '\t')) name_end--;
                char *name = trim_copy(name_start, name_end);
                if (!name) return -1;
                cur_sec = cfg_find_section(cfg, name, 1);
                free(name);
                if (!cur_sec) return -1;
            } else {
                const char *eq = NULL;
                for (const char *qp = s; qp < line_end; ++qp) {
                    if (*qp == '=') { eq = qp; break; }
                }
                if (eq) {
                    char *key = trim_copy(line_start, eq);
                    char *val = trim_copy(eq + 1, line_end);
                    if (!key || !val) {
                        free(key);
                        free(val);
                        return -1;
                    }
                    if (!cur_sec || cfg_set_kv(cur_sec, key, val) != 0) {
                        free(key);
                        free(val);
                        return -1;
                    }
                    free(key);
                    free(val);
                }
            }
        }
        line_start = line_end;
        if (line_start < end && *line_start == '\r') line_start++;
        if (line_start < end && *line_start == '\n') line_start++;
        p = line_start;
    }
    return 0;
}

/* Accessors */
const char *t_config_get(t_config *cfg, const char *section, const char *key) {
    if (!cfg || !section || !key) return NULL;
    t_config_section *sec = cfg_find_section(cfg, section, 0);
    if (!sec) return NULL;
    t_config_kv *kv = cfg_find_kv(sec, key);
    return kv ? kv->value : NULL;
}

int t_config_get_int(t_config *cfg, const char *section, const char *key, int default_val) {
    const char *v = t_config_get(cfg, section, key);
    if (!v || v[0] == '\0') return default_val;
    char *endp = NULL;
    long val = strtol(v, &endp, 10);
    if (endp == v || *endp != '\0') return default_val;
    return (int)val;
}

double t_config_get_double(t_config *cfg, const char *section, const char *key, double default_val) {
    const char *v = t_config_get(cfg, section, key);
    if (!v || v[0] == '\0') return default_val;
    char *endp = NULL;
    double val = strtod(v, &endp);
    if (endp == v || *endp != '\0') return default_val;
    return val;
}

int t_config_has(t_config *cfg, const char *section, const char *key) {
    const char *v = t_config_get(cfg, section, key);
    return v != NULL;
}

size_t t_config_section_count(t_config *cfg) {
    return cfg ? cfg->section_count : 0;
}

const char *t_config_section_name(t_config *cfg, size_t index) {
    if (!cfg || index >= cfg->section_count) return NULL;
    return cfg->sections[index].name;
}

size_t t_config_key_count(t_config *cfg, const char *section) {
    if (!cfg || !section) return 0;
    t_config_section *sec = cfg_find_section(cfg, section, 0);
    if (!sec) return 0;
    return sec->kv_count;
}

const char *t_config_key_name(t_config *cfg, const char *section, size_t index) {
    if (!cfg || !section) return NULL;
    t_config_section *sec = cfg_find_section(cfg, section, 0);
    if (!sec) return NULL;
    if (index >= sec->kv_count) return NULL;
    return sec->kv[index].key;
}
