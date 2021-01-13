#ifndef BRIDGE_H
#define BRIDGE_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowSchema;
struct ArrowArray;

void trivial_release_schema(ArrowSchema *p);

void release_schema(ArrowSchema *p);

ArrowSchema *new_schema(const char *format, const char *name, int64_t flags,
                        int64_t n_children, ArrowSchema **children);

void delete_schema(ArrowSchema *p);

void trivial_release_array(ArrowArray *p);

void release_array(ArrowArray *p);

void delete_array(ArrowArray *p);

ArrowArray *new_array(int64_t length, int64_t null_count, int64_t offset,
                      int64_t n_buffers, void **buffers, int64_t n_children,
                      ArrowArray **children);

#ifdef __cplusplus
}
#endif

#endif
