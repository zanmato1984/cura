#pragma once

#include <cura/c/api.h>
#include <cura/c/arrow.h>

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

void trivial_release_schema(ArrowSchema *p) { p->release = NULL; }

void release_schema(ArrowSchema *p) {
  p->release(p);
  p->release = NULL;
}

ArrowSchema *new_schema(const char *format, const char *name, int64_t flags,
                        int64_t n_children, ArrowSchema **children) {
  ArrowSchema *p = malloc(sizeof(ArrowSchema));
  memset(p, 0, sizeof(ArrowSchema));
  p->format = format;
  p->name = name;
  p->flags = flags;
  p->n_children = n_children;
  if (n_children > 0) {
    p->children = malloc(sizeof(ArrowSchema *) * n_children);
  }
  for (int i = 0; i < n_children; i++) {
    p->children[i] = children[i];
  }
  p->release = &trivial_release_schema;
  return p;
}

void delete_schema(ArrowSchema *p) {
  for (int i = 0; i < p->n_children; i++) {
    delete_schema(p->children[i]);
  }

  free((char *)(p->format));
  free((char *)(p->name));
  free(p->children);
  free(p);
}

void trivial_release_array(ArrowArray *p) { p->release = NULL; }

void release_array(ArrowArray *p) {
  p->release(p);
  p->release = NULL;
}

void delete_array(ArrowArray *p) {
  for (int i = 0; i < p->n_children; i++) {
    free(p->children[i]);
  }

  for (int i = 0; i < p->n_buffers; i++) {
    free((void *)(p->buffers[i]));
  }
  free((char *)(p->buffers));
  free((char *)(p->children));
  free(p);
}

ArrowArray *new_array(int64_t length, int64_t null_count, int64_t offset,
                      int64_t n_buffers, void **buffers, int64_t n_children,
                      ArrowArray **children) {
  ArrowArray *p = malloc(sizeof(ArrowArray));
  memset(p, 0, sizeof(ArrowArray));
  p->length = length;
  p->null_count = null_count;
  p->offset = offset;
  p->n_buffers = n_buffers;
  if (n_buffers > 0) {
    p->buffers = malloc(sizeof(void *) * n_buffers);
  }
  for (int i = 0; i < n_buffers; i++) {
    p->buffers[i] = buffers[i];
  }
  p->n_children = n_children;
  if (n_children > 0) {
    p->children = malloc(sizeof(void *) * n_children);
  }
  for (int i = 0; i < n_children; i++) {
    p->children[i] = children[i];
  }
  p->release = &trivial_release_array;
  return p;
}
