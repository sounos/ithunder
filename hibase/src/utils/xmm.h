#ifndef _XMM_H_
#define _XMM_H_
void* xmm_mnew(size_t size);
void* xmm_new(size_t size);
void* xmm_mrenew(void *old, size_t old_size, size_t new_size);
void* xmm_renew(void *old, size_t old_size, size_t new_size);
void* xmm_mresize(void *old, size_t old_size, size_t new_size);
void* xmm_resize(void *old, size_t old_size, size_t new_size);
void xmm_free(void *m, size_t size);
#endif
