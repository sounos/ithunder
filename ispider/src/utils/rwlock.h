#ifndef __RWLOCK__H
#define __RWLOCK__H
#ifdef HAVE_PTHREAD
#define RWLOCK_INIT(x)          pthread_rwlock_init(&(x), NULL)
#define RWLOCK_RDLOCK(x)        pthread_rwlock_rdlock(&(x))
#define RWLOCK_WRLOCK(x)        pthread_rwlock_wrlock(&(x))
#define RWLOCK_UNLOCK(x)        pthread_rwlock_unlock(&(x))
#define RWLOCK_DESTROY(x)       pthread_rwlock_destroy(&(x))
#else
#define RWLOCK_INIT(x)
#define RWLOCK_RDLOCK(x)
#define RWLOCK_WRLOCK(x)
#define RWLOCK_UNLOCK(x)
#define RWLOCK_DESTROY(x)
#endif
#endif
