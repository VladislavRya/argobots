/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 * See COPYRIGHT in top-level directory.
 */

/*
 * Creates multiple execution streams and runs ULTs on these execution streams.
 * Users can change the number of execution streams and the number of ULT via
 * arguments. Each ULT prints its ID.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <abt.h>
#include <string.h>

#define DEFAULT_NUM_XSTREAMS 2
#define DEFAULT_NUM_THREADS 8

#define NUM_ELEMS 1024


typedef struct {
    ABT_xstream *xstreams;
    int num_xstreams;
    ABT_pool *pools;
    int num_pools;
    ABT_thread *threads;
    int num_threads;
} reduction_context_t;


typedef struct {
    void *array;                         /* array, on which reduction will be performed */
    size_t num_elems;                    /* number of elements to perform reduction on */
    size_t elem_size;                    /* size of a single element */
    void* default_reduction_value;       /* 0 for sum, 1 for multiplication, etc. */
    void *result;                        /* where to store the result of reduction */
    void (*reduce_func)(void *, void *); /* provided reduction function on 2 elements */
    ABT_mutex mutex;                     /* mutex to perform final (among different threads) reduction */
} reduction_args_t;


void reduction_thread(void *arg) {
    reduction_args_t *reduction_args = (reduction_args_t *)arg;
    size_t num_elems = reduction_args->num_elems;
    size_t elem_size = reduction_args->elem_size;
    char *array = (char *)reduction_args->array;
    
    // Initialize local result to default value of a reduciton
    void *local_result = (void *)malloc(elem_size);
    memcpy(local_result, reduction_args->default_reduction_value, elem_size);

    for (size_t i = 0; i < num_elems; ++i) {
        reduction_args->reduce_func(local_result, array + i * elem_size);
    }

    ABT_mutex_lock(reduction_args->mutex);
    reduction_args->reduce_func(reduction_args->result, local_result);
    ABT_mutex_unlock(reduction_args->mutex);

    free(local_result);
}


void reduce_common(
    reduction_context_t *reduction_context,
    void *array,
    size_t num_elems,
    size_t elem_size,
    void *default_reduction_value,
    void (*reduce_func)(void *, void *),
    void *result
) {
    int num_threads = reduction_context->num_threads;
    size_t elems_per_thread = num_elems / num_threads;
    reduction_args_t *thread_args = 
        (reduction_args_t *)malloc(sizeof(reduction_args_t) * num_threads);
    ABT_mutex mutex;
    ABT_mutex_create(&mutex);

    for (int i = 0; i < num_threads; ++i) {
        thread_args[i].array = (char *)array + i * elems_per_thread * elem_size;
        thread_args[i].num_elems = (i == num_threads - 1) ? (num_elems - i * elems_per_thread) : elems_per_thread;
        thread_args[i].elem_size = elem_size;
        thread_args[i].default_reduction_value = default_reduction_value;
        thread_args[i].result = result;
        thread_args[i].reduce_func = reduce_func;
        thread_args[i].mutex = mutex;
    }

    for (int i = 0; i < num_threads; ++i) {
        int pool_id = i % reduction_context->num_xstreams;
        ABT_thread_create(
            reduction_context->pools[pool_id],
            reduction_thread,
            &thread_args[i],
            ABT_THREAD_ATTR_NULL,
            &(reduction_context->threads[i])
        );

    }

    for (int i = 0; i < num_threads; ++i) {
        ABT_thread_join(reduction_context->threads[i]);
    }

    ABT_mutex_free(&mutex);
    free(thread_args);
}

void sum_reduction_func(void *a, void *b) {
    *((int *)a) += *((int *)b);
}

int main(int argc, char **argv)
{
    int i;
    /* Read arguments. */
    int num_xstreams = DEFAULT_NUM_XSTREAMS;
    int num_threads = DEFAULT_NUM_THREADS;
    while (1) {
        int opt = getopt(argc, argv, "he:n:");
        if (opt == -1)
            break;
        switch (opt) {
            case 'e':
                num_xstreams = atoi(optarg);
                break;
            case 'n':
                num_threads = atoi(optarg);
                break;
            case 'h':
            default:
                printf("Usage: ./reduction_sum [-e NUM_XSTREAMS] "
                       "[-n NUM_THREADS]\n");
                return -1;
        }
    }
    if (num_xstreams <= 0)
        num_xstreams = 1;
    if (num_threads <= 0)
        num_threads = 1;

    /* Allocate memory. */
    ABT_xstream *xstreams =
        (ABT_xstream *)malloc(sizeof(ABT_xstream) * num_xstreams);
    int num_pools = num_xstreams;
    ABT_pool *pools = (ABT_pool *)malloc(sizeof(ABT_pool) * num_pools);
    ABT_thread *threads =
        (ABT_thread *)malloc(sizeof(ABT_thread) * num_threads);
    

    /* Initialize Argobots. */
    ABT_init(argc, argv);

    /* Get a primary execution stream. */
    ABT_xstream_self(&xstreams[0]);

    /* Create secondary execution streams. */
    for (i = 1; i < num_xstreams; i++) {
        ABT_xstream_create(ABT_SCHED_NULL, &xstreams[i]);
    }

    /* Get default pools. */
    for (i = 0; i < num_xstreams; i++) {
        ABT_xstream_get_main_pools(xstreams[i], 1, &pools[i]);
    }

    /* Create array to reduce. */
    size_t elem_size = sizeof(int);
    int *array = (int *)malloc(sizeof(int) * NUM_ELEMS);
    for (size_t idx = 0; idx < NUM_ELEMS; ++idx) {
      array[idx] = idx;
    }

    int default_reduction_value = 0;
    int result = 0;
    reduction_context_t reduction_context = {
        .xstreams = xstreams,
        .num_xstreams = num_xstreams,
        .pools = pools,
        .num_pools = num_pools,
        .threads = threads,
        .num_threads = num_threads,
    };

    reduce_common(
        &reduction_context,
        array,
        NUM_ELEMS,
        elem_size,
        &default_reduction_value,
        sum_reduction_func,
        &result
    );

    /* Join and free ULTs. */
    for (i = 0; i < num_threads; i++) {
        // ABT_thread_join(threads[i]);
        ABT_thread_free(&threads[i]);
    }

    /* Join and free secondary execution streams. */
    for (i = 1; i < num_xstreams; i++) {
        ABT_xstream_join(xstreams[i]);
        ABT_xstream_free(&xstreams[i]);
    }

    /* Finalize Argobots. */
    ABT_finalize();

    /* Free allocated memory. */
    free(xstreams);
    free(pools);
    free(threads);

    printf("Reduction result=%d\n", result);

    free(array);

    return 0;
}
