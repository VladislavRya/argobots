/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 * See COPYRIGHT in top-level directory.
 */

/*
 * Creates multiple execution streams and runs ULTs on these execution streams.
 * Users can change the number of execution streams and the number of ULT via
 * arguments. Each ULT performs a reduction operation on a given array.
 */

#include "abt_reduction.h"

#include <float.h>
#include <getopt.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DEFAULT_NUM_XSTREAMS 2
#define DEFAULT_NUM_THREADS 8

#define NUM_ELEMS 1024

int check_not_equal(int result, int expected, const char *test_name) {
    printf("%s: result=%d, expected=%d\n", test_name, result, expected);
    return result != expected;
}

int test_sum_int(reduction_context_t* reduction_context) {
    // init
    int bad_tests = 0;
    int *array = (int *)malloc(sizeof(int) * NUM_ELEMS);
    int result = 0;

    // all zeroes array
    for (size_t idx = 0; idx < NUM_ELEMS; ++idx) {
      array[idx] = 0;
    }
    reduce_sum_int(
        reduction_context,
        array,
        NUM_ELEMS,
        &result
    );
    bad_tests += check_not_equal(result, 0, "int_sum_all_zeroes");
    result = 0;

    // one element is not zero
    enum { NON_ZERO_VALUE = 123 };
    array[0] = NON_ZERO_VALUE;
    reduce_sum_int(
        reduction_context,
        array,
        NUM_ELEMS,
        &result
    );
    bad_tests += check_not_equal(result, NON_ZERO_VALUE, "int_sum_one_not_zero");
    result = 0;

    // only ones
    for (size_t idx = 0; idx < NUM_ELEMS; ++idx) {
      array[idx] = 1;
    }
    reduce_sum_int(
        reduction_context,
        array,
        NUM_ELEMS,
        &result
    );
    bad_tests += check_not_equal(result, NUM_ELEMS, "int_sum_all_ones");
    result = 0;

    // arithmetic progression
    for (size_t idx = 0; idx < NUM_ELEMS; ++idx) {
      array[idx] = idx;
    }
    reduce_sum_int(
        reduction_context,
        array,
        NUM_ELEMS,
        &result
    );
    bad_tests += check_not_equal(result, (NUM_ELEMS * (NUM_ELEMS - 1)) / 2, "int_sum_arithmetic_progression");
    result = 0;

    // free resources
    free(array);

    return bad_tests;
}

int test_different_reductions(reduction_context_t* reduction_context) {
    int bad_tests = 0;

    bad_tests += test_sum_int(reduction_context);

    return bad_tests;
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

    reduction_context_t reduction_context = {
        .xstreams = xstreams,
        .num_xstreams = num_xstreams,
        .pools = pools,
        .num_pools = num_pools,
        .threads = threads,
        .num_threads = num_threads,
    };

    int failed_tests = test_different_reductions(&reduction_context);
    if (failed_tests > 0) {
        printf("Failed %d tests\n", failed_tests);
        return -1;
    }

    /* Free ULTs. */
    for (i = 0; i < num_threads; i++) {
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

    return 0;
}
