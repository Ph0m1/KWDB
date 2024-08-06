#include "tag_perf.h"

INSTANTIATE_TEST_CASE_P(thread_8,
                        TagQueryTest,
                        testing::Values(
                            TagPerfParams{
                                .common = {.thread_count = 8},
                                .query = {.total_row_count = 100,
                                          .total_query_count = 8,
                                          .threshold_max_query_us = 3000 }},  // fault tolerance (amount of data is too small)
                            TagPerfParams{
                                .common = {.thread_count = 8},
                                .query = {.total_row_count = 4000,
                                          .total_query_count = 8,
                                          .threshold_max_query_us = 1000 }},
                            TagPerfParams{
                                .common = {.thread_count = 8},
                                .query = {.total_row_count = 100000,
                                          .total_query_count = 8,
                                          .threshold_max_query_us = 1000 }},
                            TagPerfParams{
                                .common = {.thread_count = 8},
                                .query = {.total_row_count = 1000000,
                                          .total_query_count = 8,
                                          .threshold_max_query_us = 1000 }},
                            TagPerfParams{
                                .common = {.thread_count = 8},
                                .query = {.total_row_count = 10000000,
                                          .total_query_count = 8,
                                          .threshold_max_query_us = 1000 }}
                            ));
