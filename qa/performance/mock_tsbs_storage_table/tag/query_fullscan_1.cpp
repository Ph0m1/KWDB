#include "tag_perf.h"

INSTANTIATE_TEST_CASE_P(thread_1,
                        TagFullScanTest,
                        testing::Values(
                            TagPerfParams{
                                .common = {.thread_count = 1},
                                .query = {.total_row_count = 100,
                                          .threshold_max_query_us = 1000 }},
                            TagPerfParams{
                                .common = {.thread_count = 1},
                                .query = {.total_row_count = 4000,
                                          .threshold_max_query_us = 40000 }},
                            TagPerfParams{
                                .common = {.thread_count = 1},
                                .query = {.total_row_count = 100000,
                                          .threshold_max_query_us = 1000000 }},
                            TagPerfParams{
                                .common = {.thread_count = 1},
                                .query = {.total_row_count = 1000000,
                                          .threshold_max_query_us = 10000000 }},
                            TagPerfParams{
                                .common = {.thread_count = 1},
                                .query = {.total_row_count = 10000000,
                                          .threshold_max_query_us = 100000000 }}
                            ));