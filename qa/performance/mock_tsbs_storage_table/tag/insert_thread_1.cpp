#include "tag_perf.h"

INSTANTIATE_TEST_CASE_P(thread_1,
                        TagInsertTest,
                        testing::Values(
                            TagPerfParams{
                              .common = {.thread_count = 1},
                              .insert = {.row_count_per_thread = 100,
                                         .threshold_min_rps = 100000}},
                            TagPerfParams{
                              .common = {.thread_count = 1},
                              .insert = {.row_count_per_thread = 4000,
                                         .threshold_min_rps = 500000}},
                            TagPerfParams{
                              .common = {.thread_count = 1},
                              .insert = {.row_count_per_thread = 100000,
                                         .threshold_min_rps = 500000}},
                            TagPerfParams{
                              .common = {.thread_count = 1},
                              .insert = {.row_count_per_thread = 1000000,
                                         .threshold_min_rps = 500000}},
                            TagPerfParams{
                              .common = {.thread_count = 1},
                              .insert = {.row_count_per_thread = 10000000,
                                         .threshold_min_rps = 500000}}
                            ));
