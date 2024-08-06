#include "tag_perf.h"

INSTANTIATE_TEST_CASE_P(thread_12,
                        TagInsertTest,
                        testing::Values(
                            TagPerfParams{
                              .common = {.thread_count = 12},
                              .insert = {.row_count_per_thread = 9,
                                         .threshold_min_rps = 80000}},
                            TagPerfParams{
                              .common = {.thread_count = 12},
                              .insert = {.row_count_per_thread = 334,
                                         .threshold_min_rps = 300000}},
                            TagPerfParams{
                              .common = {.thread_count = 12},
                              .insert = {.row_count_per_thread = 8334,
                                         .threshold_min_rps = 300000}},
                            TagPerfParams{
                              .common = {.thread_count = 12},
                              .insert = {.row_count_per_thread = 83334,
                                         .threshold_min_rps = 300000}},
                            TagPerfParams{
                              .common = {.thread_count = 12},
                              .insert = {.row_count_per_thread = 833334,
                                         .threshold_min_rps = 300000}}
                            ));
