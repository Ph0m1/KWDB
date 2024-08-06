#include "metric_perf.h"

INSTANTIATE_TEST_CASE_P(thread_12,
                        MetricInsertTest,
                        testing::Values(
                            MetricTestParam{12, 100, 10000}
                        ));
