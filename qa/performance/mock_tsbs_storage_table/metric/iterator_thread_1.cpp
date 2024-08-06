#include "metric_perf.h"

INSTANTIATE_TEST_CASE_P(thread_1,
                      MetricQueryTest,
                        testing::Values(
                            MetricTestParam{1, 10000}
                        ));
