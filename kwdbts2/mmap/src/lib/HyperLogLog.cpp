// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <iostream>
#include <cmath>
#include "lib/HyperLogLog.h"
#include "BigObjectUtils.h"

// POT[i] = 1/(2^i)
#define POT_SIZE        64

static double POT[POT_SIZE];
static double alpha;

using namespace std;


int hllSize(int p) {
    // Note:
    // HyerLogLog needs one more sentry byte = (1 + hllsz)

    size_t hllsz = (((1 << p) * HLL_BITS + 7) / 8);
    return (int)getPageOffset(hllsz + 1, 4);    // add one and align to 4
}

void initHyperLogLog() {
  POT[0] = 1.0;
  for (size_t i = 1; i < POT_SIZE; ++i) {
    POT[i] = 1 / ((double)(1ULL << i));
  }
  alpha = 0.7213 / ( 1.0 + 1.079 / (HLL_REG_COUNT));
}

//                          byte
// ... | x x x x x x x x | x x x x x x x x | ... 0
//              |<- ub ->|<- lb ->|

size_t HLLCount(void *p) {
    double E = 0.0;
    int zeros = 0;
    for (size_t j = 0; j < HLL_REG_COUNT; j++) {
        size_t pb = j * HLL_BITS / 8;
        uint32_t lb = (j * HLL_BITS) & 7;
        uint32_t ub = 8 - lb;
        uint8_t b0 = ((uint8_t *)p)[pb];
        uint8_t b1 = ((uint8_t *)p)[pb+1];
        uint32_t reg_count = (((b0 >> lb) | (b1 << ub)) & HLL_REGISTER_MASK);
        if (reg_count == 0) {
            zeros++;
        }
        E += POT[reg_count];
    }
    double m = (double)(HLL_REG_COUNT);
    double hllE = alpha * m * m / E;
    double H = (zeros != 0) ? m * log(m / zeros) : hllE;
    if (H <= 220)
        return H;
//    return hllE;

    double zl = log(zeros + 1);
    double beta = -0.370393911 * zeros +
                   0.070471823 * zl +
                   0.17393686 * pow(zl, 2) +
                   0.16339839 * pow(zl, 3) +
                  -0.09237745 * pow(zl, 4) +
                   0.03738027 * pow(zl, 5) +
                  -0.005384159 * pow(zl, 6) +
                   0.00042419 * pow(zl, 7);

    return (size_t)(llroundl(alpha * m * (m - zeros) * (1 / (E + beta))));
}



