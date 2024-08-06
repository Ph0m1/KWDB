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


#ifndef OBJCNTL_H_
#define OBJCNTL_H_

#include <fcntl.h>

#define IN_CREATE	0x00000001
#define IN_WRITE	0x00000002

#define O_NORECURSIVE   0x01000000
#define O_MATERIALIZATION  0x02000000
#define O_ANONYMOUS     0x08000000

#define DEFAULT_STREAM_CACHE_SIZE   1024*1024
#define MIN_STREAM_CACHE_SIZE       512
#define MAX_STREAM_CACHE_SIZE       16*1024*1024

// O_DIRECT doesn't work under linux container

#define MMAP_OPEN             (O_RDWR)
#define MMAP_OPEN_NORECURSIVE (O_RDWR | O_NORECURSIVE)
#define MMAP_CREATOPEN        (O_RDWR | O_CREAT)
#define MMAP_CREATTRUNC       (O_RDWR | O_CREAT | O_TRUNC)
#define MMAP_CREAT_ANONYMOUS  (O_RDWR | O_CREAT | O_EXCL | O_TRUNC | O_ANONYMOUS)
#define MMAP_CREAT_EXCL       (O_RDWR | O_CREAT | O_EXCL)
#define MMAP_CREAT_MATERIALIZATION (O_RDWR | O_CREAT | O_EXCL | O_TRUNC | O_MATERIALIZATION)

#endif /* OBJCNTL_H_ */
