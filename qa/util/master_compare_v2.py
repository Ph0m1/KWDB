#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# This software (KWDB) is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import subprocess as _subprocess


def diff(master_file: str, out_file: str) -> str:
  p = _subprocess.run(
      f'diff --color=always -y --suppress-common-lines -Z {master_file} {out_file}',
      shell=True, stderr=_subprocess.STDOUT, stdout=_subprocess.PIPE,
      text=True)
  return p.stdout
