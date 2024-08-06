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

import os as _os
import os.path as _path
import subprocess as _subprocess
import threading as _threading
import queue as _queue
import io as _io
from .utils import debug_print


class NodeExecuter(object):
  def __init__(self, data_root: str) -> None:
    self.__input_lck = _threading.Lock()
    self.__interact_eof = '<<<interact-eof'
    with open(_path.join(data_root, 'kwbase.listen-addr')) as f:
      cmd = f"{_os.getenv('KWBIN')} sql --insecure --host={f.readline()} \
        --format=table --set 'errexit=false' \
          --set 'interact_eof={self.__interact_eof}' --echo-sql"
      debug_print(f'=========== cmd ===========\n{cmd}')
      self.__cli = _subprocess.Popen(
          cmd, executable='/bin/bash', stdin=_subprocess.PIPE,
          stdout=_subprocess.PIPE, stderr=_subprocess.STDOUT, shell=True,
          text=True)

  def Execute(self, stmt: str) -> str:
    try:
      self.__input_lck.acquire(blocking=True)
      debug_print(f'=========== Execute stmt ===========\n{stmt}')
      self.__cli.stdin.write(stmt)
      self.__cli.stdin.flush()
      output = ''
      while True:
        line = self.__cli.stdout.readline()
        debug_print(f'=========== read line ===========\n{line}')
        if line.rstrip() == self.__interact_eof:
          break
        output += line
      return output
    finally:
      self.__input_lck.release()

  def __del__(self):
    self.__cli.terminate()
    try:
      self.__cli.wait(2)
    except _subprocess.TimeoutExpired:
      self.__cli.kill()
