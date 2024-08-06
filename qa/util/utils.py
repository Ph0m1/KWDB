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

from typing import List
from re import search as _research
from os import makedirs as _makedirs, path as _path, getenv as _getenv


class ConsoleColored:
  """Colourful print message base on bash syntax"""

  RED = "\033[0;31m"
  GREEN = "\033[0;32m"
  END = "\033[0m"

  @staticmethod
  def Red(s: str) -> str:
    return f"{ConsoleColored.RED}{s}{ConsoleColored.END}"

  @staticmethod
  def Green(s: str) -> str:
    return f"{ConsoleColored.GREEN}{s}{ConsoleColored.END}"


def FileWrite(file_path: str, content: str, mode='w+', encoding='utf-8'):
  """Write content into file

  Args:
      file_path (str): File absolute path
      content (str): Append content
      mode (str, optional): File open mode . Defaults to 'w+'.
      encoding (str, optional): Save encoding. Defaults to 'utf-8'.
  """
  basedir = _path.dirname(file_path)
  if not _path.exists(basedir):
    _makedirs(basedir, exist_ok=True)
  with open(file_path, mode=mode, encoding=encoding) as file_writer:
    file_writer.write(content)
    file_writer.flush()


def SpiltFile(file_path: str, re_pattern: str, mode='r', encoding='utf-8') -> List[List[str]]:
  """Load file content and split with regex expression, return multiline of collection.

  Args:
      file_path (str): Load file absolute path on host.
      re_pattern (str): Regex expression.
      mode (str, optional): File open mode. Defaults to 'r'.
      encoding (str, optional): Load file with encoding. Defaults to 'utf-8'.

  Returns:
      List[List[str]]: Multiline of collection.

      Example.

        [
          ['{pattern_str1}', 'scope_str1'],
          ['{pattern_str2}', 'scope_str2']
        ]
  """
  rs = list()
  cache_scope = ['', '']
  with open(file_path, mode=mode, encoding=encoding) as file_reader:
    for line in file_reader:
      if bool(_research(re_pattern, line)):
        if cache_scope[1]:
          rs.append(cache_scope)
          cache_scope = ['', '']
        cache_scope[0] = line
      else:
        cache_scope[1] += line
    if cache_scope[1]:
      rs.append(cache_scope)
  return rs


def direct_print(*args, **kwargs):
  print(*args, **kwargs, flush=True)


def debug_print(*args, **kwargs):
  if _getenv('QA_DEBUG'):
    direct_print(*args, **kwargs)


if __name__ == '__main__':
  # test SplitFile
  import os
  os.system(
      "echo '--testcase\n1\n2\n--testcase\n3\n4\n--testcase' > /tmp/test_SplitFile")
  sf = SpiltFile('/tmp/test_SplitFile', re_pattern='--testcase')
  print(sf)
  assert 3 == len(sf)
  os.system("echo '1\n2\n--testcase\n9' > /tmp/test_SplitFile")
  sf = SpiltFile('/tmp/test_SplitFile', re_pattern='--testcase')
  print(sf)
  assert 2 == len(sf)
