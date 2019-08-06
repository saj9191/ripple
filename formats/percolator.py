# This file is part of Ripple.

# Ripple is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Ripple is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Ripple.  If not, see <https://www.gnu.org/licenses/>.

import tsv
import util


class Iterator(tsv.Iterator):
  QVALUE_INDEX = 7
  THRESHOLD = 0.01

  def __init__(self, obj, chunk_size):
    tsv.Iterator.__init__(self, obj, chunk_size)
    self.cls = Iterator

  def getQValue(line):
    return float(line.split(tsv.Iterator.COLUMN_SEPARATOR)[Iterator.QVALUE_INDEX])

  def get(obj, start_byte, end_byte, identifier=""):
    content = util.read(obj, start_byte, end_byte)
    lines = list(content.split(tsv.Iterator.IDENTIFIER))

    if identifier == "q-value":
      lines = list(filter(lambda line: len(line.strip()) > 0, lines))
      lines = list(map(lambda line: (Iterator.getQValue(line), line), lines))
    elif identifier != "":
      raise Exception("Unknown identifier for percolator format", identifier)

    return lines

  def sum(self, identifier):
    more = True
    count = 0
    while more:
      [lines, more] = self.next(identifier)
      for line in lines:
        if line[0] <= Iterator.THRESHOLD:
          count += 1
    return count
