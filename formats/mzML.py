import boto3
import hashlib
import iterator
import re
import util
import xml.etree.ElementTree as ET
from enum import Enum
from iterator import Delimiter, DelimiterPosition, OffsetBounds, Options
from typing import Any, BinaryIO, ClassVar, Dict, Iterable, List, Optional, Pattern, Tuple


class Identifiers(Enum):
  mass = 0
  tic = 1


class Iterator(iterator.Iterator[Identifiers]):
  chromatogram_end_index: ClassVar[int]
  chromatogram_list_close_tag: ClassVar[str] = "</chromotogramList>"
  chromatogram_offset_regex: ClassVar[Pattern[str]] = re.compile('<offset idRef="TIC">(\d+)</offset>')
  chromatogram_start_index: ClassVar[int]
  delimiter: Delimiter = Delimiter("</spectrum>\n", "</offset>\n", DelimiterPosition.end)
  chunk_size: ClassVar[int] = 1000
  footer_end_index: ClassVar[int]
  footer_start_index: ClassVar[int]
  header_end_index: ClassVar[int]
  header_start_index: ClassVar[int]
  id_regex: ClassVar[Pattern[str]] = re.compile(".*scan=([0-9]+).*")
  identifiers: Identifiers
  index_list_offset: ClassVar[int]
  index_list_offset_regex: ClassVar[Pattern[str]] = re.compile("<indexListOffset>(\d+)</indexListOffset>")
  offset_regex: ClassVar[Pattern[str]] = re.compile("<offset[^>]*scan=[^>]*>(\d+)</offset>")
  offset_end_index: int
  offset_start_index: int
  options: ClassVar[Options] = Options(has_header=True)
  spectrum_list_close_tag: ClassVar[str] = "</spectrumList>"
  spectrum_list_count_regex: ClassVar[Pattern[str]] = re.compile('<spectrumList [\s\S]*count="(\d+)"')
  num_spectra: int = -1
  xml_namespace: ClassVar[str] = "http://psi.hupo.org/ms/mzml"

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)
    ET.register_namespace("", self.xml_namespace)
    self.__setup__()

  @classmethod
  def __add_content__(cls: Any, content: str, f: BinaryIO) -> str:
    f.write(content)
    return content

  @classmethod
  def __add_footer__(cls: Any, f: BinaryIO, content: str, offsets: List[Tuple[int, int]], metadata: Dict[str, str]):
    content += cls.__add_content__("</spectrumList>", f)
    metadata["footer_start_index"] = str(len(content))
    content += cls.__add_content__("</run></mzML>\n", f)
    list_offset = len(content)
    metadata["index_list_offset"] = str(list_offset)
    content += cls.__add_content__('<indexList count="1">\n', f)
    content += cls.__add_content__('<index name="spectrum">\n', f)
    for o in offsets:
      content += cls.__add_content__('<offset idRef="controllerType=0 controllerNumber=1 scan={0:s}">{1:d}</offset>\n'.format(o[0], o[1]), f)
    content += cls.__add_content__("</index>\n", f)
    content += cls.__add_content__("</indexList>\n", f)
    content += cls.__add_content__("<indexListOffset>{0:d}</indexListOffset>\n".format(list_offset), f)
    content += cls.__add_content__("<fileChecksum>", f)
    content += cls.__add_content__(str(hashlib.sha1(content.encode("utf-8")).hexdigest()), f)
    content += cls.__add_content__("</fileChecksum>\n</indexedmzML>", f)
    metadata["footer_end_index"] = str(len(content))
    metadata["num_spectra"] = str(len(offsets))

  @classmethod
  def __create_header__(cls: Any, f: BinaryIO, header: str, count: int, metadata: Dict[str, str]) -> str:
    header = re.sub(cls.spectrum_list_count_regex, '<spectrumList count="{0:d}"'.format(count), header)
    metadata["header_start_index"] = str(0)
    metadata["header_end_index"] = str(len(header) - 1)
    metadata["chromatogram_start_index"] = "-1"
    metadata["chromatogram_end_index"] = "-1"
    metadata["count"] = str(count)

    content = cls.__add_content__(header, f)
    return content

  @classmethod
  def __cv_param__(cls: Any, item: str, name: str) -> Optional[float]:
    spectrum = ET.fromstring(item)
    for cv_param in spectrum.iter("cv_param"):
      if cv_param.get("name") == name:
        return float(cv_param.get("value"))
    return None

  def __get_header_offset__(self):
    if "header_start_index" in self.obj.metadata:
      self.header_start_index = int(self.obj.metadata["header_start_index"])
      self.header_end_index = int(self.obj.metadata["header_end_index"])
    else:
      self.header_start_index = 0
      start_byte: int = max(0, self.index_list_offset - self.chunk_size)
      end_byte: int = min(self.index_list_offset + self.chunk_size, self.obj.content_length)
      stream: str = util.read(self.obj, start_byte, end_byte)
      offset_matches: List[Any] = list(self.offset_regex.finditer(stream))
      assert(len(offset_matches) > 0)
      self.header_end_index = int(offset_matches[0].group(1)) - 1

  def __get_footer_offset__(self):
    if "footer_start_index" in self.obj.metadata:
      self.footer_start_index = int(self.obj.metadata["footer_start_index"])
      self.footer_end_index = int(self.obj.metadata["footer_end_index"])
    else:
      if self.chromatogram_start_index == -1:
        start_byte = self.index_list_offset - self.chunk_size
      else:
        start_byte = self.chromatogram_start_index - self.chunk_size
      start_byte = max(0, start_byte)
      end_byte = min(start_byte + self.chunk_size, self.obj.content_length)
      stream = util.read(self.obj, start_byte, end_byte)
      self.footer_start_index = start_byte + stream.rindex(self.spectrum_list_close_tag)
      self.footer_end_index = self.obj.content_length

  def __get_index_list_offset__(self):
    if "index_list_offset" in self.obj.metadata:
      self.index_list_offset = int(self.obj.metadata["index_list_offset"])
      self.chromatogram_start_index = int(self.obj.metadata["chromatogram_start_index"])
      self.chromatogram_end_index = int(self.obj.metadata["chromatogram_end_index"])
    else:
      end_byte: int = self.obj.content_length
      start_byte: int = end_byte - self.chunk_size
      stream: str = util.read(self.obj, start_byte, end_byte)
      m = self.index_list_offset_regex.search(stream)
      self.index_list_offset = int(m.group(1))

      self.chromatogram_start_index = -1
      self.chromatogram_end_index = -1
      m = self.chromatogram_offset_regex.search(stream)
      if m is not None:
        self.chromatogram_start_index = int(m.group(1))
        index = stream.find(self.chromatogram_list_close_tag)
        self.chromatogram_end_index = start_byte + index

  def __get_metadata__(self):
    self.__get_index_list_offset__()
    self.__get_header_offset__()
    self.header = util.read(self.obj, self.header_start_index, self.header_end_index)
    self.__get_footer_offset__()

  def __get_offsets__(self, start_byte: int, end_byte: int, remainder: str=""):
    stream = util.read(self.obj, start_byte, end_byte)
    stream = remainder + stream
    offset_regex = list(self.offset_regex.finditer(stream))
    offsets = list(map(lambda r: int(r.group(1)), offset_regex))
    if len(offset_regex) > 0:
      regex_offset = offset_regex[-1].span(0)[1]
      stream = stream[regex_offset:]
      remainder = stream

    return [offsets, remainder]

  def __setup__(self):
    self.__get_metadata__()
    self.__spectra_offsets__()
    self.__set_offset_indices__()

  def __set_offset_indices__(self):
    self.offset_start_index = None
    self.offset_end_index = None

    start_byte: int = 0
    if self.offset_bounds:
      start_byte = self.index_list_offset
    else:
      start_byte: int = max(0, self.footer_end_index - self.read_chunk_size)
      self.offset_start_index = self.index_list_offset

    end_byte: int = min(self.footer_end_index, start_byte + self.read_chunk_size)
    remainder: str = ""
    last_match = None
    index: Optional[int] = None
    while start_byte < self.footer_end_index:
      stream: str = util.read(self.obj, start_byte, end_byte)
      stream = remainder + stream
      offset_matches = list(self.offset_regex.finditer(stream))
      index = start_byte - len(remainder)
      for m in offset_matches:
        offset: int = int(m.group(1))
        if self.offset_start_index is None and offset == self.spectra_start_index:
          self.offset_start_index = index + m.span(0)[0]
        if self.offset_end_index is None and int(m.group(1)) > self.spectra_end_index:
          self.offset_end_index = index + m.span(0)[0] - 1

      if len(offset_matches) > 0:
        remainder = stream[offset_matches[-1].span()[1] + 1:]
        last_match = offset_matches[-1]
      else:
        remainder = stream
      start_byte = end_byte + 1
      end_byte = min(self.footer_end_index, start_byte + self.read_chunk_size)

    if self.offset_end_index is None:
      self.offset_end_index = index + last_match.span(0)[1]

  def __spectra_offsets__(self):
    if self.offset_bounds:
      self.spectra_start_index = max(self.offset_bounds.start_index, self.header_end_index + 1)
      self.spectra_end_index = min(self.offset_bounds.end_index, self.footer_start_index - 1)

      start_byte: int = self.footer_start_index
      remainder: str = ""
      included_offsets = []
      before_offset: int = self.header_end_index
      after_offset: int = self.footer_start_index - 1

      # Determine the set of spectra offsets in the offset range
      while start_byte < self.footer_end_index:
        end_byte: int = min(start_byte + self.read_chunk_size, self.footer_end_index)
        [offsets, remainder] = self.__get_offsets__(start_byte, end_byte, remainder)
        for offset in offsets:
          if self.spectra_start_index <= offset and offset <= self.spectra_end_index:
            included_offsets.append(offset)
          elif offset < self.spectra_start_index:
            before_offset = max(before_offset, offset)
          else:
            assert(offset > self.spectra_end_index)
            after_offset = min(after_offset, offset)
        start_byte = end_byte + 1

      if len(included_offsets) == 0:
        self.spectra_start_index = before_offset
      else:
        self.spectra_start_index = included_offsets[0]
      self.spectra_end_index = after_offset - 1
    else:
      self.spectra_start_index = self.header_end_index + 1
      self.spectra_end_index = self.footer_start_index - 1

  @classmethod
  def combine(cls: Any, objs: List[Any], f: BinaryIO) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    iterators = []
    count = 0
    header: str = ""
    for obj in objs:
      iterator = Iterator(obj)
      iterators.append(iterator)
      count += iterator.get_item_count()

    assert(len(iterators) > 0)
    header = iterators[0].header

    content: str = cls.__create_header__(f, header, count, metadata)
    offset = len(content)
    offsets = []
    index = 0
    metadata["spectra_start_index"] = str(offset)

    for iterator in iterators:
      more = True
      while more:
        [spectra, _, more] = iterator.next()
        spectra_content: str = ""
        for xml in spectra:
          xml.set("index", str(index))
          offsets.append((xml.get("id"), offset))
          spectrum = ET.tostring(xml).decode()
          offset += len(spectrum)
          spectra_content += spectrum
          index += 1
        content += cls.__add_content__(spectra_content, f)

    metadata["spectra_end_index"] = str(offset)

    cls.__add_footer__(f, content, offsets, metadata)
    return metadata

  @classmethod
  def from_array(cls: Any, items: List[Any], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    content: str = cls.__create_header__(f, extra["header"], len(items), metadata)
    offset = len(content)
    offsets = []

    count = 0
    for xml in items:
      xml.set("index", str(count))
      m = cls.id_regex.match(xml.get("id"))
      assert(m is not None)
      offsets.append((m.group(1), offset))
      spectrum = ET.tostring(xml).decode()
      offset += len(spectrum)
      content += cls.__add_content__(spectrum, f)
      count += 1

    cls.__add_footer__(f, content, offsets, metadata)
    return metadata

  def get_extra(self) -> Dict[str, Any]:
    return { "header": self.header }

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: Identifiers) -> float:
    value: Optional[float] = None
    if identifier == Identifiers.mass:
      value = cls.__cv_param__(item, "base peak m/z")
    else:
      value = cls.__cv_param__(item, "total ion current")
    assert(value is not None)
    return value

  def get_item_count(self) -> int:
    if self.num_spectra != -1:
      return self.num_spectra

    if "num_spectra" in self.obj.metadata:
      self.num_spectra = int(self.obj.metadata["num_spectra"])
    else:
      stream = util.read(self.obj, 0, self.header_end_index)
      m = self.spectrum_list_count_regex.search(stream)
      assert(m is not None)
      self.num_spectra = int(m.group(1))
    return self.num_spectra

  def get_start_index(self) -> int:
    return self.spectra_start_index

  def get_end_index(self) -> int:
    return self.spectra_end_index

  def get_offset_end_index(self) -> int:
    return self.offset_end_index

  def get_offset_start_index(self) -> int:
    return self.offset_start_index

  @classmethod
  def to_array(cls: Any, content: str) -> Iterable[Any]:
    content = "<data>" + content + "</data>"
    root = ET.fromstring(content)
    return root.iter("spectrum")

  def transform(self, stream: str, offset_bounds: Optional[OffsetBounds]) -> Tuple[str, Optional[OffsetBounds]]:
    start_index: int
    end_index: int
    if not offset_bounds:
      start_index = self.spectra_start_index
      end_index = self.spectra_end_index
    else:
      offsets: List[int] = list(map(lambda r: int(r.group(1)), self.offset_regex.finditer(stream)))
      assert(len(offsets) > 0)
      stream = util.read(self.obj, offset_bounds.end_index, offset_bounds.end_index + self.chunk_size)
      next_offsets: List[int] = list(map(lambda r: int(r.group(1)), self.offset_regex.finditer(stream)))
      offset_bounds.start_index = offsets[0]
      if len(next_offsets) > 0:
        offset_bounds.end_index = next_offsets[0] - 1
      else:
        offset_bounds.end_index = self.spectra_end_index
      start_index = offset_bounds.start_index
      end_index = offset_bounds.end_index
    assert(start_index <= end_index)
    stream = util.read(self.obj, start_index, end_index)
    return (stream, OffsetBounds(start_index, end_index))
