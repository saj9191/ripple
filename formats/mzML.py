import boto3
import hashlib
import iterator
import re
import util
import xml.etree.ElementTree as ET


def add(content, f):
  if f is not None:
    f.write(content)
  return content


class Iterator(iterator.Iterator):
  CHROMATOGRAM_OFFSET_REGEX = re.compile('<offset idRef="TIC">(\d+)</offset>')
  CHROMATOGRAM_LIST_CLOSE_TAG = "</chromotogramList>"
  INDEX_LIST_OFFSET_REGEX = re.compile("<indexListOffset>(\d+)</indexListOffset>")
  OFFSET_REGEX = re.compile("<offset[^>]*scan=[^>]*>(\d+)</offset>")
  SPECTRUM_LIST_COUNT_REGEX = re.compile('<spectrumList [\s\S]*count="(\d+)"')
  SPECTRUM_LIST_CLOSE_TAG = "</spectrumList>"
  SPECTRUM_CLOSE_TAG = "</spectrum>"
  ID_REGEX = re.compile(".*scan=([0-9]+).*")
  XML_NAMESPACE = "http://psi.hupo.org/ms/mzml"

  def __init__(self, obj, chunk_size, offsets={}, s3=None):
    iterator.Iterator.__init__(self, Iterator, obj, chunk_size)
    ET.register_namespace("", Iterator.XML_NAMESPACE)
    self.footer_offset = 1000
    self.batch_size = 100
    self.remainder = ""
    if s3 is not None:
      self.s3 = s3
    else:
      self.s3 = boto3.resource("s3")
    self.__setup__(offsets)

  def __setup__(self, offsets):
    self.__get_metadata__(offsets)
    self.__spectra_offsets__(offsets)
    self.__set_offset_indices__(offsets)

  def __get_index_list_offset__(self):
    if "index_list_offset" in self.obj.metadata:
      self.index_list_offset = int(self.obj.metadata["index_list_offset"])
      self.chromatogram_start_index = int(self.obj.metadata["chromatogram_start_index"])
      self.chromatogram_end_index = int(self.obj.metadata["chromatogram_end_index"])
    else:
      end_byte = self.content_length
      start_byte = end_byte - self.footer_offset
      stream = util.read(self.obj, start_byte, end_byte)
      m = self.INDEX_LIST_OFFSET_REGEX.search(stream)
      self.index_list_offset = int(m.group(1))

      self.chromatogram_start_index = -1
      self.chromatogram_end_index = -1
      m = self.CHROMATOGRAM_OFFSET_REGEX.search(stream)
      if m is not None:
        self.chromatogram_start_index = int(m.group(1))
        index = stream.find(self.CHROMATOGRAM_LIST_CLOSE_TAG)
        self.chromatogram_end_index = start_byte + index

  def __get_header_offset__(self):
    if "header_start_index" in self.obj.metadata:
      self.header_start_index = int(self.obj.metadata["header_start_index"])
      self.header_end_index = int(self.obj.metadata["header_end_index"])
    else:
      self.header_start_index = 0
      start_byte = max(0, self.index_list_offset - self.footer_offset)
      end_byte = min(self.index_list_offset + self.footer_offset, self.obj.content_length)
      stream = util.read(self.obj, start_byte, end_byte)
      offset_matches = list(self.OFFSET_REGEX.finditer(stream))
      assert(len(offset_matches) > 0)
      self.header_end_index = int(offset_matches[0].group(1)) - 1

  def __get_footer_offset__(self):
    if "footer_start_index" in self.obj.metadata:
      self.footer_start_index = int(self.obj.metadata["footer_start_index"])
      self.footer_end_index = int(self.obj.metadata["footer_end_index"])
    else:
      if self.chromatogram_start_index == -1:
        start_byte = self.index_list_offset - self.footer_offset
      else:
        start_byte = self.chromatogram_start_index - self.footer_offset
      start_byte = max(0, start_byte)
      end_byte = min(start_byte + self.footer_offset, self.obj.content_length)
      stream = util.read(self.obj, start_byte, end_byte)
      self.footer_start_index = start_byte + stream.rindex(self.SPECTRUM_LIST_CLOSE_TAG)
      self.footer_end_index = self.obj.content_length

  def __get_total_count__(self):
    if "count" in self.obj.metadata:
      self.total_count = int(self.obj.metadata["count"])
    else:
      stream = util.read(self.obj, 0, self.header_end_index)
      m = self.SPECTRUM_LIST_COUNT_REGEX.search(stream)
      assert(m is not None)
      self.total_count = int(m.group(1))

  def __get_metadata__(self, offsets):
    bucket = self.obj.bucket_name
    key = self.obj.key
    header_key = util.get_auxilary_key(key, "header")
    self.__get_index_list_offset__()

    if header_key is not None and util.object_exists(self.s3, bucket, header_key):
      self.header_start_index = 0
      header_obj = self.s3.Object(bucket, header_key)
      self.header_end_index = header_obj.content_length - 1  # One char added to object
    else:
      self.__get_header_offset__()
      self.header = util.read(self.obj, self.header_start_index, self.header_end_index)
      if header_key is not None:
        self.s3.Object(bucket, header_key).put(Body=str.encode(self.header))

    self.__get_footer_offset__()
    self.__get_total_count__()

  @classmethod
  def __get_header__(cls, obj):
    bucket = obj.bucket_name
    key = obj.key
    header_key = util.get_auxilary_key(key, "header")
    if header_key is not None:
      s3 = boto3.resource("s3")
      header_obj = s3.Object(bucket, header_key)
      return util.read(header_obj, 0, header_obj.content_length)
    else:
      return None

  def __spectra_offsets__(self, offsets):
    if len(offsets) != 0 and len(offsets["offsets"]) != 0:
      self.spectra_start_index = max(offsets["offsets"][0], self.header_end_index + 1)
      self.spectra_end_index = min(offsets["offsets"][1], self.footer_start_index)

      start_byte = self.footer_start_index
      remainder = ""
      included_offsets = []
      before_offset = self.header_end_index
      after_offset = self.footer_start_index

      # Determine the set of spectra offsets in the offset range
      while start_byte < self.footer_end_index:
        end_byte = min(start_byte + self.chunk_size, self.footer_end_index)
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
        self.spectra_end_index = after_offset
      else:
        self.spectra_start_index = included_offsets[0]
        self.spectra_end_index = after_offset
    else:
      self.spectra_start_index = self.header_end_index + 1
      self.spectra_end_index = self.footer_start_index

  def __get_offsets__(self, start_byte, end_byte, remainder=""):
    stream = util.read(self.obj, start_byte, end_byte)
    stream = remainder + stream
    offset_regex = list(self.OFFSET_REGEX.finditer(stream))
    offsets = list(map(lambda r: int(r.group(1)), offset_regex))
    if len(offset_regex) > 0:
      regex_offset = offset_regex[-1].span(0)[1]
      stream = stream[regex_offset:]
    remainder = stream

    return [offsets, remainder]

  def __set_offset_indices__(self, offsets):
    self.start_index = self.index_list_offset
    self.end_index = self.footer_end_index

    if len(offsets) != 0:
      start_byte = self.index_list_offset
      end_byte = min(self.footer_end_index, start_byte + self.chunk_size)

      while start_byte < self.footer_end_index:
        stream = util.read(self.obj, start_byte, end_byte)
        stream = self.remainder + stream
        offset_matches = list(self.OFFSET_REGEX.finditer(stream))
        for m in offset_matches:
          offset = int(m.group(1))
          if offset == self.spectra_start_index:
            self.start_index = start_byte + m.span(0)[0]
          if offset == self.spectra_end_index:
            self.end_index = start_byte + m.span(0)[0]

        if len(offset_matches) > 0:
          self.remainder = stream[offset_matches[-1].span()[1] + 1:]
        else:
          self.remainder = stream
        start_byte = end_byte + 1
        end_byte = min(self.footer_end_index, start_byte + self.chunk_size)

    self.next_index = self.start_index

  def getCount(self):
    return self.total_count

  def more(self):
    return self.next_index < self.end_index

  def nextOffsets(self):
    if len(self.offsets) == 0 and not self.more():
      return ({"offsets": []}, False)

    # Plus one is so we get end byte of value
    while len(self.offsets) < (self.batch_size + 1) and self.more():
      self.updateOffsets()

    if len(self.offsets) == 0:
      return ({"offsets": []}, False)

    offsets = self.offsets[:self.batch_size]
    self.offsets = self.offsets[self.batch_size:]
    if len(self.offsets) > 0:
      end = min(self.offsets[0], self.spectra_end_index)
    else:
      end = self.spectra_end_index
    o = {"offsets": [offsets[0], end - 1]}

    return (o, len(self.offsets) > 0 or self.more())

  def updateOffsets(self):
    start_byte = self.next_index
    end_byte = min(self.end_index, start_byte + self.chunk_size)
    [offsets, remainder] = self.__get_offsets__(start_byte, end_byte, self.remainder)
    self.remainder = remainder
    self.offsets += offsets
    self.next_index = end_byte + 1

  def getIdentifier(spectrum, identifier):
    if identifier == "mass":
      return Iterator.getMass(spectrum)
    elif identifier == "tic":
      return Iterator.getTIC(spectrum)
    else:
      raise Exception("Unknown identifier", identifier)

  def cvParam(spectrum, name):
    for cvParam in spectrum.iter("cvParam"):
      if cvParam.get("name") == name:
        return float(cvParam.get("value"))

  def getMass(spectrum):
    return Iterator.cvParam(spectrum, "base peak m/z")

  def getTIC(spectrum):
    return Iterator.cvParam(spectrum, "total ion current")

  def get(obj, start_byte, end_byte, identifier):
    content = util.read(obj, start_byte, end_byte).strip()
    index = content.rfind(Iterator.SPECTRUM_CLOSE_TAG)
    if index != -1:
      content = content[:index + len(Iterator.SPECTRUM_CLOSE_TAG)]
      content = content.strip()

    # TODO: Look into this more
    if not content.endswith(Iterator.SPECTRUM_CLOSE_TAG):
      return []
    root = ET.fromstring("<data>" + content.strip() + "</data>")
    spectra = list(root.iter("spectrum"))

    # Filter out MS1
    spectra = list(filter(lambda s: Iterator.cvParam(s, "ms level") == 2.0, spectra))

    if identifier:
      spectra = list(map(lambda s: (Iterator.getIdentifier(s, identifier), s), spectra))
    else:
      spectra = list(spectra)
    return spectra

  def header(obj, start, end, count):
    content = util.read(obj, start, end)
    m = Iterator.SPECTRUM_LIST_COUNT_REGEX.search(content)
    original = m.group(0)
    replacement = original.replace(m.group(1), str(count))
    content = content.replace(original, replacement)
    return content

  def format_offsets(self, offsets):
    return offsets

  @classmethod
  def from_array(cls, obj, spectra, offsets, header=None, f=None):
    metadata = {}
    if header is None:
      header = Iterator.__get_header__(obj)

    header = re.sub(Iterator.SPECTRUM_LIST_COUNT_REGEX, '<spectrumList count="{0:d}"'.format(len(spectra)), header)
    metadata["header_start_index"] = str(0)
    metadata["header_end_index"] = str(len(header))
    metadata["chromatogram_start_index"] = "-1"
    metadata["chromatogram_end_index"] = "-1"

    content = add(header, f)
    offset = len(content)
    offsets = []

    count = 0
    for i in range(len(spectra)):
      xml = spectra[i]
      xml.set("index", str(count))
      m = Iterator.ID_REGEX.match(xml.get("id"))
      offsets.append((m.group(1), offset))
      spectrum = ET.tostring(xml).decode()
      offset += len(spectrum)
      content += add(spectrum, f)
      count += 1

    content += add("</spectrumList></run></mzML>\n", f)
    list_offset = len(content)
    metadata["index_list_offset"] = str(list_offset)
    content += add('<indexList count="1">\n', f)
    content += add('<index name="spectrum">\n', f)
    for offset in offsets:
      content += add('<offset idRef="controllerType=0 controllerNumber=1 scan={0:s}">{1:d}</offset>\n'.format(offset[0], offset[1]), f)
    content += add("</index>\n", f)
    content += add("</indexList>\n", f)
    content += add("<indexListOffset>{0:d}</indexListOffset>\n".format(list_offset), f)
    content += add("<fileChecksum>", f)
    content += add(str(hashlib.sha1(content.encode("utf-8")).hexdigest()), f)
    content += add("</fileChecksum>\n</indexedmzML>", f)
    metadata["footer_start_index"] = str(list_offset)
    metadata["footer_end_index"] = str(len(content))
    metadata["count"] = str(count)
    return content, metadata

  def createContent(content):
    index = content.rindex(Iterator.SPECTRUM_CLOSE_TAG)
    content = content[:index + len(Iterator.SPECTRUM_CLOSE_TAG)]
    root = ET.fromstring("<data>" + content + "</data>")
    return str.encode(Iterator.fromArray(list(root.iter("spectrum"))))

  def endByte(self):
    return self.end_byte

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    iterators = []
    count = 0
    for key in keys:
      obj = params["s3"].Object(bucket_name, key)
      iterator = Iterator(obj, params["chunk_size"], s3=params["s3"])
      iterators.append(iterator)
      count += iterator.getCount()

    metadata = {"count": str(count), "chromatogram_start_index": "-1", "chromatogram_end_index": "-1"}

    with open(temp_name, "w+") as f:
      content = util.read(obj, 0, int(obj.metadata["header_end_index"]))
      f.write(content)
      metadata["header_start_index"] = str(0)
      metadata["header_end_index"] = str(len(content) - 1)
      metadata["spectra_start_index"] = str(len(content))
      offset = len(content)
      offsets = []
      index = 0

      for iterator in iterators:
        more = True
        while more:
          [spectra, more] = iterator.next(identifier=False)
          content = ""
          for i in range(len(spectra)):
            xml = spectra[i]
            xml.set("index", str(index))
            offsets.append((xml.get("id"), offset))
            spectrum = ET.tostring(xml).decode()
            offset += len(spectrum)
            content += spectrum
            index += 1
          f.write(content)

      content = ""
      metadata["spectra_end_index"] = str(offset)
      metadata["footer_start_index"] = str(offset)
      content = "</spectrumList></run></mzML>\n"
      list_offset = len(content) + offset
      content += '<indexList count="1">\n'
      content += '<index name="spectrum">\n'
      for o in offsets:
        content += '<offset idRef="controllerType=0 controllerNumber=1 scan={0:s}">{1:d}</offset>\n'.format(o[0], o[1])
      content += "</index>\n"
      content += "</indexList>\n"
      metadata["index_list_offset"] = str(list_offset)
      content += "<indexListOffset>{0:d}</indexListOffset>\n".format(list_offset)
      content += "<fileChecksum>"
      content += "</fileChecksum>\n</indexedmzML>"
      metadata["footer_end_index"] = str(offset + len(content))
      f.write(content)

    return metadata
