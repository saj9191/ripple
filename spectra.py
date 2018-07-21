import constants
import hashlib
import re
import util
import xml.etree.ElementTree as ET


class SpectraIterator:
  def __init__(self, obj, batch_size, chunk_size):
    self.batch_size = batch_size
    self.chunk_size = chunk_size
    self.obj = obj

  def getBytes(self, start_byte, end_byte):
    return self.obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read().decode("utf-8")

  def nextFile(self):
    pass


class ms2SpectraIterator(SpectraIterator):
  def __init__(self, obj, batch_size, chunk_size):
    SpectraIterator.__init__(self, obj, batch_size, chunk_size)
    self.start_byte = 0
    self.num_bytes = obj.content_length
    self.remainder = ""
    self.spectra = []

  def next(self):
    while self.start_byte < self.num_bytes and len(self.spectra) < self.batch_size:
      end_byte = min(self.start_byte + self.chunk_size, self.num_bytes)
      [new_spectra_regex, remainder] = util.get_spectra(self.obj, self.start_byte, end_byte, self.num_bytes, self.remainder)
      self.spectra += list(map(lambda r: r.group(0), new_spectra_regex))
      self.remainder = remainder
      self.start_byte = end_byte + 1

    spectra = self.spectra[:self.batch_size]
    self.spectra = self.spectra[self.batch_size:]
    return spectra, (len(self.spectra) > 0 or self.start_byte < self.num_bytes)

  def nextFile(self):
    [spectra, more] = self.next()
    return "".join(spectra), more


class mzMLSpectraIterator(SpectraIterator):
  INDEX_LIST_OFFSET_REGEX = re.compile("[\s\S]*<indexListOffset>(\d+)</indexListOffset>")
  INDEX_LIST_REGEX = re.compile('[\s\S]*<indexList count="(\d+)">\s*<index name="spectrum">[\s\S*]*')
  OFFSET_REGEX = re.compile("<offset[^>]*>(\d+)</offset>")
  SPECTRUM_LIST_CLOSE = "</spectrumList>"
  HEADER = open("header.mzML").read()
  INDEX_CHUNK_SIZE = 1000

  def __init__(self, obj, batch_size, chunk_size):
    SpectraIterator.__init__(self, obj, batch_size, chunk_size)
    ET.register_namespace("", constants.XML_NAMESPACE)
    self.footer_offset = 235
    self.obj = obj
    self.content_length = obj.content_length
    self.remainder = ""
    self.offsets = []
    self.seen_count = 0
    self.findOffsets()

  def getSpectraCount(self):
    return self.total_count

  def findOffsets(self):
    end_byte = self.content_length
    start_byte = end_byte - self.footer_offset
    stream = self.getBytes(start_byte, end_byte)
    index_list_match = self.INDEX_LIST_OFFSET_REGEX.match(stream)
    assert(index_list_match is not None)

    self.spectra_list_offset = int(index_list_match.group(1))

    start_byte = self.spectra_list_offset
    end_byte = start_byte + self.INDEX_CHUNK_SIZE
    stream = self.getBytes(start_byte, end_byte)
    m = self.INDEX_LIST_REGEX.match(stream)
    self.total_count = 181743  #  int(m.group(1))
    self.current_spectra_offset = self.spectra_list_offset + stream.find("<offset")

  def nextOffsets(self):
    # Plus one is so we get end byte of spectra
    while len(self.offsets) < (self.batch_size + 1) and self.current_spectra_offset < self.content_length:
      start_byte = self.current_spectra_offset
      end_byte = min(self.content_length, start_byte + self.chunk_size)
      stream = self.getBytes(start_byte, end_byte)
      stream = self.remainder + stream
      offset_regex = list(self.OFFSET_REGEX.finditer(stream))
      self.offsets += list(map(lambda r: int(r.group(1)), offset_regex))
      regex_offset = offset_regex[-1].span(0)[1]
      self.remainder = stream[regex_offset:]
      self.current_spectra_offset = end_byte + 1

    start_offset = self.offsets[0]
    if len(self.offsets) > self.batch_size:
      end_offset = self.offsets[self.batch_size] - 1
    else:
      end_offset = self.spectra_list_offset

    self.seen_count += min(len(self.offsets), self.batch_size)
    self.offsets = self.offsets[self.batch_size:]
    return (start_offset, end_offset, self.seen_count < self.total_count)

  def getSpectra(self, start_byte, end_byte):
    content = self.getBytes(start_byte, end_byte)
    index = content.rfind(self.SPECTRUM_LIST_CLOSE)
    if index != -1:
      content = content[:index - 1]

    root = ET.fromstring("<data>" + content.strip() + "</data>")
    spectra = list(root.iter("spectrum"))
    return spectra

  def next(self):
    [start_byte, end_byte, more] = self.nextOffsets()
    return [self.getSpectra(start_byte, end_byte), more]

  def create(spectra):
    content = mzMLSpectraIterator.HEADER
    offset = len(content)
    offsets = []
    for i in range(len(spectra)):
      xml = spectra[i]
      xml.set("index", str(i))
      offsets.append((xml.get("id"), offset))
      spectrum = ET.tostring(xml).decode()
      offset += len(spectrum)
      content += spectrum

    content += "</spectrumList></run></mzML>\n"
    list_offset = len(content)
    content += '<indexList count="{0:d}">\n'.format(len(spectra))
    content += '<index name="spectrum">\n'
    for offset in offsets:
      content += '<offset idRef="controllerType=0 controllerNumber=1 scan={0:s}">{1:d}</offset>\n'.format(offset[0], offset[1])
    content += "</index>\n"
    content += "</indexList>\n"
    content += "<indexListOffset>{0:d}</indexListOffset>\n".format(list_offset)
    content += "<fileChecksum>"

    content += str(hashlib.sha1(content.encode("utf-8")).hexdigest())
    content += "</fileChecksum>\n</indexedmzML>"
    return content

  def nextFile(self):
    [spectra, more] = self.next()
    content = mzMLSpectraIterator.create(spectra)
    return [content, more]
