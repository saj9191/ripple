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
  INDEX_LIST_REGEX = re.compile("[\s\S]*<indexListOffset>(\d+)</indexListOffset>")
  OFFSET_REGEX = re.compile("<offset[^>]*>(\d+)</offset>")
  SPECTRUM_LIST_CLOSE = "</spectrumList>"
  HEADER = open("header.mzML").read()

  def __init__(self, obj, batch_size, chunk_size):
    SpectraIterator.__init__(self, obj, batch_size, chunk_size)
    ET.register_namespace("", constants.XML_NAMESPACE)
    self.footer_offset = 235
    self.obj = obj
    self.content_length = obj.content_length
    self.findOffsets()
    self.remainder = ""
    self.offset_regex = []

  def findOffsets(self):
    end_byte = self.content_length
    start_byte = end_byte - self.footer_offset
    stream = self.getBytes(start_byte, end_byte)
    index_list_match = self.INDEX_LIST_REGEX.match(stream)
    assert(index_list_match is not None)

    self.spectra_list_offset = int(index_list_match.group(1))

    start_byte = self.spectra_list_offset
    end_byte = start_byte + self.chunk_size
    stream = self.getBytes(start_byte, end_byte)
    self.current_spectra_offset = self.spectra_list_offset + stream.find("<offset")

  def next(self):
    if self.current_spectra_offset >= self.content_length:
      return ["", False]

    # TODO: Could make fancier and say start_byte < footer_start_index
    # Plus one is so we get the final index
    while len(self.offset_regex) < (self.batch_size + 1) and self.current_spectra_offset < self.content_length:
      start_byte = self.current_spectra_offset
      end_byte = min(self.content_length, start_byte + self.chunk_size)

      stream = self.getBytes(start_byte, end_byte)
      stream = self.remainder + stream
      self.offset_regex += list(self.OFFSET_REGEX.finditer(stream))
      regex_offset = self.offset_regex[-1].span(0)[1]
      self.current_spectra_offset = end_byte + 1
      self.remainder = stream[regex_offset:]
    if len(self.offset_regex) == 0:
      return ["", False]

    start_byte = int(self.offset_regex[0].group(1))
    if len(self.offset_regex) > self.batch_size:
      end_byte = int(self.offset_regex[self.batch_size].group(1)) - 1
    else:
      end_byte = self.content_length
    self.offset_regex = self.offset_regex[self.batch_size:]
    print("start", start_byte, "end", end_byte, "length", self.content_length)
    content = self.getBytes(start_byte, end_byte)
    index = content.rfind(self.SPECTRUM_LIST_CLOSE)
    if index != -1:
      content = content[:index - 1]

    root = ET.fromstring("<data>" + content.strip() + "</data>")
    spectra = list(root.iter("spectrum"))
    return [spectra, len(self.offset_regex) > 0 or self.current_spectra_offset < self.content_length]

  def create(spectra):
    content = mzMLSpectraIterator.HEADER
    offset = len(content)
    offsets = []
    count = 0
    for i in range(len(spectra)):
      xml = spectra[i]
      xml.set("index", str(i))
      offsets.append((xml.get("id"), offset))

      spectrum = ET.tostring(xml).decode()
      offset += len(spectrum)
      content += spectrum
      count += 1

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
