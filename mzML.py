import constants
import hashlib
import re
import iterator
import xml.etree.ElementTree as ET


class Iterator(iterator.Iterator):
  INDEX_LIST_OFFSET_REGEX = re.compile("[\s\S]*<indexListOffset>(\d+)</indexListOffset>")
  OFFSET_REGEX = re.compile("<offset[^>]*>(\d+)</offset>")
  SPECTRUM_LIST_COUNT_REGEX = re.compile('[\s\S]*<spectrumList [\s\S]*count="(\d+)" [\s\S]*>[\s\S]*')
  SPECTRUM_LIST_CLOSE_TAG = "</spectrumList>"
  INDEX_CHUNK_SIZE = 1000
  SPECTRUM_CLOSE_TAG = "</spectrum>"

  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, obj, batch_size, chunk_size)
    ET.register_namespace("", constants.XML_NAMESPACE)
    self.footer_offset = 235
    self.obj = obj
    self.content_length = obj.content_length
    self.remainder = ""
    self.offsets = []
    self.seen_count = 0
    self.findOffsets()

  def getCount(self):
    return self.total_count

  def findOffsets(self):
    end_byte = self.content_length
    start_byte = end_byte - self.footer_offset
    stream = self.getBytes(start_byte, end_byte)
    m = self.INDEX_LIST_OFFSET_REGEX.match(stream)
    assert(m is not None)
    self.spectra_list_offset = int(m.group(1))

    start_byte = self.spectra_list_offset
    end_byte = start_byte + self.INDEX_CHUNK_SIZE
    stream = self.getBytes(start_byte, end_byte)
    self.current_spectra_offset = self.spectra_list_offset + stream.find("<offset")

    self.updateOffsets()
    stream = self.getBytes(min(self.offsets[0] - self.INDEX_CHUNK_SIZE, 0), self.offsets[0])
    m = self.SPECTRUM_LIST_COUNT_REGEX.match(stream)
    assert(m is not None)
    self.total_count = int(m.group(1))

  def updateOffsets(self):
      start_byte = self.current_spectra_offset
      end_byte = min(self.content_length, start_byte + self.chunk_size)
      stream = self.getBytes(start_byte, end_byte)
      stream = self.remainder + stream
      offset_regex = list(self.OFFSET_REGEX.finditer(stream))
      self.offsets += list(map(lambda r: int(r.group(1)), offset_regex))
      regex_offset = offset_regex[-1].span(0)[1]
      self.remainder = stream[regex_offset:]
      self.current_spectra_offset = end_byte + 1

  def nextOffsets(self):
    # Plus one is so we get end byte of spectra
    while len(self.offsets) < (self.batch_size + 1) and self.current_spectra_offset < self.content_length:
      self.updateOffsets()

    start_offset = self.offsets[0]
    if len(self.offsets) > self.batch_size:
      end_offset = self.offsets[self.batch_size] - 1
    else:
      end_offset = self.spectra_list_offset

    self.seen_count += min(len(self.offsets), self.batch_size)
    self.offsets = self.offsets[self.batch_size:]
    return (start_offset, end_offset, self.seen_count < self.total_count)

  def getMass(spectrum):
    for cvParam in spectrum.iter("cvParam"):
      if cvParam.get("name") == "base peak m/z":
        return float(cvParam.get("value"))

  def getSpectra(self, start_byte, end_byte, mass=False):
    content = self.getBytes(start_byte, end_byte)
    index = content.rfind(self.SPECTRUM_LIST_CLOSE_TAG)
    if index != -1:
      content = content[:index - 1]

    root = ET.fromstring("<data>" + content.strip() + "</data>")
    spectra = root.iter("spectrum")

    if mass:
      spectra = list(map(lambda s: (Iterator.getMass(s), s), spectra))
    else:
      spectra = list(spectra)
    return spectra

  def next(self):
    [start_byte, end_byte, more] = self.nextOffsets()
    return [self.getSpectra(start_byte, end_byte), more]

  def create(spectra, sort=False):
    content = open("header.mzML").read()
    offset = len(content)
    offsets = []
    if sort:
      spectra = list(map(lambda s: (Iterator.getMass(s), s), spectra))
      spectra = sorted(spectra, key=lambda s: s[0])
      spectra = list(map(lambda s: s[1], spectra))

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

  def createContent(content):
    index = content.rindex(Iterator.SPECTRUM_CLOSE_TAG)
    content = content[:index + len(Iterator.SPECTRUM_CLOSE_TAG)]
    root = ET.fromstring("<data>" + content + "</data>")
    return str.encode(Iterator.create(list(root.iter("spectrum"))))

  def nextFile(self):
    [spectra, more] = self.next()
    content = Iterator.create(spectra)
    return [content, more]
