import boto3
import hashlib
import re
import iterator
import xml.etree.ElementTree as ET


class Element():
  def __init__(self, spectra, more, iterator):
    self.spectra = spectra
    self.more = more
    self.iterator = iterator

  def __lt__(self, other):
    return self.spectra[0][0] < other.spectra[0][0]


class Iterator(iterator.Iterator):
  INDEX_LIST_OFFSET_REGEX = re.compile("[\s\S]*<indexListOffset>(\d+)</indexListOffset>")
  OFFSET_REGEX = re.compile("<offset[^>]*>(\d+)</offset>")
  SPECTRUM_LIST_COUNT_REGEX = re.compile('[\s\S]*<spectrumList [\s\S]*count="(\d+)" [\s\S]*>[\s\S]*')
  SPECTRUM_LIST_CLOSE_TAG = "</spectrumList>"
  INDEX_CHUNK_SIZE = 1000
  SPECTRUM_CLOSE_TAG = "</spectrum>"
  XML_NAMESPACE = "http://psi.hupo.org/ms/mzml"

  def __init__(self, obj, batch_size, chunk_size):
    iterator.Iterator.__init__(self, Iterator, obj, batch_size, chunk_size)
    ET.register_namespace("", Iterator.XML_NAMESPACE)
    self.footer_offset = 235
    self.remainder = ""
    self.findOffsets()

  def getCount(self):
    return self.total_count

  def findOffsets(self):
    end_byte = self.content_length
    start_byte = end_byte - self.footer_offset
    stream = iterator.Iterator.getBytes(self.obj, start_byte, end_byte)
    m = self.INDEX_LIST_OFFSET_REGEX.match(stream)
    assert(m is not None)
    self.spectra_list_offset = int(m.group(1))

    start_byte = self.spectra_list_offset
    end_byte = start_byte + self.INDEX_CHUNK_SIZE
    stream = iterator.Iterator.getBytes(self.obj, start_byte, end_byte)
    self.spectra_list_offset += stream.find("<offset")
    self.current_offset = self.spectra_list_offset

    start_byte = self.spectra_list_offset - self.INDEX_CHUNK_SIZE
    end_byte = self.spectra_list_offset
    stream = iterator.Iterator.getBytes(self.obj, start_byte, end_byte)

    index = stream.rfind(Iterator.SPECTRUM_CLOSE_TAG)
    self.end_byte = start_byte + index + len(Iterator.SPECTRUM_CLOSE_TAG) - 1

    self.updateOffsets()
    if len(self.offsets) == 0:
      self.total_count = 0
    else:
      end_byte = self.offsets[0]
      start_byte = min(end_byte - self.INDEX_CHUNK_SIZE, 0)
      stream = Iterator.getBytes(self.obj, start_byte, end_byte)
      m = self.SPECTRUM_LIST_COUNT_REGEX.match(stream)
      assert(m is not None)
      self.total_count = int(m.group(1))

  def updateOffsets(self):
    start_byte = self.current_offset
    end_byte = min(self.content_length, start_byte + self.chunk_size)
    stream = Iterator.getBytes(self.obj, start_byte, end_byte)
    stream = self.remainder + stream
    offset_regex = list(self.OFFSET_REGEX.finditer(stream))
    self.offsets += list(map(lambda r: int(r.group(1)), offset_regex))
    if len(offset_regex) > 0:
      regex_offset = offset_regex[-1].span(0)[1]
      stream = stream[regex_offset:]
      self.remainder = stream
    else:
      self.remainder = ""
    self.current_offset = end_byte + 1

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
    content = Iterator.getBytes(obj, start_byte, end_byte)
    index = content.rfind(Iterator.SPECTRUM_LIST_CLOSE_TAG)
    if index != -1:
      content = content[:index - 1]

    root = ET.fromstring("<data>" + content.strip() + "</data>")
    spectra = root.iter("spectrum")

    # Filter out MS1
    spectra = list(filter(lambda s: Iterator.cvParam(s, "ms level") == 2.0, spectra))

    if identifier:
      spectra = list(map(lambda s: (Iterator.getIdentifier(s, identifier), s), spectra))
    else:
      spectra = list(spectra)
    return spectra

  def fromArray(spectra, includeHeader=False):
    content = open("header.mzML").read()
    content = content.replace("-123456789", str(len(spectra)))
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
    content += '<indexList count="2">\n'
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
    return str.encode(Iterator.fromArray(list(root.iter("spectrum"))))

  def nextFile(self):
    [spectra, more] = self.next()
    content = Iterator.fromArray(spectra)
    return [content, more]

  def endByte(self):
    return self.end_byte

  @classmethod
  def combine(cls, bucket_name, keys, temp_name, params):
    iterators = []
    count = 0
    s3 = boto3.resource("s3")
    for key in keys:
      obj = s3.Object(bucket_name, key)
      iterator = Iterator(obj, params["batch_size"], params["chunk_size"])
      iterators.append(iterator)
      count += iterator.getCount()

    spectra = []
    for iterator in iterators:
      more = True
      while more:
        print(iterator.obj)
        [s, more] = iterator.next(identifier=False)
        spectra += s

    content = open("header.mzML").read()
    content = content.replace("-123456789", str(count))
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
    content += '<indexList count="2">\n'
    content += '<index name="spectrum">\n'
    for offset in offsets:
      content += '<offset idRef="controllerType=0 controllerNumber=1 scan={0:s}">{1:d}</offset>\n'.format(offset[0], offset[1])
    content += "</index>\n"
    content += "</indexList>\n"
    content += "<indexListOffset>{0:d}</indexListOffset>\n".format(list_offset)
    content += "<fileChecksum>"

    content += str(hashlib.sha1(content.encode("utf-8")).hexdigest())
    content += "</fileChecksum>\n</indexedmzML>"
    with open(temp_name, "w+") as f:
      f.write(content)
