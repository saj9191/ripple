import inspect
import os
import sys
import unittest
from tutils import S3, Bucket, Object

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import mzML

INPUT = """<?xml version="1.0" encoding="utf-8"?>
<indexedmzML>
  <mzML>
    <run id="run_id">
      <spectrumList count="3">
        <spectrum id="controllerType=0 controllerNumber=1 scan=1" index="0">
          <cvParam name="ms level" value="2"/>
        </spectrum>
        <spectrum id="controllerType=0 controllerNumber=1 scan=2" index="1">
          <cvParam name="ms level" value="2"/>
          <cvParam />
        </spectrum>
        <spectrum id="controllerType=0 controllerNumber=1 scan=3" index="2">
          <cvParam name="ms level" value="2"/>
          <cvParam />
          <cvParam />
        </spectrum>
      </spectrumList>
    </run>
  </mzML>
  <indexList count="2">
    <index name="spectrum">
      <offset idRef="controllerType=0 controllerNumber=1 scan=1">123</offset>
      <offset idRef="controllerType=0 controllerNumber=1 scan=2">267</offset>
      <offset idRef="controllerType=0 controllerNumber=1 scan=3">434</offset>
    </index>
  </indexList>
  <indexListOffset>659</indexListOffset>
</indexedmzML>
"""

bucket = Bucket("bucket", [])
s3 = S3([bucket])

class IteratorMethods(unittest.TestCase):
  def test_next_offsets(self):
    obj = Object("0/123.4-13/1/1-1-1-test.mzML", INPUT)
    it = mzML.Iterator(obj, 200, s3=s3)
    [offsets, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(offsets["offsets"][0], 123)
    self.assertEqual(offsets["offsets"][1], 647)

  def test_next(self):
    obj = Object("0/123.4-13/1/1-1-1-test.mzML", INPUT)
    it = mzML.Iterator(obj, 200, s3=s3)
    [spectra, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(len(spectra), 3)
    self.assertEqual(spectra[0].get("id"), "controllerType=0 controllerNumber=1 scan=1")
    self.assertEqual(spectra[1].get("id"), "controllerType=0 controllerNumber=1 scan=2")
    self.assertEqual(spectra[2].get("id"), "controllerType=0 controllerNumber=1 scan=3")

  def test_adjust(self):
    obj = Object("0/123.4-13/1/1-1-1-test.mzML", INPUT)

    # Multiple spectra start in range
    offsets = {"offsets": [120, 440]}
    it = mzML.Iterator(obj, 200, offsets, s3=s3)
    [o, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(o["offsets"][0], 123)
    self.assertEqual(o["offsets"][1], 433)

    # One spectra starts in range
    offsets = {"offsets": [120, 250]}
    it = mzML.Iterator(obj, 200, offsets, s3=s3)
    [o, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(o["offsets"][0], 123)
    self.assertEqual(o["offsets"][1], 266)

    # No spectra start in range
    offsets = {"offsets": [126, 240]}
    it = mzML.Iterator(obj, 200, offsets, s3=s3)
    [o, more] = it.nextOffsets()
    self.assertFalse(more)
    self.assertEqual(o["offsets"][0], 123)
    self.assertEqual(o["offsets"][1], 266)


if __name__ == "__main__":
  unittest.main()
