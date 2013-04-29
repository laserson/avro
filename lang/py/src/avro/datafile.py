# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Read/Write Avro File Object Containers.
"""
import zlib
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
from avro import schema
from avro import io
try:
  import snappy
  has_snappy = True
except ImportError:
  has_snappy = False
#
# Constants
#

VERSION = 1
MAGIC = 'Obj' + chr(VERSION)
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
SYNC_INTERVAL = 1000 * SYNC_SIZE # TODO(hammer): make configurable
META_SCHEMA = schema.parse("""\
{"type": "record", "name": "org.apache.avro.file.Header",
 "fields" : [
   {"name": "magic", "type": {"type": "fixed", "name": "magic", "size": %d}},
   {"name": "meta", "type": {"type": "map", "values": "bytes"}},
   {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": %d}}]}
""" % (MAGIC_SIZE, SYNC_SIZE))
VALID_CODECS = ['null', 'deflate']
if has_snappy:
    VALID_CODECS.append('snappy')
VALID_ENCODINGS = ['binary'] # not used yet

CODEC_KEY = "avro.codec"
SCHEMA_KEY = "avro.schema"

#
# Exceptions
#

class DataFileException(schema.AvroException):
  """
  Raised when there's a problem reading or writing file object containers.
  """
  def __init__(self, fail_msg):
    schema.AvroException.__init__(self, fail_msg)

#
# Write Path
#

class DataFileWriter(object):
  @staticmethod
  def generate_sync_marker():
    return generate_sixteen_random_bytes()

  # TODO(hammer): make 'encoder' a metadata property
  def __init__(self, writer, datum_writer, writers_schema=None, codec='null'):
    """
    If the schema is not present, presume we're appending.

    @param writer: File-like object to write into.
    """
    self._writer = writer
    self._datum_writer = datum_writer
    self._buffer_writer = StringIO()
    self._block_count = 0
    self._meta = {}
    self._header_written = False

    if writers_schema is not None:
      if codec not in VALID_CODECS:
        raise DataFileException("Unknown codec: %r" % codec)
      self._sync_marker = DataFileWriter.generate_sync_marker()
      self.set_meta('avro.codec', codec)
      self.set_meta('avro.schema', str(writers_schema))
      self.datum_writer.writers_schema = writers_schema
    else:
      # open writer for reading to collect metadata
      dfr = DataFileReader(writer)
      
      # TODO(hammer): collect arbitrary metadata
      # collect metadata
      self._sync_marker = dfr.sync_marker
      self.set_meta('avro.codec', dfr.get_meta('avro.codec'))

      # get schema used to write existing file
      schema_from_file = dfr.get_meta('avro.schema')
      self.set_meta('avro.schema', schema_from_file)
      self.datum_writer.writers_schema = schema.parse(schema_from_file)

      # seek to the end of the file and prepare for writing
      writer.seek(0, 2)
      self._header_written = True

  # read-only properties
  writer = property(lambda self: self._writer)
  datum_writer = property(lambda self: self._datum_writer)
  buffer_writer = property(lambda self: self._buffer_writer)
  sync_marker = property(lambda self: self._sync_marker)
  meta = property(lambda self: self._meta)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  # read/write properties
  def set_block_count(self, new_val):
    self._block_count = new_val
  block_count = property(lambda self: self._block_count, set_block_count)

  # utility functions to read/write metadata entries
  def get_meta(self, key):
    return self._meta.get(key)
  def set_meta(self, key, val):
    self._meta[key] = val

  def _write_header(self):
    header = {'magic': MAGIC,
              'meta': self.meta,
              'sync': self.sync_marker}
    self.datum_writer.write_data(META_SCHEMA, header, self.writer)
    self._header_written = True

  # TODO(hammer): make a schema for blocks and use datum_writer
  def _write_block(self):
    if not self._header_written:
      self._write_header()

    if self.block_count > 0:
      # write number of items in block
      io.write_long(self.writer, self.block_count)

      # write block contents
      uncompressed_data = self.buffer_writer.getvalue()
      if self.get_meta(CODEC_KEY) == 'null':
        compressed_data = uncompressed_data
        compressed_data_length = len(compressed_data)
      elif self.get_meta(CODEC_KEY) == 'deflate':
        # The first two characters and last character are zlib
        # wrappers around deflate data.
        compressed_data = zlib.compress(uncompressed_data)[2:-1]
        compressed_data_length = len(compressed_data)
      elif self.get_meta(CODEC_KEY) == 'snappy':
        compressed_data = snappy.compress(uncompressed_data)
        compressed_data_length = len(compressed_data) + 4 # crc32
      else:
        fail_msg = '"%s" codec is not supported.' % self.get_meta(CODEC_KEY)
        raise DataFileException(fail_msg)

      # Write length of block
      io.write_long(self.writer, compressed_data_length)

      # Write block
      self.writer.write(compressed_data)
      
      # Write CRC32 checksum for Snappy
      if self.get_meta(CODEC_KEY) == 'snappy':
	io.write_crc32(self.writer, uncompressed_data)

      # write sync marker
      self.writer.write(self.sync_marker)

      # reset buffer
      self.buffer_writer.truncate(0) 
      self.block_count = 0

  def append(self, datum):
    """Append a datum to the file."""
    self.datum_writer.write(datum, self.buffer_writer)
    self.block_count += 1

    # if the data to write is larger than the sync interval, write the block
    if self.buffer_writer.tell() >= SYNC_INTERVAL:
      self._write_block()

  def sync(self):
    """
    Return the current position as a value that may be passed to
    DataFileReader.seek(long). Forces the end of the current block,
    emitting a synchronization marker.
    """
    self._write_block()
    return self.writer.tell()

  def flush(self):
    """Flush the current state of the file, including metadata."""
    self._write_block()
    self.writer.flush()

  def close(self):
    """Close the file."""
    self.flush()
    self.writer.close()

class DataFileReader(object):
  """Read files written by DataFileWriter."""
  # TODO(hammer): allow user to specify expected schema?
  # TODO(hammer): allow user to specify the encoder
  def __init__(self, reader, readers_schema=None):
    self._reader = reader
    self._readers_schema = readers_schema
    self._avro_bytes = reader  # may be reset every block (compression)
    
    # read the header: magic, meta, sync
    self._read_header()

    # ensure codec is valid
    self.codec = self.get_meta('avro.codec')
    if self.codec is None:
      self.codec = "null"
    if self.codec not in VALID_CODECS:
      raise DataFileException('Unknown codec: %s.' % self.codec)

    # get file length
    self._file_length = self.determine_file_length()

    # get ready to read
    self._block_count = 0
    self.writers_schema = schema.parse(self.get_meta(SCHEMA_KEY))
    if self.readers_schema is None:
      self.readers_schema = self.writers_schema
    self.schema_helper = {}
    io.resolve_schemas(self.schema_helper, self.writers_schema, self.readers_schema)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  def __iter__(self):
    return self

  # read-only properties
  reader = property(lambda self: self._reader)
  avro_bytes = property(lambda self: self._avro_bytes)
  sync_marker = property(lambda self: self._sync_marker)
  meta = property(lambda self: self._meta)
  file_length = property(lambda self: self._file_length)

  # read/write properties
  def set_readers_schema(self, new_val):
    self._readers_schema = new_val
  readers_schema = property(lambda self: self._readers_schema, set_readers_schema)
  def set_block_count(self, new_val):
    self._block_count = new_val
  block_count = property(lambda self: self._block_count, set_block_count)

  # utility functions to read/write metadata entries
  def get_meta(self, key):
    return self._meta.get(key)
  def set_meta(self, key, val):
    self._meta[key] = val

  def determine_file_length(self):
    """
    Get file length and leave file cursor where we found it.
    """
    remember_pos = self.reader.tell()
    self.reader.seek(0, 2)
    file_length = self.reader.tell()
    self.reader.seek(remember_pos)
    return file_length

  def is_EOF(self):
    return self.reader.tell() == self.file_length

  def _read_header(self):
    # seek to the beginning of the file to get magic block
    self.reader.seek(0, 0) 

    # read header into a dict
    schema_helper = {}
    io.resolve_schemas(schema_helper, META_SCHEMA, META_SCHEMA)
    header = io.read_data(self.reader, schema_helper, META_SCHEMA, META_SCHEMA)

    # check magic number
    if header.get('magic') != MAGIC:
      fail_msg = "Not an Avro data file: %s doesn't match %s."\
                 % (header.get('magic'), MAGIC)
      raise schema.AvroException(fail_msg)

    # set metadata
    self._meta = header['meta']

    # set sync marker
    self._sync_marker = header['sync']

  def _read_block_header(self):
    self.block_count = io.read_long(self.reader)
    if self.codec == "null":
      # Skip a long; we don't need to use the length.
      io.skip_long(self.reader)
      self._avro_bytes = self._reader
    elif self.codec == 'deflate':
      # Compressed data is stored as (length, data), which
      # corresponds to how the "bytes" type is encoded.
      data = io.read_bytes(self.reader)
      # -15 is the log of the window size; negative indicates
      # "raw" (no zlib headers) decompression.  See zlib.h.
      uncompressed = zlib.decompress(data, -15)
      self._avro_bytes = StringIO(uncompressed)
    elif self.codec == 'snappy':
      # Compressed data includes a 4-byte CRC32 checksum
      length = io.read_long(self.reader)
      data = self.reader.read(length - 4)
      uncompressed = snappy.decompress(data)
      self._avro_bytes = StringIO(uncompressed)
      io.check_crc32(self._reader, uncompressed);
    else:
      raise DataFileException("Unknown codec: %r" % self.codec)

  def _skip_sync(self):
    """
    Read the length of the sync marker; if it matches the sync marker,
    return True. Otherwise, seek back to where we started and return False.
    """
    proposed_sync_marker = self.reader.read(SYNC_SIZE)
    if proposed_sync_marker != self.sync_marker:
      self.reader.seek(-SYNC_SIZE, 1)
      return False
    else:
      return True

  # TODO(hammer): handle block of length zero
  # TODO(hammer): clean this up with recursion
  def next(self):
    """Return the next datum in the file."""
    if self.block_count == 0:
      if self.is_EOF():
        raise StopIteration
      elif self._skip_sync():
        if self.is_EOF(): raise StopIteration
        self._read_block_header()
      else:
        self._read_block_header()

    datum = io.read_data(self.avro_bytes, self.schema_helper,
			 self.writers_schema, self.readers_schema)

    self.block_count -= 1
    return datum

  def close(self):
    """Close this reader."""
    self.reader.close()

def generate_sixteen_random_bytes():
  try:
    import os
    return os.urandom(16)
  except:
    import random
    return [ chr(random.randrange(256)) for i in range(16) ]
