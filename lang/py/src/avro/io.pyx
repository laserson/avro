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
Input/Output utilities, including:

 * i/o-specific constants
 * i/o-specific exceptions
 * schema validation
 * leaf value encoding and decoding
 * datum reader/writer stuff (?)

Also includes a generic representation for data, which
uses the following mapping:

  * Schema records are implemented as dict.
  * Schema arrays are implemented as list.
  * Schema maps are implemented as dict.
  * Schema strings are implemented as unicode.
  * Schema bytes are implemented as str.
  * Schema ints are implemented as int.
  * Schema longs are implemented as long.
  * Schema floats are implemented as float.
  * Schema doubles are implemented as float.
  * Schema booleans are implemented as bool.
"""
import sys
import struct
from binascii import crc32

try:
  import json
except ImportError:
  import simplejson as json

from avro import schema

PRIMITIVE_TYPES = set(schema.PRIMITIVE_TYPES)

#
# Exceptions
#

class AvroTypeException(schema.AvroException):
  """Raised when datum is not an example of schema."""
  def __init__(self, expected_schema, datum):
    pretty_expected = json.dumps(json.loads(str(expected_schema)), indent=2)
    fail_msg = "The datum %s is not an example of the schema %s"\
	       % (datum, pretty_expected)
    schema.AvroException.__init__(self, fail_msg)

class SchemaResolutionException(schema.AvroException):
  def __init__(self, fail_msg, writers_schema=None, readers_schema=None):
    pretty_writers = json.dumps(json.loads(str(writers_schema)), indent=2)
    pretty_readers = json.dumps(json.loads(str(readers_schema)), indent=2)
    if writers_schema: fail_msg += "\nWriter's Schema: %s" % pretty_writers
    if readers_schema: fail_msg += "\nReader's Schema: %s" % pretty_readers
    schema.AvroException.__init__(self, fail_msg)


#
# Binary decoder functions
#

# TODO(hammer): shouldn't ! be < for little-endian (according to spec?)
if sys.version_info >= (2, 5, 0):
  struct_class = struct.Struct
else:
  class SimpleStruct(object):
    def __init__(self, format):
      self.format = format
    def pack(self, *args):
      return struct.pack(self.format, *args)
    def unpack(self, *args):
      return struct.unpack(self.format, *args)
  struct_class = SimpleStruct

STRUCT_INT = struct_class('!I')     # big-endian unsigned int
STRUCT_LONG = struct_class('!Q')    # big-endian unsigned long long
STRUCT_FLOAT = struct_class('!f')   # big-endian float
STRUCT_DOUBLE = struct_class('!d')  # big-endian double
STRUCT_CRC32 = struct_class('>I')   # big-endian unsigned int

# read() is not defined because it's trivial: stream.read(n)

def skip(stream, n):
  stream.seek(stream.tell() + n)

def read_null(stream):
  """
  null is written as zero bytes
  """
  return None

def skip_null(stream):
  pass

def read_boolean(stream):
  """
  a boolean is written as a single byte
  whose value is either 0 (false) or 1 (true).
  """
  return ord(stream.read(1)) == 1

def skip_boolean(stream):
  skip(stream, 1)

def read_int(stream):
  """
  int and long values are written using variable-length, zig-zag coding.
  """
  return read_long(stream)

def skip_int(stream):
  skip_long(stream)

def read_long(stream):
  """
  int and long values are written using variable-length, zig-zag coding.
  """
  b = ord(stream.read(1))
  n = b & 0x7F
  shift = 7
  while (b & 0x80) != 0:
    b = ord(stream.read(1))
    n |= (b & 0x7F) << shift
    shift += 7
  datum = (n >> 1) ^ -(n & 1)
  return datum

def skip_long(stream):
  b = ord(stream.read(1))
  while (b & 0x80) != 0:
    b = ord(stream.read(1))

def read_float(stream):
  """
  A float is written as 4 bytes.
  The float is converted into a 32-bit integer using a method equivalent to
  Java's floatToIntBits and then encoded in little-endian format.
  """
  bits = (((ord(stream.read(1)) & 0xffL)) |
    ((ord(stream.read(1)) & 0xffL) <<  8) |
    ((ord(stream.read(1)) & 0xffL) << 16) |
    ((ord(stream.read(1)) & 0xffL) << 24))
  return STRUCT_FLOAT.unpack(STRUCT_INT.pack(bits))[0]

def skip_float(stream):
  skip(stream, 4)

def read_double(stream):
  """
  A double is written as 8 bytes.
  The double is converted into a 64-bit integer using a method equivalent to
  Java's doubleToLongBits and then encoded in little-endian format.
  """
  bits = (((ord(stream.read(1)) & 0xffL)) |
    ((ord(stream.read(1)) & 0xffL) <<  8) |
    ((ord(stream.read(1)) & 0xffL) << 16) |
    ((ord(stream.read(1)) & 0xffL) << 24) |
    ((ord(stream.read(1)) & 0xffL) << 32) |
    ((ord(stream.read(1)) & 0xffL) << 40) |
    ((ord(stream.read(1)) & 0xffL) << 48) |
    ((ord(stream.read(1)) & 0xffL) << 56))
  return STRUCT_DOUBLE.unpack(STRUCT_LONG.pack(bits))[0]

def skip_double(stream):
  skip(stream, 8)

def read_bytes(stream):
  """
  Bytes are encoded as a long followed by that many bytes of data.
  """
  return stream.read(read_long(stream))

def skip_bytes(stream):
  skip(stream, read_long(stream))

def read_utf8(stream):
  """
  A string is encoded as a long followed by
  that many bytes of UTF-8 encoded character data.
  """
  return unicode(read_bytes(stream), "utf-8")

def skip_utf8(stream):
  skip_bytes(stream)

def check_crc32(stream, bytes):
  checksum = STRUCT_CRC32.unpack(stream.read(4))[0]
  if crc32(bytes) & 0xffffffff != checksum:
    raise schema.AvroException("Checksum failure")


#
# Binary encoder functions
#

def write_null(stream, datum):
  """
  null is written as zero bytes
  """
  pass

def write_boolean(stream, datum):
  """
  a boolean is written as a single byte
  whose value is either 0 (false) or 1 (true).
  """
  if datum:
    stream.write(chr(1))
  else:
    stream.write(chr(0))

def write_int(stream, datum):
  """
  int and long values are written using variable-length, zig-zag coding.
  """
  write_long(stream, datum)

def write_long(stream, datum):
  """
  int and long values are written using variable-length, zig-zag coding.
  """
  datum = (datum << 1) ^ (datum >> 63)
  while (datum & ~0x7F) != 0:
    stream.write(chr((datum & 0x7f) | 0x80))
    datum >>= 7
  stream.write(chr(datum))

def write_float(stream, datum):
  """
  A float is written as 4 bytes.
  The float is converted into a 32-bit integer using a method equivalent to
  Java's floatToIntBits and then encoded in little-endian format.
  """
  bits = STRUCT_INT.unpack(STRUCT_FLOAT.pack(datum))[0]
  stream.write(chr((bits) & 0xFF))
  stream.write(chr((bits >> 8) & 0xFF))
  stream.write(chr((bits >> 16) & 0xFF))
  stream.write(chr((bits >> 24) & 0xFF))

def write_double(stream, datum):
  """
  A double is written as 8 bytes.
  The double is converted into a 64-bit integer using a method equivalent to
  Java's doubleToLongBits and then encoded in little-endian format.
  """
  bits = STRUCT_LONG.unpack(STRUCT_DOUBLE.pack(datum))[0]
  stream.write(chr((bits) & 0xFF))
  stream.write(chr((bits >> 8) & 0xFF))
  stream.write(chr((bits >> 16) & 0xFF))
  stream.write(chr((bits >> 24) & 0xFF))
  stream.write(chr((bits >> 32) & 0xFF))
  stream.write(chr((bits >> 40) & 0xFF))
  stream.write(chr((bits >> 48) & 0xFF))
  stream.write(chr((bits >> 56) & 0xFF))

def write_bytes(stream, datum):
  """
  Bytes are encoded as a long followed by that many bytes of data.
  """
  write_long(stream, len(datum))
  stream.write(struct.pack('%ds' % len(datum), datum))

def write_utf8(stream, datum):
  """
  A string is encoded as a long followed by
  that many bytes of UTF-8 encoded character data.
  """
  datum = datum.encode("utf-8")
  write_bytes(stream, datum)

def write_crc32(stream, bytes):
  """
  A 4-byte, big-endian CRC32 checksum
  """
  stream.write(STRUCT_CRC32.pack(crc32(bytes) & 0xffffffff))


#
# Read complex types
#

def read_fixed(stream, resolver_cache, writers_schema, readers_schema):
  """
  Fixed instances are encoded using the number of bytes declared
  in the schema.
  """
  return stream.read(writers_schema.size)

def skip_fixed(stream, resolver_cache, writers_schema):
  return skip(stream, writers_schema.size)

def read_enum(stream, resolver_cache, writers_schema, readers_schema):
  """
  An enum is encoded by a int, representing the zero-based position
  of the symbol in the schema.
  """
  key = (writers_schema.fingerprint, readers_schema.fingerprint)
  # read data
  index_of_symbol = read_int(stream)
  if index_of_symbol >= resolver_cache[key]['len']:
    fail_msg = "Can't access enum index %d for enum with %d symbols"\
	       % (index_of_symbol, resolver_cache[key]['len'])
    raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
  read_symbol = writers_schema.symbols[index_of_symbol]

  # schema resolution
  if read_symbol not in resolver_cache[key]['set']:
    fail_msg = "Symbol %s not present in Reader's Schema" % read_symbol
    raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

  return read_symbol

def skip_enum(stream):
  return skip_int(stream)

def read_array(stream, resolver_cache, writers_schema, readers_schema):
  """
  Arrays are encoded as a series of blocks.

  Each block consists of a long count value,
  followed by that many array items.
  A block with count zero indicates the end of the array.
  Each item is encoded per the array's item schema.

  If a block's count is negative,
  then the count is followed immediately by a long block size,
  indicating the number of bytes in the block.
  The actual count in this case
  is the absolute value of the count written.
  """
  read_items = []
  block_count = read_long(stream)
  while block_count != 0:
    if block_count < 0:
      block_count = -block_count
      block_size = read_long(stream)
    for i in xrange(block_count):
      read_items.append(read_data(stream, resolver_cache, writers_schema.items,
				  readers_schema.items))
    block_count = read_long(stream)
  return read_items

def skip_array(stream, resolver_cache, writers_schema):
  block_count = read_long(stream)
  while block_count != 0:
    if block_count < 0:
      block_size = read_long(stream)
      skip(stream, block_size)
    else:
      for i in xrange(block_count):
	skip_data(stream, writers_schema.items)
    block_count = read_long(stream)

def read_map(stream, resolver_cache, writers_schema, readers_schema):
  """
  Maps are encoded as a series of blocks.

  Each block consists of a long count value,
  followed by that many key/value pairs.
  A block with count zero indicates the end of the map.
  Each item is encoded per the map's value schema.

  If a block's count is negative,
  then the count is followed immediately by a long block size,
  indicating the number of bytes in the block.
  The actual count in this case
  is the absolute value of the count written.
  """
  read_items = {}
  block_count = read_long(stream)
  while block_count != 0:
    if block_count < 0:
      block_count = -block_count
      block_size = read_long(stream)
    for i in xrange(block_count):
      key = read_utf8(stream)
      read_items[key] = read_data(stream, resolver_cache, writers_schema.values,
				  readers_schema.values)
    block_count = read_long(stream)
  return read_items

def skip_map(stream, resolver_cache, writers_schema, readers_schema):
  block_count = read_long(stream)
  while block_count != 0:
    if block_count < 0:
      block_size = read_long(stream)
      skip(stream, block_size)
    else:
      for i in xrange(block_count):
	skip_utf8(stream)
	skip_data(stream, resolver_cache, writers_schema.values, readers_schema.values)
    block_count = read_long(stream)

def read_union(stream, resolver_cache, writers_schema, readers_schema):
  """
  A union is encoded by first writing a long value indicating
  the zero-based position within the union of the schema of its value.
  The value is then encoded per the indicated schema within the union.
  """
  key = (writers_schema.fingerprint, readers_schema.fingerprint)
  # schema resolution
  cdef int index_of_schema = int(read_long(stream))
  if index_of_schema >= resolver_cache[key]['len']:
    fail_msg = "Can't access branch index %d for union with %d branches"\
	       % (index_of_schema, len(writers_schema.schemas))
    raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
  selected_writers_schema = writers_schema.schemas[index_of_schema]

  # this call to read_data will fail out if the selected writers schema
  # is not compatible with the readers schema, because there will be no
  # corresponding hash entry in resolver_cache for that combo
  return read_data(stream, resolver_cache, selected_writers_schema, readers_schema)

def skip_union(stream, resolver_cache, writers_schema, readers_schema):
  index_of_schema = int(read_long(stream))
  if index_of_schema >= len(writers_schema.schemas):
    fail_msg = "Can't access branch index %d for union with %d branches"\
	       % (index_of_schema, len(writers_schema.schemas))
    raise SchemaResolutionException(fail_msg, writers_schema)
  return skip_data(stream, resolver_cache, writers_schema.schemas[index_of_schema])

def read_record(stream, resolver_cache, writers_schema, readers_schema):
  """
  A record is encoded by encoding the values of its fields
  in the order that they are declared. In other words, a record
  is encoded as just the concatenation of the encodings of its fields.
  Field values are encoded per their schema.

  Schema Resolution:
   * the ordering of fields may be different: fields are matched by name.
   * schemas for fields with the same name in both records are resolved
     recursively.
   * if the writer's record contains a field with a name not present in the
     reader's record, the writer's value for that field is ignored.
   * if the reader's record schema has a field that contains a default value,
     and writer's schema does not have a field with the same name, then the
     reader should use the default value from its field.
   * if the reader's record schema has a field with no default value, and
     writer's schema does not have a field with the same name, then the
     field's value is unset.
  """
  key = (writers_schema.fingerprint, readers_schema.fingerprint)
  readers_fields_dict = resolver_cache[key]['readers_fields_dict']
  read_record = {}
  for writers_field in writers_schema.fields:
    if resolver_cache[key]['read_or_skip'][writers_field.name] == 'read':
      readers_field = readers_fields_dict[writers_field.name]
      field_val = read_data(stream, resolver_cache, writers_field.type, readers_field.type)
      read_record[writers_field.name] = field_val
    elif resolver_cache[key]['read_or_skip'][writers_field.name] == 'skip':
      skip_data(stream, resolver_cache, writers_field.type)
    else: # don't know how we get here
      fail_msg = "Some kind of resolution error"
      raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

  # fill in default values
  for (field_name, default_val) in resolver_cache[key]['readers_default_values'].iteritems():
    read_record[field_name] = default_val

  return read_record

def skip_record(stream, resolver_cache, writers_schema):
  for field in writers_schema.fields:
    skip_data(stream, resolver_cache, field.type)



def check_props(schema_one, schema_two, prop_list):
  for prop in prop_list:
    if getattr(schema_one, prop) != getattr(schema_two, prop):
      return False
  return True


def is_schema_match(writers_schema, readers_schema):
  w_type = writers_schema.type
  r_type = readers_schema.type

  if 'union' in [w_type, r_type] or 'error_union' in [w_type, r_type]:
    return True
  elif (w_type in PRIMITIVE_TYPES and r_type in PRIMITIVE_TYPES
	and w_type == r_type):
    return True
  elif (w_type == r_type == 'record' and
	check_props(writers_schema, readers_schema,
				['fullname'])):
    return True
  elif (w_type == r_type == 'error' and
	check_props(writers_schema, readers_schema,
				['fullname'])):
    return True
  elif (w_type == r_type == 'request'):
    return True
  elif (w_type == r_type == 'fixed' and
	check_props(writers_schema, readers_schema,
				['fullname', 'size'])):
    return True
  elif (w_type == r_type == 'enum' and
	check_props(writers_schema, readers_schema,
				['fullname'])):
    return True
  elif (w_type == r_type == 'map' and
	check_props(writers_schema.values,
				readers_schema.values, ['type'])):
    return True
  elif (w_type == r_type == 'array' and
	check_props(writers_schema.items,
				readers_schema.items, ['type'])):
    return True

  # Handle schema promotion
  if w_type == 'int' and r_type in ['long', 'float', 'double']:
    return True
  elif w_type == 'long' and r_type in ['float', 'double']:
    return True
  elif w_type == 'float' and r_type == 'double':
    return True

  return False


def resolve_schemas(resolver_cache, writers_schema, readers_schema):
  key = (writers_schema.fingerprint, readers_schema.fingerprint)

  # break infinite recursion (possible?)
  if key in resolver_cache:
    return

  if not is_schema_match(writers_schema, readers_schema):
    fail_msg = "Schemas don't match"
    raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

  w_type = writers_schema.type
  r_type = readers_schema.type

  # check for reader union compatibility
  if (r_type in ['union', 'error_union'] and
      w_type not in ['union', 'error_union']):
    found_match = False
    for (i, s) in enumerate(readers_schema.schemas):
      try:
	resolve_schemas(resolver_cache, writers_schema, s)
	found_match = True
	break
      except SchemaResolutionException:
	continue
    if not found_match:
      fail_msg = "Writer type not present in reader union."
      raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
    resolver_cache[key] = {'read': read_data, 'skip': skip_data}
    resolver_cache[key]['matching_index'] = i
    resolver_cache[key]['reader_union_writer_not'] = True
    return

  if w_type == 'null':
    resolver_cache[key] = {'read': read_null, 'skip': skip_null}
  elif w_type == 'boolean':
    resolver_cache[key] = {'read': read_boolean, 'skip': skip_boolean}
  elif w_type == 'string':
    resolver_cache[key] = {'read': read_utf8, 'skip': skip_utf8}
  elif w_type == 'int':
    resolver_cache[key] = {'read': read_int, 'skip': skip_int}
  elif w_type == 'long':
    resolver_cache[key] = {'read': read_long, 'skip': skip_long}
  elif w_type == 'float':
    resolver_cache[key] = {'read': read_float, 'skip': skip_float}
  elif w_type == 'double':
    resolver_cache[key] = {'read': read_double, 'skip': skip_double}
  elif w_type == 'bytes':
    resolver_cache[key] = {'read': read_bytes, 'skip': skip_bytes}
  elif w_type == 'fixed':
    resolver_cache[key] = {'read': read_fixed, 'skip': skip_fixed}
  elif w_type == 'enum':
    resolver_cache[key] = {'read': read_enum, 'skip': skip_enum}
    resolver_cache[key]['len'] = len(readers_schema.symbols)
    resolver_cache[key]['set'] = set(readers_schema.symbols)
  elif w_type == 'array':
    resolver_cache[key] = {'read': read_array, 'skip': skip_array}
    resolve_schemas(resolver_cache, writers_schema.items, readers_schema.items)
  elif w_type == 'map':
    resolver_cache[key] = {'read': read_map, 'skip': skip_map}
    resolve_schemas(resolver_cache, writers_schema.values, readers_schema.values)
  elif w_type in ['union', 'error_union']:
    resolver_cache[key] = {'read': read_union, 'skip': skip_union}
    resolver_cache[key]['len'] = len(writers_schema.schemas)
    # check the union schemas against every possible reader schema
    # it will only insert entries into resolver_cache for combos that
    # are acceptable
    found_one = False
    for s in writers_schema.schemas:
      try:
	resolve_schemas(resolver_cache, s, readers_schema)
      except SchemaResolutionException:
	continue
      found_one = True
    if not found_one:
      fail_msg = "None of the writers schema are compatible with the readers schema."
      raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
  elif w_type in ['record', 'error', 'request']:
    resolver_cache[key] = {'read': read_record, 'skip': skip_record}

    # determine which fields to read and which to skip
    writers_fields_dict = writers_schema.fields_dict
    readers_fields_dict = readers_schema.fields_dict
    resolver_cache[key]['readers_fields_dict'] = readers_fields_dict
    resolver_cache[key]['read_or_skip'] = {}
    for writers_field in writers_schema.fields:
      readers_field = readers_fields_dict.get(writers_field.name)
      if readers_field is not None:
	resolver_cache[key]['read_or_skip'][writers_field.name] = 'read'
	resolve_schemas(resolver_cache, writers_field.type, readers_field.type)
      else:
	resolver_cache[key]['read_or_skip'][writers_field.name] = 'skip'
	resolve_skip_schema(resolver_cache, writers_field.type)


    # compute default values
    reader_only_field_names = set(readers_fields_dict.keys()) - set(writers_fields_dict.keys())
    resolver_cache[key]['readers_default_values'] = {}
    for field_name in reader_only_field_names:
      field = readers_fields_dict[field_name]
      if field.has_default:
	field_val = read_default_value(field.type, field.default)
	resolver_cache[key]['readers_default_values'][field.name] = field_val
      else:
	fail_msg = 'No default value for field %s' % field_name
	raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)
  else:
    fail_msg = "Cannot read unknown schema type: %s" % w_type
    raise schema.AvroException(fail_msg)

  # handle primitive type promotion; we already know they are compatible bc
  # is_schema_match() returned True
  if w_type in PRIMITIVE_TYPES:
    if r_type == 'int':
      resolver_cache[key]['pass_through'] = int
    elif r_type == 'long':
      resolver_cache[key]['pass_through'] = long
    elif r_type == 'float':
      resolver_cache[key]['pass_through'] = float
    elif r_type == 'double':
      resolver_cache[key]['pass_through'] = float
    else:
      resolver_cache[key]['pass_through'] = lambda x: x

  # second part of test from above
  resolver_cache[key]['reader_union_writer_not'] = False

def resolve_skip_schema(resolver_cache, writers_schema):
  key = writers_schema.fingerprint

  if key in resolver_cache:
    return

  w_type = writers_schema.type

  if w_type == 'null':
    resolver_cache[key] = {'skip': skip_null}
  elif w_type == 'boolean':
    resolver_cache[key] = {'skip': skip_boolean}
  elif w_type == 'string':
    resolver_cache[key] = {'skip': skip_utf8}
  elif w_type == 'int':
    resolver_cache[key] = {'skip': skip_int}
  elif w_type == 'long':
    resolver_cache[key] = {'skip': skip_long}
  elif w_type == 'float':
    resolver_cache[key] = {'skip': skip_float}
  elif w_type == 'double':
    resolver_cache[key] = {'skip': skip_double}
  elif w_type == 'bytes':
    resolver_cache[key] = {'skip': skip_bytes}
  elif w_type == 'fixed':
    resolver_cache[key] = {'skip': skip_fixed}
  elif w_type == 'enum':
    resolver_cache[key] = {'skip': skip_enum}
  elif w_type == 'array':
    resolver_cache[key] = {'skip': skip_array}
  elif w_type == 'map':
    resolver_cache[key] = {'skip': skip_map}
  elif w_type in ['union', 'error_union']:
    resolver_cache[key] = {'skip': skip_union}
  elif w_type in ['record', 'error', 'request']:
    resolver_cache[key] = {'skip': skip_record}
  else:
    fail_msg = "Cannot skip unknown schema type: %s" % w_type
    raise schema.AvroException(fail_msg)

def read_data(stream, resolver_cache, writers_schema, readers_schema):
  key = (writers_schema.fingerprint, readers_schema.fingerprint)

  try:
    read_op = resolver_cache[key]['read']
  except KeyError:
    fail_msg = 'Schema resolution not in cache; must be unresolvable'
    raise SchemaResolutionException(fail_msg, writers_schema, readers_schema)

  # if readers_schema is union, I must pull out the correct match
  if resolver_cache[key]['reader_union_writer_not'] == True:
    matching_index = resolver_cache[key]['matching_index']
    readers_schema = readers_schema.schemas[matching_index]
    return read_op(stream, resolver_cache, writers_schema, readers_schema)

  if writers_schema.type in PRIMITIVE_TYPES:
    promotion = resolver_cache[key]['pass_through']
    return promotion(read_op(stream))

  return read_op(stream, resolver_cache, writers_schema, readers_schema)

def skip_data(stream, resolver_cache, writers_schema):
  try:
    skip_op = resolver_cache[writers_schema.fingerprint]['skip']
  except KeyError:
    fail_msg = 'Schema resolution not in cache; must be unresolvable'
    raise SchemaResolutionException(fail_msg, writers_schema)
  if writers_schema.type in PRIMITIVE_TYPES:
    skip_op(stream)
  else:
    skip_op(stream, resolver_cache, writers_schema)




def read_default_value(field_schema, default_value):
  """
  Basically a JSON Decoder?
  """
  if field_schema.type == 'null':
    return None
  elif field_schema.type == 'boolean':
    return bool(default_value)
  elif field_schema.type == 'int':
    return int(default_value)
  elif field_schema.type == 'long':
    return long(default_value)
  elif field_schema.type in ['float', 'double']:
    return float(default_value)
  elif field_schema.type in ['enum', 'fixed', 'string', 'bytes']:
    return default_value
  elif field_schema.type == 'array':
    read_array = []
    for json_val in default_value:
      item_val = read_default_value(field_schema.items, json_val)
      read_array.append(item_val)
    return read_array
  elif field_schema.type == 'map':
    read_map = {}
    for key, json_val in default_value.items():
      map_val = read_default_value(field_schema.values, json_val)
      read_map[key] = map_val
    return read_map
  elif field_schema.type in ['union', 'error_union']:
    return read_default_value(field_schema.schemas[0], default_value)
  elif field_schema.type == 'record':
    read_record = {}
    for field in field_schema.fields:
      json_val = default_value.get(field.name)
      if json_val is None: json_val = field.default
      field_val = read_default_value(field.type, json_val)
      read_record[field.name] = field_val
    return read_record
  else:
    fail_msg = 'Unknown type: %s' % field_schema.type
    raise schema.AvroException(fail_msg)


























































##############################################################################
#
#
#       OOOOOOOOO     LLLLLLLLLLL             DDDDDDDDDDDDD
#     OO:::::::::OO   L:::::::::L             D::::::::::::DDD
#   OO:::::::::::::OO L:::::::::L             D:::::::::::::::DD
#  O:::::::OOO:::::::OLL:::::::LL             DDD:::::DDDDD:::::D
#  O::::::O   O::::::O  L:::::L                 D:::::D    D:::::D
#  O:::::O     O:::::O  L:::::L                 D:::::D     D:::::D
#  O:::::O     O:::::O  L:::::L                 D:::::D     D:::::D
#  O:::::O     O:::::O  L:::::L                 D:::::D     D:::::D
#  O:::::O     O:::::O  L:::::L                 D:::::D     D:::::D
#  O:::::O     O:::::O  L:::::L                 D:::::D     D:::::D
#  O:::::O     O:::::O  L:::::L                 D:::::D     D:::::D
#  O::::::O   O::::::O  L:::::L         LLLLLL  D:::::D    D:::::D
#  O:::::::OOO:::::::OLL:::::::LLLLLLLLL:::::LDDD:::::DDDDD:::::D
#   OO:::::::::::::OO L::::::::::::::::::::::LD:::::::::::::::DD
#     OO:::::::::OO   L::::::::::::::::::::::LD::::::::::::DDD
#       OOOOOOOOO     LLLLLLLLLLLLLLLLLLLLLLLLDDDDDDDDDDDDD
#
#
#
#
#
#
#



#
# Validate
#

INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1

def validate(expected_schema, datum):
  """Determine if a python datum is an instance of a schema."""
  schema_type = expected_schema.type
  if schema_type == 'null':
    return datum is None
  elif schema_type == 'boolean':
    return isinstance(datum, bool)
  elif schema_type == 'string':
    return isinstance(datum, basestring)
  elif schema_type == 'bytes':
    return isinstance(datum, str)
  elif schema_type == 'int':
    return ((isinstance(datum, int) or isinstance(datum, long))
	    and INT_MIN_VALUE <= datum <= INT_MAX_VALUE)
  elif schema_type == 'long':
    return ((isinstance(datum, int) or isinstance(datum, long))
	    and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE)
  elif schema_type in ['float', 'double']:
    return (isinstance(datum, int) or isinstance(datum, long)
	    or isinstance(datum, float))
  elif schema_type == 'fixed':
    return isinstance(datum, str) and len(datum) == expected_schema.size
  elif schema_type == 'enum':
    return datum in expected_schema.symbols
  elif schema_type == 'array':
    return (isinstance(datum, list) and
      False not in [validate(expected_schema.items, d) for d in datum])
  elif schema_type == 'map':
    return (isinstance(datum, dict) and
      False not in [isinstance(k, basestring) for k in datum.keys()] and
      False not in
	[validate(expected_schema.values, v) for v in datum.values()])
  elif schema_type in ['union', 'error_union']:
    return True in [validate(s, datum) for s in expected_schema.schemas]
  elif schema_type in ['record', 'error', 'request']:
    return (isinstance(datum, dict) and
      False not in
	[validate(f.type, datum.get(f.name)) for f in expected_schema.fields])




class DatumWriter(object):
  """DatumWriter for generic python objects."""
  def __init__(self, writers_schema=None):
    self._writers_schema = writers_schema

  # read/write properties
  def set_writers_schema(self, writers_schema):
    self._writers_schema = writers_schema
  writers_schema = property(lambda self: self._writers_schema,
			    set_writers_schema)

  def write(self, datum, stream):
    # validate datum
    if not validate(self.writers_schema, datum):
      raise AvroTypeException(self.writers_schema, datum)

    self.write_data(self.writers_schema, datum, stream)

  def write_data(self, writers_schema, datum, stream):
    # function dispatch to write datum
    if writers_schema.type == 'null':
      write_null(stream, datum)
    elif writers_schema.type == 'boolean':
      write_boolean(stream, datum)
    elif writers_schema.type == 'string':
      write_utf8(stream, datum)
    elif writers_schema.type == 'int':
      write_int(stream, datum)
    elif writers_schema.type == 'long':
      write_long(stream, datum)
    elif writers_schema.type == 'float':
      write_float(stream, datum)
    elif writers_schema.type == 'double':
      write_double(stream, datum)
    elif writers_schema.type == 'bytes':
      write_bytes(stream, datum)
    elif writers_schema.type == 'fixed':
      self.write_fixed(writers_schema, datum, stream)
    elif writers_schema.type == 'enum':
      self.write_enum(writers_schema, datum, stream)
    elif writers_schema.type == 'array':
      self.write_array(writers_schema, datum, stream)
    elif writers_schema.type == 'map':
      self.write_map(writers_schema, datum, stream)
    elif writers_schema.type in ['union', 'error_union']:
      self.write_union(writers_schema, datum, stream)
    elif writers_schema.type in ['record', 'error', 'request']:
      self.write_record(writers_schema, datum, stream)
    else:
      fail_msg = 'Unknown type: %s' % writers_schema.type
      raise schema.AvroException(fail_msg)

  def write_fixed(self, writers_schema, datum, stream):
    """
    Fixed instances are encoded using the number of bytes declared
    in the schema.
    """
    stream.write(datum)

  def write_enum(self, writers_schema, datum, stream):
    """
    An enum is encoded by a int, representing the zero-based position
    of the symbol in the schema.
    """
    index_of_datum = writers_schema.symbols.index(datum)
    write_int(stream, index_of_datum)

  def write_array(self, writers_schema, datum, stream):
    """
    Arrays are encoded as a series of blocks.

    Each block consists of a long count value,
    followed by that many array items.
    A block with count zero indicates the end of the array.
    Each item is encoded per the array's item schema.

    If a block's count is negative,
    then the count is followed immediately by a long block size,
    indicating the number of bytes in the block.
    The actual count in this case
    is the absolute value of the count written.
    """
    if len(datum) > 0:
      write_long(stream, len(datum))
      for item in datum:
	self.write_data(writers_schema.items, item, stream)
    write_long(stream, 0)

  def write_map(self, writers_schema, datum, stream):
    """
    Maps are encoded as a series of blocks.

    Each block consists of a long count value,
    followed by that many key/value pairs.
    A block with count zero indicates the end of the map.
    Each item is encoded per the map's value schema.

    If a block's count is negative,
    then the count is followed immediately by a long block size,
    indicating the number of bytes in the block.
    The actual count in this case
    is the absolute value of the count written.
    """
    if len(datum) > 0:
      write_long(stream, len(datum))
      for key, val in datum.items():
	write_utf8(stream, key)
	self.write_data(writers_schema.values, val, stream)
    write_long(stream, 0)

  def write_union(self, writers_schema, datum, stream):
    """
    A union is encoded by first writing a long value indicating
    the zero-based position within the union of the schema of its value.
    The value is then encoded per the indicated schema within the union.
    """
    # resolve union
    index_of_schema = -1
    for i, candidate_schema in enumerate(writers_schema.schemas):
      if validate(candidate_schema, datum):
	index_of_schema = i
    if index_of_schema < 0: raise AvroTypeException(writers_schema, datum)

    # write data
    write_long(stream, index_of_schema)
    self.write_data(writers_schema.schemas[index_of_schema], datum, stream)

  def write_record(self, writers_schema, datum, stream):
    """
    A record is encoded by encoding the values of its fields
    in the order that they are declared. In other words, a record
    is encoded as just the concatenation of the encodings of its fields.
    Field values are encoded per their schema.
    """
    for field in writers_schema.fields:
      self.write_data(field.type, datum.get(field.name), stream)
