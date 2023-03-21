# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: api/v1beta1/topic.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from api.v1beta1 import event_pb2 as api_dot_v1beta1_dot_event__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x17\x61pi/v1beta1/topic.proto\x12\x0e\x65nsign.v1beta1\x1a\x17\x61pi/v1beta1/event.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\x96\x02\n\x05Topic\x12\n\n\x02id\x18\x01 \x01(\x0c\x12\x12\n\nproject_id\x18\x02 \x01(\x0c\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x10\n\x08readonly\x18\x04 \x01(\x08\x12\x0e\n\x06offset\x18\x05 \x01(\x04\x12\x0e\n\x06shards\x18\x06 \x01(\r\x12-\n\nplacements\x18\x0c \x03(\x0b\x32\x19.ensign.v1beta1.Placement\x12#\n\x05types\x18\r \x03(\x0b\x32\x14.ensign.v1beta1.Type\x12+\n\x07\x63reated\x18\x0e \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08modified\x18\x0f \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"L\n\nTopicsPage\x12%\n\x06topics\x18\x01 \x03(\x0b\x32\x15.ensign.v1beta1.Topic\x12\x17\n\x0fnext_page_token\x18\x02 \x01(\t\"~\n\x08TopicMod\x12\n\n\x02id\x18\x01 \x01(\t\x12\x35\n\toperation\x18\x02 \x01(\x0e\x32\".ensign.v1beta1.TopicMod.Operation\"/\n\tOperation\x12\x08\n\x04NOOP\x10\x00\x12\x0b\n\x07\x41RCHIVE\x10\x01\x12\x0b\n\x07\x44\x45STROY\x10\x02\"\x85\x01\n\x0eTopicTombstone\x12\n\n\x02id\x18\x01 \x01(\t\x12\x34\n\x05state\x18\x02 \x01(\x0e\x32%.ensign.v1beta1.TopicTombstone.Status\"1\n\x06Status\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x0c\n\x08READONLY\x10\x01\x12\x0c\n\x08\x44\x45LETING\x10\x02\"\x9c\x01\n\tPlacement\x12\r\n\x05\x65poch\x18\x01 \x01(\x04\x12\x32\n\x08sharding\x18\x02 \x01(\x0e\x32 .ensign.v1beta1.ShardingStrategy\x12\'\n\x07regions\x18\x03 \x03(\x0b\x32\x16.ensign.v1beta1.Region\x12#\n\x05nodes\x18\x04 \x03(\x0b\x32\x14.ensign.v1beta1.Node\"x\n\x04Node\x12\n\n\x02id\x18\x01 \x01(\t\x12\x10\n\x08hostname\x18\x02 \x01(\t\x12\x0e\n\x06quorum\x18\x03 \x01(\x04\x12\r\n\x05shard\x18\x04 \x01(\x04\x12&\n\x06region\x18\x05 \x01(\x0b\x32\x16.ensign.v1beta1.Region\x12\x0b\n\x03url\x18\x06 \x01(\t*m\n\x10ShardingStrategy\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x0f\n\x0bNO_SHARDING\x10\x01\x12\x17\n\x13\x43ONSISTENT_KEY_HASH\x10\x02\x12\n\n\x06RANDOM\x10\x03\x12\x16\n\x12PUBLISHER_ORDERING\x10\x04\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'api.v1beta1.topic_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _SHARDINGSTRATEGY._serialized_start=1005
  _SHARDINGSTRATEGY._serialized_end=1114
  _TOPIC._serialized_start=102
  _TOPIC._serialized_end=380
  _TOPICSPAGE._serialized_start=382
  _TOPICSPAGE._serialized_end=458
  _TOPICMOD._serialized_start=460
  _TOPICMOD._serialized_end=586
  _TOPICMOD_OPERATION._serialized_start=539
  _TOPICMOD_OPERATION._serialized_end=586
  _TOPICTOMBSTONE._serialized_start=589
  _TOPICTOMBSTONE._serialized_end=722
  _TOPICTOMBSTONE_STATUS._serialized_start=673
  _TOPICTOMBSTONE_STATUS._serialized_end=722
  _PLACEMENT._serialized_start=725
  _PLACEMENT._serialized_end=881
  _NODE._serialized_start=883
  _NODE._serialized_end=1003
# @@protoc_insertion_point(module_scope)
