# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: api/v1beta1/ensign.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from pyensign.api.v1beta1 import event_pb2 as api_dot_v1beta1_dot_event__pb2
from pyensign.api.v1beta1 import topic_pb2 as api_dot_v1beta1_dot_topic__pb2
from pyensign.api.v1beta1 import groups_pb2 as api_dot_v1beta1_dot_groups__pb2
from pyensign.api.v1beta1 import query_pb2 as api_dot_v1beta1_dot_query__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x18\x61pi/v1beta1/ensign.proto\x12\x0e\x65nsign.v1beta1\x1a\x17\x61pi/v1beta1/event.proto\x1a\x17\x61pi/v1beta1/topic.proto\x1a\x18\x61pi/v1beta1/groups.proto\x1a\x17\x61pi/v1beta1/query.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto"}\n\x10PublisherRequest\x12-\n\x05\x65vent\x18\x01 \x01(\x0b\x32\x1c.ensign.v1beta1.EventWrapperH\x00\x12\x31\n\x0bopen_stream\x18\x02 \x01(\x0b\x32\x1a.ensign.v1beta1.OpenStreamH\x00\x42\x07\n\x05\x65mbed"\xc6\x01\n\x0ePublisherReply\x12"\n\x03\x61\x63k\x18\x01 \x01(\x0b\x32\x13.ensign.v1beta1.AckH\x00\x12$\n\x04nack\x18\x02 \x01(\x0b\x32\x14.ensign.v1beta1.NackH\x00\x12,\n\x05ready\x18\x03 \x01(\x0b\x32\x1b.ensign.v1beta1.StreamReadyH\x00\x12\x33\n\x0c\x63lose_stream\x18\x04 \x01(\x0b\x32\x1b.ensign.v1beta1.CloseStreamH\x00\x42\x07\n\x05\x65mbed"\x9b\x01\n\x10SubscribeRequest\x12"\n\x03\x61\x63k\x18\x01 \x01(\x0b\x32\x13.ensign.v1beta1.AckH\x00\x12$\n\x04nack\x18\x02 \x01(\x0b\x32\x14.ensign.v1beta1.NackH\x00\x12\x34\n\x0csubscription\x18\x03 \x01(\x0b\x32\x1c.ensign.v1beta1.SubscriptionH\x00\x42\x07\n\x05\x65mbed"\xab\x01\n\x0eSubscribeReply\x12-\n\x05\x65vent\x18\x01 \x01(\x0b\x32\x1c.ensign.v1beta1.EventWrapperH\x00\x12,\n\x05ready\x18\x02 \x01(\x0b\x32\x1b.ensign.v1beta1.StreamReadyH\x00\x12\x33\n\x0c\x63lose_stream\x18\x03 \x01(\x0b\x32\x1b.ensign.v1beta1.CloseStreamH\x00\x42\x07\n\x05\x65mbed"@\n\x03\x41\x63k\x12\n\n\x02id\x18\x01 \x01(\x0c\x12-\n\tcommitted\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp"\x90\x03\n\x04Nack\x12\n\n\x02id\x18\x01 \x01(\x0c\x12\'\n\x04\x63ode\x18\x02 \x01(\x0e\x32\x19.ensign.v1beta1.Nack.Code\x12\r\n\x05\x65rror\x18\x03 \x01(\t"\xc3\x02\n\x04\x43ode\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x1b\n\x17MAX_EVENT_SIZE_EXCEEDED\x10\x01\x12\x10\n\x0cTOPIC_UKNOWN\x10\x02\x12\x13\n\x0fTOPIC_ARCHVIVED\x10\x03\x12\x11\n\rTOPIC_DELETED\x10\x04\x12\x15\n\x11PERMISSION_DENIED\x10\x05\x12\x15\n\x11\x43ONSENSUS_FAILURE\x10\x06\x12\x14\n\x10SHARDING_FAILURE\x10\x07\x12\x0c\n\x08REDIRECT\x10\x08\x12\x0c\n\x08INTERNAL\x10\t\x12\x0f\n\x0bUNPROCESSED\x10\x64\x12\x0b\n\x07TIMEOUT\x10\x65\x12\x16\n\x12UNHANDLED_MIMETYPE\x10\x66\x12\x10\n\x0cUNKNOWN_TYPE\x10g\x12\x15\n\x11\x44\x45LIVER_AGAIN_ANY\x10h\x12\x18\n\x14\x44\x45LIVER_AGAIN_NOT_ME\x10i"/\n\nOpenStream\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x0e\n\x06topics\x18\x02 \x03(\t"@\n\x0b\x43loseStream\x12\x0e\n\x06\x65vents\x18\x01 \x01(\x04\x12\x0e\n\x06topics\x18\x02 \x01(\x04\x12\x11\n\tconsumers\x18\x03 \x01(\x04"\x9b\x01\n\x0bStreamReady\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x11\n\tserver_id\x18\x02 \x01(\t\x12\x37\n\x06topics\x18\x03 \x03(\x0b\x32\'.ensign.v1beta1.StreamReady.TopicsEntry\x1a-\n\x0bTopicsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c:\x02\x38\x01"\x85\x01\n\x0cSubscription\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x0e\n\x06topics\x18\x02 \x03(\t\x12$\n\x05query\x18\x03 \x01(\x0b\x32\x15.ensign.v1beta1.Query\x12,\n\x05group\x18\x04 \x01(\x0b\x32\x1d.ensign.v1beta1.ConsumerGroup"\x1d\n\x0bInfoRequest\x12\x0e\n\x06topics\x18\x01 \x03(\x0c"\xba\x01\n\x0bProjectInfo\x12\x12\n\nproject_id\x18\x01 \x01(\x0c\x12\x12\n\nnum_topics\x18\x02 \x01(\x04\x12\x1b\n\x13num_readonly_topics\x18\x03 \x01(\x04\x12\x0e\n\x06\x65vents\x18\x07 \x01(\x04\x12\x12\n\nduplicates\x18\x08 \x01(\x04\x12\x17\n\x0f\x64\x61ta_size_bytes\x18\t \x01(\x04\x12)\n\x06topics\x18\x0f \x03(\x0b\x32\x19.ensign.v1beta1.TopicInfo"T\n\x0bHealthCheck\x12\x10\n\x08\x61ttempts\x18\x01 \x01(\r\x12\x33\n\x0flast_checked_at\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp"\xbb\x02\n\x0cServiceState\x12\x33\n\x06status\x18\x01 \x01(\x0e\x32#.ensign.v1beta1.ServiceState.Status\x12\x0f\n\x07version\x18\x02 \x01(\t\x12)\n\x06uptime\x18\x03 \x01(\x0b\x32\x19.google.protobuf.Duration\x12.\n\nnot_before\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12-\n\tnot_after\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp"[\n\x06Status\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x0b\n\x07HEALTHY\x10\x01\x12\r\n\tUNHEALTHY\x10\x02\x12\n\n\x06\x44\x41NGER\x10\x03\x12\x0b\n\x07OFFLINE\x10\x04\x12\x0f\n\x0bMAINTENANCE\x10\x05"6\n\x08PageInfo\x12\x11\n\tpage_size\x18\x01 \x01(\r\x12\x17\n\x0fnext_page_token\x18\x02 \x01(\t2\xeb\x06\n\x06\x45nsign\x12Q\n\x07Publish\x12 .ensign.v1beta1.PublisherRequest\x1a\x1e.ensign.v1beta1.PublisherReply"\x00(\x01\x30\x01\x12S\n\tSubscribe\x12 .ensign.v1beta1.SubscribeRequest\x1a\x1e.ensign.v1beta1.SubscribeReply"\x00(\x01\x30\x01\x12@\n\x05\x45nSQL\x12\x15.ensign.v1beta1.Query\x1a\x1c.ensign.v1beta1.EventWrapper"\x00\x30\x01\x12\x44\n\x07\x45xplain\x12\x15.ensign.v1beta1.Query\x1a .ensign.v1beta1.QueryExplanation"\x00\x12\x44\n\nListTopics\x12\x18.ensign.v1beta1.PageInfo\x1a\x1a.ensign.v1beta1.TopicsPage"\x00\x12=\n\x0b\x43reateTopic\x12\x15.ensign.v1beta1.Topic\x1a\x15.ensign.v1beta1.Topic"\x00\x12?\n\rRetrieveTopic\x12\x15.ensign.v1beta1.Topic\x1a\x15.ensign.v1beta1.Topic"\x00\x12I\n\x0b\x44\x65leteTopic\x12\x18.ensign.v1beta1.TopicMod\x1a\x1e.ensign.v1beta1.TopicTombstone"\x00\x12H\n\nTopicNames\x12\x18.ensign.v1beta1.PageInfo\x1a\x1e.ensign.v1beta1.TopicNamesPage"\x00\x12K\n\x0bTopicExists\x12\x19.ensign.v1beta1.TopicName\x1a\x1f.ensign.v1beta1.TopicExistsInfo"\x00\x12\x42\n\x04Info\x12\x1b.ensign.v1beta1.InfoRequest\x1a\x1b.ensign.v1beta1.ProjectInfo"\x00\x12\x45\n\x06Status\x12\x1b.ensign.v1beta1.HealthCheck\x1a\x1c.ensign.v1beta1.ServiceState"\x00\x62\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "api.v1beta1.ensign_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    _STREAMREADY_TOPICSENTRY._options = None
    _STREAMREADY_TOPICSENTRY._serialized_options = b"8\001"
    _PUBLISHERREQUEST._serialized_start = 210
    _PUBLISHERREQUEST._serialized_end = 335
    _PUBLISHERREPLY._serialized_start = 338
    _PUBLISHERREPLY._serialized_end = 536
    _SUBSCRIBEREQUEST._serialized_start = 539
    _SUBSCRIBEREQUEST._serialized_end = 694
    _SUBSCRIBEREPLY._serialized_start = 697
    _SUBSCRIBEREPLY._serialized_end = 868
    _ACK._serialized_start = 870
    _ACK._serialized_end = 934
    _NACK._serialized_start = 937
    _NACK._serialized_end = 1337
    _NACK_CODE._serialized_start = 1014
    _NACK_CODE._serialized_end = 1337
    _OPENSTREAM._serialized_start = 1339
    _OPENSTREAM._serialized_end = 1386
    _CLOSESTREAM._serialized_start = 1388
    _CLOSESTREAM._serialized_end = 1452
    _STREAMREADY._serialized_start = 1455
    _STREAMREADY._serialized_end = 1610
    _STREAMREADY_TOPICSENTRY._serialized_start = 1565
    _STREAMREADY_TOPICSENTRY._serialized_end = 1610
    _SUBSCRIPTION._serialized_start = 1613
    _SUBSCRIPTION._serialized_end = 1746
    _INFOREQUEST._serialized_start = 1748
    _INFOREQUEST._serialized_end = 1777
    _PROJECTINFO._serialized_start = 1780
    _PROJECTINFO._serialized_end = 1966
    _HEALTHCHECK._serialized_start = 1968
    _HEALTHCHECK._serialized_end = 2052
    _SERVICESTATE._serialized_start = 2055
    _SERVICESTATE._serialized_end = 2370
    _SERVICESTATE_STATUS._serialized_start = 2279
    _SERVICESTATE_STATUS._serialized_end = 2370
    _PAGEINFO._serialized_start = 2372
    _PAGEINFO._serialized_end = 2426
    _ENSIGN._serialized_start = 2429
    _ENSIGN._serialized_end = 3304
# @@protoc_insertion_point(module_scope)
