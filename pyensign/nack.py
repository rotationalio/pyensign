from pyensign.api.v1beta1.ensign_pb2 import Nack

Unknown = Nack.Code.UNKNOWN

# Client-side error codes. Subscribers should use these codes when nack-ing events to
# ensure compatibility with Ensign.
Unprocessed = Nack.Code.UNPROCESSED
Timeout = Nack.Code.TIMEOUT
UnhandledMimetype = Nack.Code.UNHANDLED_MIMETYPE
UnknownType = Nack.Code.UNKNOWN_TYPE
DeliverAgainAny = Nack.Code.DELIVER_AGAIN_ANY
DeliverAgainNotMe = Nack.Code.DELIVER_AGAIN_NOT_ME

# Server-side error codes. These codes are returned by the Ensign service. Publishers
# should inspect the error code in a Nack returned by the Ensign service to determine
# why the event was not published.
MaxEventSizeExceeded = Nack.Code.MAX_EVENT_SIZE_EXCEEDED
TopicUnknown = Nack.Code.TOPIC_UKNOWN
TopicArchived = Nack.Code.TOPIC_ARCHVIVED
TopicDeleted = Nack.Code.TOPIC_DELETED
PermissionDenied = Nack.Code.PERMISSION_DENIED
ConsensusFailure = Nack.Code.CONSENSUS_FAILURE
ShardingFailure = Nack.Code.SHARDING_FAILURE
Redirect = Nack.Code.REDIRECT
Internal = Nack.Code.INTERNAL
