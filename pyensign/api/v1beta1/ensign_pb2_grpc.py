# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from api.v1beta1 import ensign_pb2 as api_dot_v1beta1_dot_ensign__pb2
from api.v1beta1 import event_pb2 as api_dot_v1beta1_dot_event__pb2
from api.v1beta1 import topic_pb2 as api_dot_v1beta1_dot_topic__pb2


class EnsignStub(object):
    """The Ensign service is meant to allow publishers (producers) and subscribers
    (consumers) of events to interact with the Ensign eventing system; e.g. this is a
    user-oriented API that is the basis of the user SDKs that we will build. There are
    two primary interactions that the user client may have: publishing or subscribing to
    topics to send and receive events or managing topics that are available.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Publish = channel.stream_stream(
            "/ensign.v1beta1.Ensign/Publish",
            request_serializer=api_dot_v1beta1_dot_event__pb2.Event.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_ensign__pb2.Publication.FromString,
        )
        self.Subscribe = channel.stream_stream(
            "/ensign.v1beta1.Ensign/Subscribe",
            request_serializer=api_dot_v1beta1_dot_ensign__pb2.Subscription.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_event__pb2.Event.FromString,
        )
        self.ListTopics = channel.unary_unary(
            "/ensign.v1beta1.Ensign/ListTopics",
            request_serializer=api_dot_v1beta1_dot_ensign__pb2.PageInfo.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_topic__pb2.TopicsPage.FromString,
        )
        self.CreateTopic = channel.unary_unary(
            "/ensign.v1beta1.Ensign/CreateTopic",
            request_serializer=api_dot_v1beta1_dot_topic__pb2.Topic.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_topic__pb2.Topic.FromString,
        )
        self.RetrieveTopic = channel.unary_unary(
            "/ensign.v1beta1.Ensign/RetrieveTopic",
            request_serializer=api_dot_v1beta1_dot_topic__pb2.Topic.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_topic__pb2.Topic.FromString,
        )
        self.DeleteTopic = channel.unary_unary(
            "/ensign.v1beta1.Ensign/DeleteTopic",
            request_serializer=api_dot_v1beta1_dot_topic__pb2.TopicMod.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_topic__pb2.TopicTombstone.FromString,
        )
        self.TopicNames = channel.unary_unary(
            "/ensign.v1beta1.Ensign/TopicNames",
            request_serializer=api_dot_v1beta1_dot_ensign__pb2.PageInfo.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_topic__pb2.TopicNamesPage.FromString,
        )
        self.TopicExists = channel.unary_unary(
            "/ensign.v1beta1.Ensign/TopicExists",
            request_serializer=api_dot_v1beta1_dot_topic__pb2.TopicName.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_topic__pb2.TopicExistsInfo.FromString,
        )
        self.Status = channel.unary_unary(
            "/ensign.v1beta1.Ensign/Status",
            request_serializer=api_dot_v1beta1_dot_ensign__pb2.HealthCheck.SerializeToString,
            response_deserializer=api_dot_v1beta1_dot_ensign__pb2.ServiceState.FromString,
        )


class EnsignServicer(object):
    """The Ensign service is meant to allow publishers (producers) and subscribers
    (consumers) of events to interact with the Ensign eventing system; e.g. this is a
    user-oriented API that is the basis of the user SDKs that we will build. There are
    two primary interactions that the user client may have: publishing or subscribing to
    topics to send and receive events or managing topics that are available.
    """

    def Publish(self, request_iterator, context):
        """Both the Publish and Subscribe RPCs are bidirectional streaming to allow for acks
        and nacks of events to be sent between Ensign and the client. The Publish stream
        is opened and the client sends events and receives acks/nacks -- when the client
        closes the publish stream, the server sends back information about the current
        state of the topic. When the Subscribe stream is opened, the client must send an
        open stream message with the subscription info before receiving events. Once it
        receives events it must send back acks/nacks up the stream so that Ensign
        advances the topic offset for the rest of the clients in the group.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Subscribe(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def ListTopics(self, request, context):
        """This is a simple topic management interface. Right now we assume that topics are
        immutable, therefore there is no update topic RPC call. There are two ways to
        delete a topic - archiving it makes the topic readonly so that no events can be
        published to it, but it can still be read. Destroying the topic deletes it and
        removes all of its data, freeing up the topic name to be used again.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def CreateTopic(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def RetrieveTopic(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def DeleteTopic(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def TopicNames(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def TopicExists(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Status(self, request, context):
        """Implements a client-side heartbeat that can also be used by monitoring tools."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_EnsignServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Publish": grpc.stream_stream_rpc_method_handler(
            servicer.Publish,
            request_deserializer=api_dot_v1beta1_dot_event__pb2.Event.FromString,
            response_serializer=api_dot_v1beta1_dot_ensign__pb2.Publication.SerializeToString,
        ),
        "Subscribe": grpc.stream_stream_rpc_method_handler(
            servicer.Subscribe,
            request_deserializer=api_dot_v1beta1_dot_ensign__pb2.Subscription.FromString,
            response_serializer=api_dot_v1beta1_dot_event__pb2.Event.SerializeToString,
        ),
        "ListTopics": grpc.unary_unary_rpc_method_handler(
            servicer.ListTopics,
            request_deserializer=api_dot_v1beta1_dot_ensign__pb2.PageInfo.FromString,
            response_serializer=api_dot_v1beta1_dot_topic__pb2.TopicsPage.SerializeToString,
        ),
        "CreateTopic": grpc.unary_unary_rpc_method_handler(
            servicer.CreateTopic,
            request_deserializer=api_dot_v1beta1_dot_topic__pb2.Topic.FromString,
            response_serializer=api_dot_v1beta1_dot_topic__pb2.Topic.SerializeToString,
        ),
        "RetrieveTopic": grpc.unary_unary_rpc_method_handler(
            servicer.RetrieveTopic,
            request_deserializer=api_dot_v1beta1_dot_topic__pb2.Topic.FromString,
            response_serializer=api_dot_v1beta1_dot_topic__pb2.Topic.SerializeToString,
        ),
        "DeleteTopic": grpc.unary_unary_rpc_method_handler(
            servicer.DeleteTopic,
            request_deserializer=api_dot_v1beta1_dot_topic__pb2.TopicMod.FromString,
            response_serializer=api_dot_v1beta1_dot_topic__pb2.TopicTombstone.SerializeToString,
        ),
        "TopicNames": grpc.unary_unary_rpc_method_handler(
            servicer.TopicNames,
            request_deserializer=api_dot_v1beta1_dot_ensign__pb2.PageInfo.FromString,
            response_serializer=api_dot_v1beta1_dot_topic__pb2.TopicNamesPage.SerializeToString,
        ),
        "TopicExists": grpc.unary_unary_rpc_method_handler(
            servicer.TopicExists,
            request_deserializer=api_dot_v1beta1_dot_topic__pb2.TopicName.FromString,
            response_serializer=api_dot_v1beta1_dot_topic__pb2.TopicExistsInfo.SerializeToString,
        ),
        "Status": grpc.unary_unary_rpc_method_handler(
            servicer.Status,
            request_deserializer=api_dot_v1beta1_dot_ensign__pb2.HealthCheck.FromString,
            response_serializer=api_dot_v1beta1_dot_ensign__pb2.ServiceState.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "ensign.v1beta1.Ensign", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class Ensign(object):
    """The Ensign service is meant to allow publishers (producers) and subscribers
    (consumers) of events to interact with the Ensign eventing system; e.g. this is a
    user-oriented API that is the basis of the user SDKs that we will build. There are
    two primary interactions that the user client may have: publishing or subscribing to
    topics to send and receive events or managing topics that are available.
    """

    @staticmethod
    def Publish(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/ensign.v1beta1.Ensign/Publish",
            api_dot_v1beta1_dot_event__pb2.Event.SerializeToString,
            api_dot_v1beta1_dot_ensign__pb2.Publication.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Subscribe(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/ensign.v1beta1.Ensign/Subscribe",
            api_dot_v1beta1_dot_ensign__pb2.Subscription.SerializeToString,
            api_dot_v1beta1_dot_event__pb2.Event.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def ListTopics(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ensign.v1beta1.Ensign/ListTopics",
            api_dot_v1beta1_dot_ensign__pb2.PageInfo.SerializeToString,
            api_dot_v1beta1_dot_topic__pb2.TopicsPage.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def CreateTopic(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ensign.v1beta1.Ensign/CreateTopic",
            api_dot_v1beta1_dot_topic__pb2.Topic.SerializeToString,
            api_dot_v1beta1_dot_topic__pb2.Topic.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def RetrieveTopic(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ensign.v1beta1.Ensign/RetrieveTopic",
            api_dot_v1beta1_dot_topic__pb2.Topic.SerializeToString,
            api_dot_v1beta1_dot_topic__pb2.Topic.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def DeleteTopic(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ensign.v1beta1.Ensign/DeleteTopic",
            api_dot_v1beta1_dot_topic__pb2.TopicMod.SerializeToString,
            api_dot_v1beta1_dot_topic__pb2.TopicTombstone.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def TopicNames(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ensign.v1beta1.Ensign/TopicNames",
            api_dot_v1beta1_dot_ensign__pb2.PageInfo.SerializeToString,
            api_dot_v1beta1_dot_topic__pb2.TopicNamesPage.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def TopicExists(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ensign.v1beta1.Ensign/TopicExists",
            api_dot_v1beta1_dot_topic__pb2.TopicName.SerializeToString,
            api_dot_v1beta1_dot_topic__pb2.TopicExistsInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Status(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ensign.v1beta1.Ensign/Status",
            api_dot_v1beta1_dot_ensign__pb2.HealthCheck.SerializeToString,
            api_dot_v1beta1_dot_ensign__pb2.ServiceState.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
