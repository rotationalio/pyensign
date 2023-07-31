from grpc import aio, StatusCode

from pyensign.exceptions import AuthenticationError


class MetadataInterceptor:
    """
    MetadataInterceptor is the base class for interceptors that add metadata to each
    gRPC request (e.g. authentication credentials)
    """

    def __init__(self, metadata_fn):
        self.metadata_fn = metadata_fn

    def get_metadata(self, client_call_details):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        try:
            metadata.append(self.metadata_fn())
        except AuthenticationError as e:
            # TODO: gRPC wants either an AioRpcError or an asyncio.CancelledError,
            # this is a bit of a hack to get gRPC to handle the exception, so users
            # will see an EnsignRPCError instead of the AuthenticationError
            raise aio.AioRpcError(
                StatusCode.UNAUTHENTICATED,
                initial_metadata=None,
                trailing_metadata=None,
                details=str(e),
            )
        return metadata


class MetadataUnaryInterceptor(MetadataInterceptor, aio.UnaryUnaryClientInterceptor):
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        """
        Intercepts a unary-unary call to add a metadata tuple.
        """

        client_call_details = client_call_details._replace(
            metadata=self.get_metadata(client_call_details)
        )
        return await continuation(client_call_details, request)


class MetadataStreamInterceptor(MetadataInterceptor, aio.StreamStreamClientInterceptor):
    async def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        """
        Intercepts a stream-stream call to add a metadata tuple.
        """

        client_call_details = client_call_details._replace(
            metadata=self.get_metadata(client_call_details)
        )
        return await continuation(client_call_details, request_iterator)
