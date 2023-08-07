from datetime import datetime


def to_datetime(pbtime):
    """
    Convert a google.protobuf.Timestamp into a datetime.datetime.
    """

    return datetime.fromtimestamp(pbtime.seconds + pbtime.nanos / 1e9)
