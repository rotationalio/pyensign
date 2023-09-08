from datetime import datetime


def to_datetime(pbtime):
    """
    Convert a google.protobuf.Timestamp into a datetime.datetime.
    """
    # Temporary workaround for zero-valued timestamps
    if pbtime.seconds < 0:
        return None
    return datetime.fromtimestamp(pbtime.seconds + pbtime.nanos / 1e9)
