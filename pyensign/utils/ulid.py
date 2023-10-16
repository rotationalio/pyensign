from ulid import ULID


def parse(id):
    """
    Parse a ULID from its string or bytes representation.
    """

    if isinstance(id, ULID):
        return id
    elif isinstance(id, str):
        return ULID.from_str(id)
    elif isinstance(id, bytes):
        return ULID.from_bytes(id)
    else:
        raise TypeError("cannot parse ULID from {}".format(type(id)))
