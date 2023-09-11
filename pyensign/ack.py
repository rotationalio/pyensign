from pyensign.utils import pbtime


class Ack:
    """
    Ack represents an acknowledgement from the server that an event was published or an
    acknowledgement from the client that an event was processed.
    """

    def __init__(self, id, committed):
        self.id = id
        self.committed = pbtime.to_datetime(committed)

    def __str__(self):
        return f"id: {self.id}\ncommitted: {self.committed}"
