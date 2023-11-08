from enum import Enum

try:
    import pandas as pd
except ImportError:
    pd = None
    raise ImportError("please `pip install pyensign[ml]` to use PyEnsign DataFrames")


class DataFrame(pd.DataFrame):
    """
    An Ensign DataFrame is an extension of a Pandas DataFrame and is a view on a
    collection of events.
    """

    @classmethod
    async def from_events(
        cls, events, orient="columns", columns=None, fillna=True, index="id"
    ):
        """
        Create a DataFrame from an async generator of Events. Each event is decoded
        according to its mimetype and added as a record to the retured DataFrame. If
        the event payload is a dict-like object, the keys are the columns and the
        values are the rows. If the payload is an array-like object, each element in
        the array is treated like a separate column. If the payload is some other
        object, the DataFrame will have one column where each row is the decoded object
        for an event.

        Parameters
        ----------
        events: async generator
            An async generator that yields events.Event objects (e.g. the result of
            Ensign.query()).

        orient: str, default "columns"
            The orientation of the DataFrame. If "columns", the events are the rows and
            the columns are the fields or elements in the event payload. If "index",
            the events are the columns and the fields or elements are the rows.

        columns: list, default None
            A list of column names to use if orient is "index". If None, the column
            names are inferred from the event data or will be a RangeIndex.

        fillna: bool, default True
            If True, missing keys in dict-like events will be filled with None. If
            False, an error is raised if the events have mismatched schemas. An error
            is always raised if the events are array-like and have mismatched lengths.

        index: str, default "id"
            If "id", the event index will be the event IDs. If "range", the event index
            will be a pd.RangeIndex. Only applies if orient is "columns".
        """

        orient = orient.lower()
        if orient not in ["columns", "index"]:
            raise ValueError(
                "unsupported orient option, expected one of columns, index"
            )

        index = index.lower()
        if index not in ["id", "range"]:
            raise ValueError("unsupported index option, expected one of id, range")

        obj_type = None
        field_keys = set()
        array_len = 0
        event_data = {}

        # Make an initial pass over the events to infer the DataFrame schema and ensure
        # that the event data types are compatible.
        async for event in events:
            # Every event should have unique ID
            if not event.id:
                raise ValueError("missing event id")
            if event.id in event_data:
                raise ValueError("duplicate event id: {}".format(event.id))

            # Decode the event data
            try:
                obj = event.decode()
            except ValueError as e:
                raise ValueError(
                    "could not decode event data for event with id {}".format(event.id)
                ) from e
            event_data[event.id] = obj

            # Parse the type of the object
            type = ObjectType.parse_object(obj)
            if obj_type and obj_type != type:
                raise ValueError(
                    "incompatible event data types: {} and {}".format(obj_type, type)
                )
            obj_type = type

            # Compatibility checks
            if type == ObjectType.ARRAY:
                if array_len == 0:
                    array_len = len(obj)
                elif len(obj) != array_len:
                    raise ValueError(
                        "expected array-like event data of length {}, got length {}".format(
                            array_len, len(obj)
                        )
                    )
            elif type == ObjectType.DICT:
                obj_keys = set(obj.keys())
                if len(field_keys) == 0:
                    field_keys = obj_keys
                elif fillna:
                    field_keys = field_keys.union(obj_keys)
                elif field_keys != obj_keys:
                    raise ValueError(
                        "mismatched event schemas for fillna=False, expected {}, got {}".format(
                            field_keys, obj_keys
                        )
                    )

        # Assemble the DataFrame data with the user-provided orientation
        if obj_type == ObjectType.SCALAR:
            return cls._scalar_dataframe(event_data, orient, columns, index)
        elif obj_type == ObjectType.ARRAY:
            return cls._array_dataframe(event_data, orient, columns, index, array_len)
        else:
            return cls._dict_dataframe(event_data, orient, columns, index, field_keys)

    @classmethod
    def _scalar_dataframe(cls, events, orient, columns, index):
        data = []
        keys = sorted(events.keys())
        if orient == "columns":
            for key in keys:
                data.append(events[key])
            columns = pd.RangeIndex(1)
            index = keys if index == "id" else pd.RangeIndex(len(data))
        else:
            data.append([events[key] for key in keys])
            columns = keys if not columns else columns
            index = pd.RangeIndex(len(data))
        return cls(data=data, columns=columns, index=index)

    @classmethod
    def _array_dataframe(cls, events, orient, columns, index, array_len):
        data = []
        keys = sorted(events.keys())
        if orient == "columns":
            for key in keys:
                data.append([v for v in events[key]])
            columns = pd.RangeIndex(array_len)
            index = keys if index == "id" else pd.RangeIndex(len(data))
        else:
            for i in range(array_len):
                data.append([events[key][i] for key in keys])
            columns = keys if not columns else columns
            index = pd.RangeIndex(array_len)
        return cls(data=data, columns=columns, index=index)

    @classmethod
    def _dict_dataframe(cls, events, orient, columns, index, field_keys):
        data = []
        keys = sorted(events.keys())
        fields = sorted(field_keys)
        if orient == "columns":
            for key in keys:
                data.append([events[key].get(field, None) for field in fields])
            columns = fields
            index = keys if index == "id" else pd.RangeIndex(len(data))
        else:
            for field in fields:
                data.append([events[key].get(field, None) for key in keys])
            columns = keys if not columns else columns
            index = pd.RangeIndex(len(data)) if not fields else fields
        return cls(data=data, columns=columns, index=index)


class ObjectType(Enum):
    SCALAR = 0
    ARRAY = 1
    DICT = 2

    def __str__(self):
        if self == ObjectType.SCALAR:
            return "scalar"
        elif self == ObjectType.ARRAY:
            return "array"
        elif self == ObjectType.DICT:
            return "dict"

    @classmethod
    def parse_object(cls, obj):
        """
        Parse an object into its type.
        """

        t = type(obj)
        if t == dict:
            return ObjectType.DICT
        elif t in (list, tuple, pd.Series):
            # TODO: Check for numpy arrays?
            return ObjectType.ARRAY
        else:
            return ObjectType.SCALAR
