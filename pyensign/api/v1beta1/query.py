from pyensign.api.v1beta1 import query_pb2


def format_query(query, params=None, include_duplicates=False):
    """
    Accepts a query string and parameters, performs any necessary preprocessing and
    valiation and returns a properly formatted query for use with Ensign.

    Parameters
    ----------
    query : str
        A query string, which may have placeholders for parameters.

    params : dict
        A dictionary of parameters to be substituted into the query string.

    include_duplicates : bool
        Adds the include_duplicates query option to the returned query.

    Returns
    -------
    query_pb2.Query
        A formatted query object which is compatible with Ensign.

    Raises
    ------
    TypeError
        If the query is not a string or an unsupported parameter is provided.

    ValueError
        If the query is empty.
    """

    if not isinstance(query, str):
        raise TypeError("query must be a string")

    query = query.strip()
    if query == "":
        raise ValueError("query must not be empty")

    # Parse the args into the protobuf parameters
    parameters = []
    if params:
        if not isinstance(params, dict):
            raise TypeError("params must be a dict")

        for name, value in params.items():
            if isinstance(value, bool):
                parameters.append(query_pb2.Parameter(name=name, b=value))
            elif isinstance(value, int):
                parameters.append(query_pb2.Parameter(name=name, i=value))
            elif isinstance(value, float):
                parameters.append(query_pb2.Parameter(name=name, d=value))
            elif isinstance(value, bytes):
                parameters.append(query_pb2.Parameter(name=name, y=value))
            elif isinstance(value, str):
                parameters.append(query_pb2.Parameter(name=name, s=value))
            else:
                raise TypeError(
                    "unsupported parameter type: {}".format(type(value).__name__)
                )

    return query_pb2.Query(
        query=query, params=parameters, include_duplicates=include_duplicates
    )
