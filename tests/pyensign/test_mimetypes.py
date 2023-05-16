import pytest

from pyensign import mimetypes as mt
from pyensign.mimetype.v1beta1 import mimetype_pb2


def test_mimetypes():
    """
    Test that all mimetypes in the protobufs are defined in the Python code and vice
    versa.
    """

    proto_mimetypes = mimetype_pb2.DESCRIPTOR.enum_types_by_name["MIME"].values_by_name
    proto_values = [v.number for v in proto_mimetypes.values()]

    for value in proto_values:
        assert value in mt.names_by_value

    for value in mt.names_by_value:
        assert value in proto_values

    # Ensure both Python maps have the same entries
    for name, value in mt.values_by_name.items():
        assert value in mt.names_by_value
        assert mt.names_by_value[value] == name

    for value, name in mt.names_by_value.items():
        assert name in mt.values_by_name
        assert mt.values_by_name[name] == value

    assert len(mt.names_by_value) == len(mt.values_by_name)


@pytest.mark.parametrize(
    "mimetype,expected",
    [
        (0, mt.ApplicationOctetStream),
        (100, mt.ApplicationXML),
        ("application/json", mt.ApplicationJSON),
        ("user/format-5", mt.UserSpecified5),
        ("APPLICATION/XML", mt.ApplicationXML),
        ("  TEXT/csv  ", mt.TextCSV),
        ("application/json; charset=utf-8", mt.ApplicationJSON),
        ("application/atom+xml; charset=latin1", mt.ApplicationAtom),
        ("application/vnd.myapp.type+xml", mt.ApplicationXML),
        ("text/atom+xml", mt.ApplicationAtom),
        ("application/csv", mt.TextCSV),
        ("text/vnd.myapp.type+xml", mt.ApplicationXML),
    ],
)
def test_parse(mimetype, expected):
    """
    Test that valid mimetypes are parsed into the correct enums.
    """

    assert mt.parse(mimetype) == expected


@pytest.mark.parametrize(
    "mimetype",
    [
        (None),
        (99999),
        ("text/svg+png"),
        ("image/png"),
        ("application/vnd.myapp.type"),
    ],
)
def test_parse_invalid(mimetype):
    """
    Test that invalid mimetypes raise a ValueError.
    """

    with pytest.raises(ValueError):
        mt.parse(mimetype)


def test_prefixes():
    """
    Test that prefix constants are spelled correctly.
    """

    prefixes = ["application", "text", "user"]
    for _, name in mt.names_by_value.items():
        parts = name.split("/")
        assert len(parts) == 2
        assert parts[0] in prefixes


@pytest.mark.parametrize(
    "mimetype,expected",
    [
        (
            "application/json",
            {"prefix": "application", "mime": "json", "ext": None, "pairs": None},
        ),
        (
            "application/ld+json",
            {"prefix": "application", "mime": "ld", "ext": "json", "pairs": None},
        ),
        (
            "application/json; charset=utf8",
            {
                "prefix": "application",
                "mime": "json",
                "pairs": "; charset=utf8",
                "ext": None,
            },
        ),
        (
            "application/protobuf; msg=trisa.v1beta1.SecureEnvelope counterparty=trisa.example.com",
            {
                "prefix": "application",
                "mime": "protobuf",
                "pairs": "; msg=trisa.v1beta1.SecureEnvelope counterparty=trisa.example.com",
                "ext": None,
            },
        ),
        (
            "application/atom+xml; charset=utf-8",
            {
                "prefix": "application",
                "mime": "atom",
                "ext": "xml",
                "pairs": "; charset=utf-8",
            },
        ),
        (
            "application/vnd.google.protobuf",
            {
                "prefix": "application",
                "mime": "vnd.google.protobuf",
                "ext": None,
                "pairs": None,
            },
        ),
        (
            "application/vnd.myapp.type+xml",
            {
                "prefix": "application",
                "mime": "vnd.myapp.type",
                "ext": "xml",
                "pairs": None,
            },
        ),
        (
            "application/x-protobuf",
            {"prefix": "application", "mime": "x-protobuf", "ext": None, "pairs": None},
        ),
        ("text/plain", {"prefix": "text", "mime": "plain", "ext": None, "pairs": None}),
        (
            "text/html; charset=utf8",
            {"prefix": "text", "mime": "html", "pairs": "; charset=utf8", "ext": None},
        ),
        (
            "text/tsv+csv",
            {"prefix": "text", "mime": "tsv", "ext": "csv", "pairs": None},
        ),
        (
            "text/tsv+csv; charset=utf8",
            {"prefix": "text", "mime": "tsv", "ext": "csv", "pairs": "; charset=utf8"},
        ),
        (
            "user/format-0",
            {"prefix": "user", "mime": "format-0", "ext": None, "pairs": None},
        ),
        (
            "user/format-853",
            {"prefix": "user", "mime": "format-853", "ext": None, "pairs": None},
        ),
    ],
)
def test_parse_str(mimetype, expected):
    """
    Test parsing a mimetype into its components.
    """

    assert mt._parse_str(mimetype) == expected
