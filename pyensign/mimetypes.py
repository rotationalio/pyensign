import re

from pyensign.mimetype.v1beta1 import mimetype_pb2 as mt

# Expose more readable mimetype aliases for developers.
Unspecified = mt.UNSPECIFIED
Unknown = mt.UNKNOWN
TextPlain = mt.TEXT_PLAIN
TextCSV = mt.TEXT_CSV
TextHTML = mt.TEXT_HTML
TextCalendar = mt.TEXT_CALENDAR
ApplicationOctetStream = mt.APPLICATION_OCTET_STREAM
ApplicationJSON = mt.APPLICATION_JSON
ApplicationJSONUTF8 = mt.APPLICATION_JSON_UTF8
ApplicationJSONLD = mt.APPLICATION_JSON_LD
AppplicationJSONLines = mt.APPLICATION_JSON_LINES
ApplicationUBJSON = mt.APPLICATION_UBJSON
ApplicationBSON = mt.APPLICATION_BSON
ApplicationXML = mt.APPLICATION_XML
ApplicationAtom = mt.APPLICATION_ATOM
ApplicationMsgPack = mt.APPLICATION_MSGPACK
ApplicationParquet = mt.APPLICATION_PARQUET
ApplicationAvro = mt.APPLICATION_AVRO
ApplicationProtobuf = mt.APPLICATION_PROTOBUF
ApplicationPDF = mt.APPLICATION_PDF
ApplicationJavaArchive = mt.APPLICATION_JAVA_ARCHIVE
ApplicationPythonPickle = mt.APPLICATION_PYTHON_PICKLE
UserSpecified0 = mt.USER_SPECIFIED0
UserSpecified1 = mt.USER_SPECIFIED1
UserSpecified2 = mt.USER_SPECIFIED2
UserSpecified3 = mt.USER_SPECIFIED3
UserSpecified4 = mt.USER_SPECIFIED4
UserSpecified5 = mt.USER_SPECIFIED5
UserSpecified6 = mt.USER_SPECIFIED6
UserSpecified7 = mt.USER_SPECIFIED7
UserSpecified8 = mt.USER_SPECIFIED8
UserSpecified9 = mt.USER_SPECIFIED9

# Map the mimetype integer value to the string defined by the IETF spec.
# https://www.iana.org/assignments/media-types/media-types.xhtml
names_by_value = {
    0: "application/octet-stream",
    1: "text/plain",
    2: "text/csv",
    3: "text/html",
    4: "text/calendar",
    50: "application/json",
    51: "application/ld+json",
    52: "application/jsonlines",
    53: "application/ubjson",
    54: "application/bson",
    100: "application/xml",
    101: "application/atom+xml",
    252: "application/msgpack",
    253: "application/parquet",
    254: "application/avro",
    255: "application/protobuf",
    512: "application/pdf",
    513: "application/java-archive",
    514: "application/python-pickle",
    1000: "user/format-0",
    1001: "user/format-1",
    1002: "user/format-2",
    1003: "user/format-3",
    1004: "user/format-4",
    1005: "user/format-5",
    1006: "user/format-6",
    1007: "user/format-7",
    1008: "user/format-8",
    1009: "user/format-9",
}

# Map the IETF string to the integer value defined by the protocol buffers.
values_by_name = {v: k for k, v in names_by_value.items()}

# Parse a mimetype as a dictionary of its components using the format:
# prefix/mime+ext; key1=value1; key2=value2. Pairs will include the leading semicolon
# and whitespace between keys and values.
_mimetype_pattern = re.compile(
    r"^(?P<prefix>application|user|text)\/(?P<mime>[\w\-_\.]+)(\+(?P<ext>[\w-]+))?(?P<pairs>;(\s+([\w\.\-_]+=[\w\.\-_]+))+)?$"
)


def _parse_str(mimetype):
    match = _mimetype_pattern.search(mimetype)
    if not match:
        raise ValueError("unparseable mimetype: {}".format(mimetype))
    return match.groupdict()


def _build(prefix, mime, ext="", pairs=""):
    """
    Build a mimetype string from its components.
    """

    if ext:
        ext = "+" + ext

    return "{}/{}{}{}".format(prefix, mime, ext, pairs)


def parse(mimetype):
    """
    Parse a mimetype from a string or integer and return a standardized value that
    represents the mimetype. This method uses a best effort approach in order to match
    the given mimetype to an IETF standard mimetype. For example,
    application/vnd.myapp.type+xml will be matched to application/xml.

    Parameters
    ----------
    mimetype : str or int
        The mimetype to parse.

    Returns
    -------
    int
        The protocol buffer representation of the mimetype.

    Raises
    ------
    ValueError
        If the mimetype is not recognized.
    """

    # Just attempt a lookup if the mimetype doesn't need to be parsed.
    if not isinstance(mimetype, str):
        if mimetype in names_by_value:
            return mimetype
        raise ValueError("unrecognized mimetype: {}".format(mimetype))

    # First try to match the mimetype exactly.
    s = mimetype.lower().strip()
    if s in values_by_name:
        return values_by_name[s]

    # Parse the mimetype into its components.
    components = _parse_str(mimetype)

    # Try to match against just the prefix and mime.
    m = _build(components["prefix"], components["mime"])
    if m in values_by_name:
        return values_by_name[m]

    # Try to match against the prefix, mime, and extension.
    m = _build(components["prefix"], components["mime"], ext=components["ext"])
    if m in values_by_name:
        return values_by_name[m]

    # Try to match using the extension for the mime type.
    m = _build(components["prefix"], components["ext"])
    if m in values_by_name:
        return values_by_name[m]

    # Try matching against the alternate prefix.
    prefix = "application" if components["prefix"] == "text" else "text"
    m = _build(prefix, components["mime"])
    if m in values_by_name:
        return values_by_name[m]

    m = _build(prefix, components["mime"], ext=components["ext"])
    if m in values_by_name:
        return values_by_name[m]

    m = _build(prefix, components["ext"])
    if m in values_by_name:
        return values_by_name[m]

    raise ValueError("unrecognized mimetype: {}".format(mimetype))
