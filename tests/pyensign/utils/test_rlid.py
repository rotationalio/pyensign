import pytest

from pyensign.utils.rlid import RLID


class TestRLID:
    """
    Test creating and encoding RLIDs.
    """

    @pytest.mark.parametrize(
        "bytes, expected",
        [
            (
                bytearray([0x01, 0x83, 0x42, 0x5F, 0x66, 0x6F, 0x00, 0x6F, 0xEB, 0x6B]),
                "061m4qv6dw06ztvb",
            ),
            (
                bytearray([0x42 for _ in range(10)]),
                "89144gj289144gj2",
            ),
        ],
    )
    def test_encode(self, bytes, expected):
        rlid = RLID(bytes)
        assert str(rlid) == expected

    @pytest.mark.parametrize(
        "bytes, exception",
        [
            (
                None,
                TypeError,
            ),
            (
                "bad",
                ValueError,
            ),
            (
                bytearray(),
                ValueError,
            ),
            (
                bytearray([0x01, 0x83, 0x42, 0x5F, 0x66, 0x6F, 0x00, 0x6F, 0xEB]),
                ValueError,
            ),
        ],
    )
    def test_bad_rlid(self, bytes, exception):
        with pytest.raises(exception):
            RLID(bytes)
