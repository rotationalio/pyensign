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

    @pytest.mark.parametrize(
        "ids, expected",
        [
            (
                [
                    RLID(
                        bytearray(
                            [0x08, 0x12, 0xF5, 0x59, 0x12, 0xA2, 0x3B, 0xFE, 0x01, 0x98]
                        )
                    ),
                    RLID(
                        bytearray(
                            [0x07, 0x11, 0xF4, 0x58, 0x11, 0xA1, 0x3A, 0xFD, 0x00, 0x97]
                        )
                    ),
                    RLID(
                        bytearray(
                            [0x18, 0x22, 0xF6, 0x69, 0x22, 0xB2, 0x4B, 0xFF, 0x11, 0xF0]
                        )
                    ),
                ],
                [
                    RLID(
                        bytearray(
                            [0x07, 0x11, 0xF4, 0x58, 0x11, 0xA1, 0x3A, 0xFD, 0x00, 0x97]
                        )
                    ),
                    RLID(
                        bytearray(
                            [0x08, 0x12, 0xF5, 0x59, 0x12, 0xA2, 0x3B, 0xFE, 0x01, 0x98]
                        )
                    ),
                    RLID(
                        bytearray(
                            [0x18, 0x22, 0xF6, 0x69, 0x22, 0xB2, 0x4B, 0xFF, 0x11, 0xF0]
                        )
                    ),
                ],
            ),
        ],
    )
    def test_sort(self, ids, expected):
        """
        RLIDs should be sortable.
        """
        assert sorted(ids) == expected
