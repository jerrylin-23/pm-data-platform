import pytest

from pmplatform.model import decimal_text, iso_ns, parse_ns, scaled


@pytest.mark.parametrize("text,scale,expected", [("219.217767", 6, 219217767), (".48", 4, 4800),
                                                ("-54.00", 6, -54000000), ("0.1200", 4, 1200)])
def test_exact_decimal(text, scale, expected):
    assert scaled(text, scale) == expected
    assert scaled(decimal_text(expected, scale), scale) == expected


@pytest.mark.parametrize("text", ["", "+", ".", "1e-3", "NaN", "0.00001", 0.1, True])
def test_bad_decimal(text):
    with pytest.raises(ValueError):
        scaled(text, 4)


def test_ns_roundtrip():
    ns = 1782753357257123456
    assert parse_ns(iso_ns(ns)) == ns
