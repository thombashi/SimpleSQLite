"""
.. codeauthor:: Tsuyoshi Hombashi <tsuyoshi.hombashi@gmail.com>
"""

from collections import namedtuple

import pytest

from simplesqlite.converter import RecordConvertor


attrs_2 = ["attr_a", "attr_b"]
attrs_3 = ["attr_a", "attr_b", "attr_c"]

NamedTuple2 = namedtuple("NamedTuple2", " ".join(attrs_2))
NamedTuple3 = namedtuple("NamedTuple3", " ".join(attrs_3))


class Test_RecordConvertor_to_record:
    @pytest.mark.parametrize(
        ["attr_names", "value", "expected"],
        [
            [attrs_2, [5, 6], [5, 6]],
            [attrs_2, (5, 6), [5, 6]],
            [attrs_2, {"attr_a": 5, "attr_b": 6}, [5, 6]],
            [attrs_2, {"attr_a": 5, "attr_b": 6, "not_exist_attr": 100}, [5, 6]],
            [attrs_2, {"attr_a": 5}, [5, None]],
            [attrs_2, {"attr_b": 6}, [None, 6]],
            [attrs_2, {}, [None, None]],
            [attrs_2, NamedTuple2(5, 6), [5, 6]],
            [attrs_2, NamedTuple2(5, None), [5, None]],
            [attrs_2, NamedTuple2(None, 6), [None, 6]],
            [attrs_2, NamedTuple2(None, None), [None, None]],
            [attrs_2, NamedTuple3(5, 6, 7), [5, 6]],
            [attrs_3, NamedTuple3(5, 6, 7), [5, 6, 7]],
        ],
    )
    def test_normal(self, attr_names, value, expected):
        assert RecordConvertor.to_record(attr_names, value) == expected

    @pytest.mark.parametrize(
        ["attr_names", "value", "expected"],
        [[None, [5, 6], TypeError], [attrs_2, None, ValueError], [None, None, TypeError]],
    )
    def test_exception(self, attr_names, value, expected):
        with pytest.raises(expected):
            RecordConvertor.to_record(attr_names, value)


class Test_RecordConvertor_to_records:
    @pytest.mark.parametrize(
        ["attr_names", "value", "expected"],
        [
            [
                attrs_2,
                [
                    [1, 2],
                    (3, 4),
                    {"attr_a": 5, "attr_b": 6},
                    {"attr_a": 7, "attr_b": 8, "not_exist_attr": 100},
                    {"attr_a": 9},
                    {"attr_b": 10},
                    {},
                    NamedTuple2(11, None),
                ],
                [[1, 2], [3, 4], [5, 6], [7, 8], [9, None], [None, 10], [None, None], [11, None]],
            ]
        ],
    )
    def test_normal(self, attr_names, value, expected):
        assert RecordConvertor.to_records(attr_names, value) == expected

    @pytest.mark.parametrize(
        ["attr_names", "value", "expected"],
        [[None, [5, 6], TypeError], [attrs_2, None, TypeError], [None, None, TypeError]],
    )
    def test_exception(self, attr_names, value, expected):
        with pytest.raises(expected):
            RecordConvertor.to_records(attr_names, value)

    @pytest.mark.parametrize(
        ["attr_names", "value"],
        [
            [["a", "b"], [[5, 9223372036854775808]]],
            [["a", "b"], [{"a": -9223372036854775809, "b": 0}]],
        ],
    )
    def test_exception_value(self, attr_names, value):
        with pytest.raises(OverflowError):
            RecordConvertor.to_records(attr_names, value)
