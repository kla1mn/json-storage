import pytest
from json_storage.services.jsonpath_parser import JSONPathParser, PathSegment


def test_root_only():
    assert JSONPathParser.parse_json_path('$') == []


def test_simple_path():
    result = JSONPathParser.parse_json_path('$.foo.bar')
    assert result == [
        PathSegment(name='foo', is_array=False),
        PathSegment(name='bar', is_array=False),
    ]


def test_array_segment():
    result = JSONPathParser.parse_json_path('$.foo[*].bar')
    assert result == [
        PathSegment(name='foo', is_array=True),
        PathSegment(name='bar', is_array=False),
    ]


def test_difficult_path():
    result = JSONPathParser.parse_json_path('$.test1.test2[*].test3.test4[*].test5')
    assert result == [
        PathSegment(name='test1', is_array=False),
        PathSegment(name='test2', is_array=True),
        PathSegment(name='test3', is_array=False),
        PathSegment(name='test4', is_array=True),
        PathSegment(name='test5', is_array=False),
    ]


def test_invalid_no_dollar():
    with pytest.raises(ValueError):
        JSONPathParser.parse_json_path('foo.bar')


def test_invalid_segment():
    with pytest.raises(ValueError):
        JSONPathParser.parse_json_path('$.foo..bar')
