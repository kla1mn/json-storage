from json_storage.services.dsl_translator import DSLTranslator, EsPath
from json_storage.services.jsonpath_parser import JSONPathParser


def test_to_es_path_simple():
    segments = JSONPathParser.parse_json_path("$.user.status")
    es_path = DSLTranslator.to_es_path(segments)

    assert es_path == EsPath(
        field="user.status",
        is_nested=False,
        nested_path=None,
    )


def test_to_es_path_array_of_primitives():
    segments = JSONPathParser.parse_json_path("$.tags[*]")
    es_path = DSLTranslator.to_es_path(segments)

    assert es_path == EsPath(
        field="tags",
        is_nested=False,
        nested_path=None,
    )


def test_to_es_path_nested_simple():
    segments = JSONPathParser.parse_json_path("$.items[*].productId")
    es_path = DSLTranslator.to_es_path(segments)

    assert es_path == EsPath(
        field="items.productId",
        is_nested=True,
        nested_path="items",
    )


def test_to_es_path_nested_deep():
    segments = JSONPathParser.parse_json_path("$.order.items[*].price")
    es_path = DSLTranslator.to_es_path(segments)

    assert es_path == EsPath(
        field="order.items.price",
        is_nested=True,
        nested_path="order.items",
    )


def test_to_es_path_empty_segments_raises():
    try:
        DSLTranslator.to_es_path([])
        assert False, "Expected ValueError"
    except ValueError:
        assert True


def test_schema_to_es_mapping_only_simple_fields():
    search_schema = {
        "status": "$.status",
        "userId": "$.user.id",
    }

    mapping = DSLTranslator.schema_to_es_mapping(search_schema)

    assert mapping == {
        "mappings": {
            "properties": {
                "status": {"type": "keyword"},
                "user.id": {"type": "keyword"},
            }
        }
    }


def test_schema_to_es_mapping_simple_and_nested():
    search_schema = {
        "status": "$.status",
        "productId": "$.items[*].productId",
    }

    mapping = DSLTranslator.schema_to_es_mapping(search_schema)

    assert mapping == {
        "mappings": {
            "properties": {
                "status": {"type": "keyword"},
                "items": {
                    "type": "nested",
                    "properties": {
                        "productId": {"type": "keyword"},
                    },
                },
            }
        }
    }


def test_schema_to_es_mapping_array_of_primitives_not_nested():
    search_schema = {
        "tags": "$.tags[*]",
    }

    mapping = DSLTranslator.schema_to_es_mapping(search_schema)

    assert mapping == {
        "mappings": {
            "properties": {
                "tags": {"type": "keyword"},
            }
        }
    }


def test_build_query_simple_eq():
    query = DSLTranslator.build_query_from_expression('$.status == "paid"')

    assert query == {
        "query": {
            "term": {
                "status": "paid",
            }
        }
    }


def test_build_query_numeric_range_and():
    query = DSLTranslator.build_query_from_expression(
        "$.price > 10 && $.price <= 20"
    )

    assert query == {
        "query": {
            "bool": {
                "must": [
                    {"range": {"price": {"gt": 10}}},
                    {"range": {"price": {"lte": 20}}},
                ]
            }
        }
    }


def test_build_query_nested_term():
    query = DSLTranslator.build_query_from_expression(
        '$.items[*].productId == "A1"'
    )

    assert query == {
        "query": {
            "nested": {
                "path": "items",
                "query": {
                    "term": {
                        "items.productId": "A1",
                    }
                },
            }
        }
    }


def test_build_query_or():
    query = DSLTranslator.build_query_from_expression(
        '$.status == "paid" || $.status == "pending"'
    )

    assert query == {
        "query": {
            "bool": {
                "should": [
                    {"term": {"status": "paid"}},
                    {"term": {"status": "pending"}},
                ],
                "minimum_should_match": 1,
            }
        }
    }


def test_build_query_not_with_neq():
    query = DSLTranslator.build_query_from_expression(
        '$.status != "paid"'
    )

    assert query == {
        "query": {
            "bool": {
                "must_not": [
                    {"term": {"status": "paid"}}
                ]
            }
        }
    }


def test_build_query_grouped_and_or():
    query = DSLTranslator.build_query_from_expression(
        '($.price > 10 && $.price <= 20) || $.status == "paid"'
    )

    assert query == {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {"range": {"price": {"gt": 10}}},
                                {"range": {"price": {"lte": 20}}},
                            ]
                        }
                    },
                    {"term": {"status": "paid"}},
                ],
                "minimum_should_match": 1,
            }
        }
    }
