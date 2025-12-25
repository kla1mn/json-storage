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
