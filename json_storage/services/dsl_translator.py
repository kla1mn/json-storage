from collections import defaultdict
from dataclasses import dataclass

from .jsonpath_parser import JSONPathParser, PathSegment


@dataclass(frozen=True)
class EsPath:
    field: str
    is_nested: bool = False
    nested_path: str | None = None


class DSLTranslator:
    @staticmethod
    def to_es_path(segments: list[PathSegment]) -> EsPath:
        """
        $.user.status             -> field="user.status", is_nested=False
        $.tags[*]                 -> field="tags",        is_nested=False
        $.items[*].productId      -> field="items.productId",
                                     is_nested=True, nested_path="items"
        $.order.items[*].price    -> field="order.items.price",
                                     is_nested=True, nested_path="order.items"
        """
        if not segments:
            raise ValueError("Empty segments list is not a valid field path")

        names = [s.name for s in segments]
        field = ".".join(names)

        array_first = next((i for i, s in enumerate(segments) if s.is_array), None)

        if array_first is None:
            return EsPath(field=field, is_nested=False, nested_path=None)

        if len(segments) == 1:
            return EsPath(field=field, is_nested=False, nested_path=None)

        nested_path = ".".join(names[: array_first + 1])
        return EsPath(field=field, is_nested=True, nested_path=nested_path)


    @staticmethod
    def schema_to_es_mapping(search_schema: dict[str, str]) -> dict:
        properties: dict = {}
        nested_props: dict[str, dict] = defaultdict(lambda: {"type": "nested", "properties": {}})

        for logical_name, json_path in search_schema.items():
            segments: list[PathSegment] = JSONPathParser.parse_json_path(json_path)
            es_path: EsPath = DSLTranslator.to_es_path(segments)

            if es_path.is_nested:
                nested_path = es_path.nested_path
                inner_name = es_path.field[len(nested_path) + 1:]  # после "items."
                nested_props[nested_path]["properties"][inner_name] = {"type": "keyword"}
            else:
                properties[es_path.field] = {"type": "keyword"}

        for nested_path, nested_def in nested_props.items():
            properties[nested_path] = nested_def

        return {
            "mappings": {
                "properties": properties
            }
        }
