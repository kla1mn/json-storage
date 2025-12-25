from dataclasses import dataclass
import re


@dataclass(frozen=True)
class PathSegment:
    name: str
    is_array: bool = False


class JSONPathParser:
    def __init__(self):
        super().__init__()

    @staticmethod
    def parse_json_path(json_path: str) -> list[PathSegment]:
        """
        Поддерживаем только:
        - абсолютные пути от корня: $.foo.bar[*].baz
        - сегменты вида: name или name[*]
        - без фильтров, без '..', без [?()], без ['name']
        """

        m = re.fullmatch(r'\$(?:\.(.*))?', json_path.strip())
        if not m:
            raise ValueError("Only absolute JSONPath starting with '$' is supported")

        inner = m.group(1)
        if not inner:
            return []

        segments: list[PathSegment] = []

        token_re = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*)(\[\*\])?$')

        for raw in inner.split('.'):
            raw = raw.strip()
            if not raw:
                raise ValueError(f'Empty path segment in {json_path!r}')

            m = token_re.match(raw)
            if not m:
                raise ValueError(f'Unsupported JSONPath segment: {raw!r}')

            name, array_marker = m.group(1), m.group(2)
            segments.append(PathSegment(name=name, is_array=bool(array_marker)))

        return segments
