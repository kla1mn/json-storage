from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from .jsonpath_parser import JSONPathParser, PathSegment


@dataclass(frozen=True)
class EsPath:
    field: str
    is_nested: bool = False
    nested_path: str | None = None


@dataclass(frozen=True)
class Condition:
    path: str
    op: str
    value: Any


@dataclass(frozen=True)
class NotExpr:
    expr: 'Expr'


@dataclass(frozen=True)
class AndExpr:
    left: 'Expr'
    right: 'Expr'


@dataclass(frozen=True)
class OrExpr:
    left: 'Expr'
    right: 'Expr'


Expr = Condition | NotExpr | AndExpr | OrExpr


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
            raise ValueError('Empty segments list is not a valid field path')

        names = [s.name for s in segments]
        field = '.'.join(names)

        array_first = next((i for i, s in enumerate(segments) if s.is_array), None)

        if array_first is None:
            return EsPath(field=field, is_nested=False, nested_path=None)

        if len(segments) == 1:
            return EsPath(field=field, is_nested=False, nested_path=None)

        nested_path = '.'.join(names[: array_first + 1])
        return EsPath(field=field, is_nested=True, nested_path=nested_path)

    @staticmethod
    def schema_to_es_mapping(search_schema: dict[str, str]) -> dict:
        properties: dict = {}
        nested_props: dict[str, dict] = defaultdict(
            lambda: {'type': 'nested', 'properties': {}}
        )

        for logical_name, json_path in search_schema.items():
            segments: list[PathSegment] = JSONPathParser.parse_json_path(json_path)
            es_path: EsPath = DSLTranslator.to_es_path(segments)

            if es_path.is_nested:
                nested_path = es_path.nested_path
                inner_name = es_path.field[len(nested_path) + 1 :]
                nested_props[nested_path]['properties'][inner_name] = {
                    'type': 'keyword'
                }
            else:
                properties[es_path.field] = {'type': 'keyword'}

        for nested_path, nested_def in nested_props.items():
            properties[nested_path] = nested_def

        return {'mappings': {'properties': properties}}

    @staticmethod
    def build_query_from_expression(expr: str) -> dict:
        """
        Принимает строку вида:

            $.status == "paid"
            $.price > 10 && $.price <= 20
            $.items[*].productId == "A1" || $.tags[*] == "hot"

        и возвращает ES-запрос:

            {'query': {'term': {'status': 'paid'}}}
            {'query': {'bool': {'must': [{'range': {'price': {...}}}, {'range': {'price': {...}}}]}}}
            {'query': {'bool': {'minimum_should_match': 1, 'should': [{'nested': {'path': 'items', 'query': {...}}}, {'term': {'tags': 'hot'}}]}}}

        Поддерживается:
        - операторы: ==, !=, >, >=, <, <=
        - логика: &&, ||, !, скобки (...)
        - значения: строки "text", числа 10 / 10.5, true/false
        """
        tokens = DSLTranslator._tokenize(expr)
        ast, pos = DSLTranslator._parse_expression(tokens, 0)
        if pos != len(tokens):
            raise ValueError('Unexpected tokens at end of expression')
        clause = DSLTranslator._expr_to_es(ast)
        return {'query': clause}

    @staticmethod
    def _tokenize(s: str) -> list[tuple[str, Any]]:
        tokens: list[tuple[str, Any]] = []
        i = 0
        n = len(s)

        def peek2(i: int) -> str:
            return s[i : i + 2]

        while i < n:
            ch = s[i]
            if ch.isspace():
                i += 1
                continue

            if ch == '$':
                start = i
                i += 1
                while i < n:
                    if s[i].isspace():
                        break
                    if s[i] in ('(', ')', '!', '>', '<'):
                        break
                    two = peek2(i)
                    if two in ('==', '!=', '>=', '<=', '&&', '||'):
                        break
                    i += 1
                path = s[start:i]
                tokens.append(('PATH', path))
                continue

            two = peek2(i)

            if two == '&&':
                tokens.append(('AND', '&&'))
                i += 2
                continue
            if two == '||':
                tokens.append(('OR', '||'))
                i += 2
                continue

            if two == '!=':
                tokens.append(('OP', '!='))
                i += 2
                continue
            if ch == '!':
                tokens.append(('NOT', '!'))
                i += 1
                continue

            if two == '==':
                tokens.append(('OP', '=='))
                i += 2
                continue
            if two == '>=':
                tokens.append(('OP', '>='))
                i += 2
                continue
            if two == '<=':
                tokens.append(('OP', '<='))
                i += 2
                continue
            if ch == '>':
                tokens.append(('OP', '>'))
                i += 1
                continue
            if ch == '<':
                tokens.append(('OP', '<'))
                i += 1
                continue

            if ch == '(':
                tokens.append(('LPAREN', ch))
                i += 1
                continue
            if ch == ')':
                tokens.append(('RPAREN', ch))
                i += 1
                continue

            if ch == '"':
                j = i + 1
                buf = []
                while j < n and s[j] != '"':
                    buf.append(s[j])
                    j += 1
                if j >= n or s[j] != '"':
                    raise ValueError('Unterminated string literal')
                value = ''.join(buf)
                tokens.append(('STRING', value))
                i = j + 1
                continue

            if ch.isdigit() or (ch == '-' and i + 1 < n and s[i + 1].isdigit()):
                j = i
                has_dot = False
                while j < n and (s[j].isdigit() or s[j] in ('.', 'e', 'E', '+', '-')):
                    if s[j] == '.':
                        has_dot = True
                    j += 1
                num_str = s[i:j]
                try:
                    value = float(num_str) if has_dot else int(num_str)
                except ValueError:
                    raise ValueError(f'Invalid number literal: {num_str!r}')
                tokens.append(('NUMBER', value))
                i = j
                continue

            if ch.isalpha():
                j = i
                while j < n and s[j].isalpha():
                    j += 1
                word = s[i:j]
                if word == 'true':
                    tokens.append(('BOOL', True))
                elif word == 'false':
                    tokens.append(('BOOL', False))
                elif word == 'null':
                    tokens.append(('NULL', None))
                else:
                    raise ValueError(f'Unexpected identifier: {word!r}')
                i = j
                continue

            raise ValueError(f'Unexpected character: {ch!r}')

        return tokens

    @staticmethod
    def _parse_expression(tokens: list[tuple[str, Any]], pos: int) -> tuple[Expr, int]:
        def parse_or(p: int) -> tuple[Expr, int]:
            node, p = parse_and(p)
            while p < len(tokens) and tokens[p][0] == 'OR':
                _, _ = tokens[p]
                p += 1
                right, p = parse_and(p)
                node = OrExpr(left=node, right=right)
            return node, p

        def parse_and(p: int) -> tuple[Expr, int]:
            node, p = parse_unary(p)
            while p < len(tokens) and tokens[p][0] == 'AND':
                _, _ = tokens[p]
                p += 1
                right, p = parse_unary(p)
                node = AndExpr(left=node, right=right)
            return node, p

        def parse_unary(p: int) -> tuple[Expr, int]:
            if p < len(tokens) and tokens[p][0] == 'NOT':
                p += 1
                expr, p = parse_unary(p)
                return NotExpr(expr=expr), p
            return parse_primary(p)

        def parse_primary(p: int) -> tuple[Expr, int]:
            if p < len(tokens) and tokens[p][0] == 'LPAREN':
                p += 1
                node, p = parse_or(p)
                if p >= len(tokens) or tokens[p][0] != 'RPAREN':
                    raise ValueError("Missing ')'")
                p += 1
                return node, p
            return parse_condition(p)

        def parse_condition(p: int) -> tuple[Expr, int]:
            if p >= len(tokens) or tokens[p][0] != 'PATH':
                raise ValueError('Expected JSONPath (token PATH)')
            _, path = tokens[p]
            p += 1

            if p >= len(tokens) or tokens[p][0] != 'OP':
                raise ValueError('Expected comparison operator after path')
            _, op = tokens[p]
            p += 1

            if p >= len(tokens):
                raise ValueError('Expected value after operator')
            ttype, value = tokens[p]
            if ttype not in ('STRING', 'NUMBER', 'BOOL', 'NULL'):
                raise ValueError('Expected literal value')
            p += 1

            if op == '!=':
                inner = Condition(path=path, op='==', value=value)
                return NotExpr(expr=inner), p

            return Condition(path=path, op=op, value=value), p

        node, pos2 = parse_or(pos)
        return node, pos2

    @staticmethod
    def _expr_to_es(expr: Expr) -> dict:
        if isinstance(expr, Condition):
            segments = JSONPathParser.parse_json_path(expr.path)
            es_path = DSLTranslator.to_es_path(segments)

            if expr.op == '==':
                inner: dict[str, Any] = {'term': {es_path.field: expr.value}}
            elif expr.op in ('>', '>=', '<', '<='):
                op_map = {'>': 'gt', '>=': 'gte', '<': 'lt', '<=': 'lte'}
                inner = {'range': {es_path.field: {op_map[expr.op]: expr.value}}}
            else:
                raise ValueError(f'Unsupported operator: {expr.op!r}')

            if es_path.is_nested:
                return {
                    'nested': {
                        'path': es_path.nested_path,
                        'query': inner,
                    }
                }
            return inner

        if isinstance(expr, NotExpr):
            clause = DSLTranslator._expr_to_es(expr.expr)
            return {'bool': {'must_not': [clause]}}

        if isinstance(expr, AndExpr):
            left = DSLTranslator._expr_to_es(expr.left)
            right = DSLTranslator._expr_to_es(expr.right)
            return {'bool': {'must': [left, right]}}

        if isinstance(expr, OrExpr):
            left = DSLTranslator._expr_to_es(expr.left)
            right = DSLTranslator._expr_to_es(expr.right)
            return {
                'bool': {
                    'should': [left, right],
                    'minimum_should_match': 1,
                }
            }

        raise TypeError(f'Unsupported expression node: {expr!r}')
