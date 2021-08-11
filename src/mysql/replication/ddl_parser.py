import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional

import sqlparse
from sqlparse.sql import Identifier, Statement, Token, IdentifierList, TokenList

TABLE_NAME_INDEX = 4

FIRST_KEYWORD_INDEX = 8


class TableChangeType(Enum):
    DROP_COLUMN = auto()
    ADD_COLUMN = auto()


@dataclass
class TableSchemaChange:
    type: TableChangeType
    table_name: str
    schema: Optional[str]
    column_name: str
    after_column: str = None
    first_position: bool = False
    data_type: str = None
    collation: str = None
    column_key: str = None


class AlterStatementParser:
    """
    Parse ALTER statements.

    - Case sensitive.
    - Expects valid queries.
    - Multi statements are not supported. Apart from multi statement including USE {schema}; at the beginning.
    """
    # Supported statements - match patterns based on position
    SUPPORTED_ALTER_TABLE_STATEMENTS = ['ALTER TABLE {table_name} DROP COLUMN {col_name}',
                                        'ALTER TABLE {table_name} DROP {col_name}',
                                        'ALTER TABLE {table_name} ADD COLUMN {col_name}',
                                        'ALTER TABLE {table_name} ADD {col_name}']

    # minimal size of a query (ALTER TABLE xx XXX SOMETHING)
    MINIMAL_TOKEN_COUNT = 9

    @staticmethod
    def _is_matching_pattern(statement: Statement, pattern: str):
        match = True
        for idx, value in enumerate(re.split(r'(\s+)', pattern)):
            if value.startswith('{'):
                continue
            if statement.tokens[idx].normalized != value:
                match = False
                break
        return match

    def _is_supported_alter_table_statement(self, normalized_statement):
        if not (normalized_statement.get_type() == 'ALTER' and len(
                normalized_statement.tokens) > self.MINIMAL_TOKEN_COUNT
                and normalized_statement.tokens[2].value == 'TABLE'):
            return False
        else:
            for pattern in self.SUPPORTED_ALTER_TABLE_STATEMENTS:
                match = self._is_matching_pattern(statement=normalized_statement, pattern=pattern)
                if match:
                    return True

            return False

    @staticmethod
    def __is_column_identifier(token: Token) -> bool:
        return isinstance(token, Identifier) or (
                token.ttype == sqlparse.tokens.Name and isinstance(token.parent, Identifier))

    @staticmethod
    def __ungroup_identifier_lists(statement: Statement):
        """
        Dirty fix of a sqlparser bug that falsely groups statements like (FIRST, ADD) in
        ADD COLUMN email VARCHAR(100) NOT NULL FIRST, ADD
        """
        tokens = []
        for t in statement:

            if isinstance(t, IdentifierList):
                tokens.extend(t.flatten())
            else:
                tokens.append(t)
        return TokenList(tokens)

    @staticmethod
    def _is_column_keyword(statement: Token):
        return statement.ttype == sqlparse.tokens.Keyword and statement.normalized == 'COLUMN'

    def _normalize_identifier(self, identifier: str):
        """
        Remove quotes
        """
        return identifier.replace('`', '')

    def _get_table_name(self, statement: Statement):
        schema = ''
        table_name = statement.tokens[TABLE_NAME_INDEX].normalized
        split = table_name.split('.')
        if len(split) > 1:
            table_name = split[1]
            schema = split[0]
        return self._normalize_identifier(schema), self._normalize_identifier(table_name)

    def _get_element_next_to_position(self, statement: TokenList, position):
        return statement.token_next(position, skip_cm=True)[1].normalized

    def _process_drop_event(self, table_name, schema, statement: Statement) -> List[TableSchemaChange]:
        schema_changes = []
        for idx, value in enumerate(statement.tokens[FIRST_KEYWORD_INDEX:], start=FIRST_KEYWORD_INDEX):
            if self._is_column_keyword(value):
                continue
            if isinstance(value, Identifier):
                column_name = value.normalized
                schema_changes.append(TableSchemaChange(TableChangeType.DROP_COLUMN, table_name, schema, column_name))
        return schema_changes

    def _process_add_event(self, table_name, schema, statement: Statement) -> List[TableSchemaChange]:
        # because some statements including FIRST were invalidly parsed as identifier groups
        # happens when tokens of type Keyword are separated by comma
        flattened_tokens = self.__ungroup_identifier_lists(statement)
        schema_changes = []
        schema_change = None
        is_after_statement = False
        token_count = len(flattened_tokens.tokens)
        for idx, value in enumerate(flattened_tokens.tokens[FIRST_KEYWORD_INDEX:], start=FIRST_KEYWORD_INDEX):
            if self._is_column_keyword(value):
                continue



            # capture new col name
            elif self.__is_column_identifier(value) and not is_after_statement:
                column_name = value.normalized
                # next one is always datatype
                data_type = self._get_element_next_to_position(flattened_tokens, idx).upper()
                schema_change = TableSchemaChange(TableChangeType.ADD_COLUMN, table_name, schema, column_name,
                                                  data_type=data_type)

            # AFTER statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'AFTER':
                # next one is always column name
                is_after_statement = True
                schema_change.after_column = self._get_element_next_to_position(flattened_tokens, idx)

            # COLLATE  statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'COLLATE':
                # next one is always column name
                schema_change.collation = self._get_element_next_to_position(flattened_tokens, idx)

            # PRIMARY KEY  statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'PRIMARY' \
                    and self._get_element_next_to_position(flattened_tokens, idx) == 'KEY':
                # next one is always column name
                schema_change.column_key = 'PRI'

            # process FIRST statement
            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'FIRST':
                schema_change.first_position = True

            # is at the end of multiline statement or end of the query
            elif (value.ttype == sqlparse.tokens.Punctuation and value.normalized == ',') or idx == token_count - 1:
                schema_changes.append(schema_change)

                # reset
                is_after_statement = False

        return schema_changes

    def _get_schema_from_use_statement(self, statement: Statement):
        schema = self._get_element_next_to_position(statement, 0)
        return self._normalize_identifier(schema)

    def _extract_alter_statement_and_schema(self, normalized_statements: Statement):
        use_schema = ''
        normalized_statement = ''
        for statement in normalized_statements:
            first_token = statement.token_first(skip_cm=True)
            if first_token.normalized == 'ALTER':
                normalized_statement = statement
            elif first_token.normalized == 'USE':
                use_schema = self._get_schema_from_use_statement(statement)
        return use_schema, normalized_statement

    def get_table_changes(self, sql: str, schema: str) -> List[TableSchemaChange]:
        normalized_statements = sqlparse.parse(sqlparse.format(sql, strip_comments=True, reindent_aligned=True))
        use_schema, normalized_statement = self._extract_alter_statement_and_schema(normalized_statements)

        # normalized / formatted by now, should be safe to use fixed index
        if not normalized_statement or not self._is_supported_alter_table_statement(normalized_statement):
            return []

        query_schema, table_name = self._get_table_name(normalized_statement)

        schema_name = schema or query_schema or use_schema

        type = normalized_statement.tokens[6].normalized
        table_changes = []
        if type == 'DROP':
            table_changes.extend(self._process_drop_event(table_name, schema_name, normalized_statement))
        elif type == 'ADD':
            table_changes.extend(self._process_add_event(table_name, schema_name, normalized_statement))
        return table_changes
