import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import List

import sqlparse
from sqlparse.sql import Identifier, Statement, Token, IdentifierList

add_single = """ use `cdc`; /* ApplicationName=DataGrip 2021.1.3 */ ALTER TABLE cdc.customers_binary
    ADD COLUMN tests_col5 VARCHAR(255)"""

drop_multi = """ /* some commmennts
  aaa */ ALTER   TABLE      TableName
    DROP ColuMN Column1,
    DROP COLUMN Column2,
    DROP column_3;"""

add_multi = """ /* some commmennts
  aaa */ ALTER   TABLE      TableName
    ADD COLUMN email VARCHAR(100) NOT NULL FIRST,
ADD COLUMN hourly_rate decimal(10,2) NOT NULL AFTER some_col;"""


class TableChangeType(Enum):
    DROP_COLUMN = auto()
    ADD_COLUMN = auto()


@dataclass
class TableSchemaChange:
    type: TableChangeType
    table_name: str
    schema: str
    column_name: str
    after_column: str = None
    first_position: bool = False


class AlterStatementParser:
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
        return tokens

    def _normalize_identifier(self, identifier: str):
        """
        Remove quotes
        """
        return identifier.replace('`', '')

    def _get_table_name(self, statement: Statement):
        schema = None
        table_name = statement.tokens[4].normalized
        split = table_name.split('.')
        if len(split) > 0:
            table_name = split[1]
            schema = split[0]
        return self._normalize_identifier(schema), self._normalize_identifier(table_name)

    @staticmethod
    def _process_drop_event(table_name, schema, statement: Statement) -> List[TableSchemaChange]:
        schema_changes = []
        for idx, value in enumerate(statement.tokens[8:], start=8):
            if value.ttype == sqlparse.tokens.Keyword and value.normalized == 'COLUMN':
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
        token_count = len(flattened_tokens)
        for idx, value in enumerate(flattened_tokens[8:], start=8):
            if value.ttype == sqlparse.tokens.Keyword and value.normalized == 'COLUMN':
                continue

            elif value.ttype == sqlparse.tokens.Keyword and value.normalized == 'AFTER':
                is_after_statement = True

            # capture new col name
            elif self.__is_column_identifier(value) and not is_after_statement:
                column_name = value.normalized
                schema_change = TableSchemaChange(TableChangeType.ADD_COLUMN, table_name, schema, column_name)

            # process AFTER statement
            elif self.__is_column_identifier(value) and is_after_statement:
                schema_change.after_column = value.normalized

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
        schema = statement.token_next(0, skip_cm=True)[1].value
        return self._normalize_identifier(schema)

    def _extract_alter_statement_and_schema(self, normalized_statements: Statement):
        use_schema = None
        normalized_statement = ''
        for statement in normalized_statements:
            first_token = statement.token_first(skip_cm=True)
            if first_token.normalized == 'ALTER':
                normalized_statement = statement
            elif first_token.normalized == 'USE':
                use_schema = self._get_schema_from_use_statement(statement)
        return use_schema, normalized_statement

    def get_table_changes(self, sql: str):
        normalized_statements = sqlparse.parse(sqlparse.format(sql, strip_comments=True, reindent_aligned=True))
        use_schema, normalized_statement = self._extract_alter_statement_and_schema(normalized_statements)

        # normalized / formatted by now, should be safe to use fixed index
        if not normalized_statement or not self._is_supported_alter_table_statement(normalized_statement):
            return []

        schema, table_name = self._get_table_name(normalized_statement)

        if not schema:
            schema = use_schema

        type = normalized_statement.tokens[6].normalized
        table_changes = []
        if type == 'DROP':
            table_changes.extend(self._process_drop_event(table_name, schema, normalized_statement))
        elif type == 'ADD':
            table_changes.extend(self._process_add_event(table_name, schema, normalized_statement))
        return table_changes


parser = AlterStatementParser()

table_changes = parser.get_table_changes(add_single)
table_changes = parser.get_table_changes(drop_multi)
table_changes = parser.get_table_changes(add_multi)
