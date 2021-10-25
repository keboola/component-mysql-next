"""
Created on 12. 11. 2018

@author: esner
"""
import unittest

import sqlparse

from mysql.replication.ddl_parser import AlterStatementParser, TableSchemaChange, TableChangeType


class TestComponent(unittest.TestCase):

    def normalize_sql(self, sql):
        normalized = sqlparse.parse(sqlparse.format(sql, strip_comments=True, reindent_aligned=True))
        use_schema, normalized_statement = self.parser._extract_alter_statement_and_schema(normalized)
        return normalized_statement.normalized

    def setUp(self) -> None:
        self.parser = AlterStatementParser()

    def test_multi_add_statement_w_comments(self):
        add_multi = """ /* some commmennts
          aaa */ ALTER   TABLE      TableName
            ADD COLUMN email VARCHAR(100) CHARACTER SET utf8 NOT NULL FIRST,
        ADD COLUMN hourly_rate char NOT NULL AFTER some_col;"""

        normalized = self.normalize_sql(add_multi)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='email',
                                    first_position=True,
                                    data_type='VARCHAR(100)',
                                    charset_name='utf8',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='hourly_rate',
                                    after_column='some_col',
                                    data_type='CHAR',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_multi, 'cdc')

        self.assertEqual([change1, change2], table_changes)

    def test_multi_add_statement_w_comments_lowercase(self):
        add_multi = """ /* some commmennts
          aaa */ ALTER   TaBLe      TableName
            AdD COLuMN email VarChar(100) character set utf8 not null first,
        add column hourly_rate char not null after some_col;"""

        normalized = self.normalize_sql(add_multi)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='email',
                                    first_position=True,
                                    data_type='VARCHAR(100)',
                                    charset_name='utf8',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='hourly_rate',
                                    after_column='some_col',
                                    data_type='CHAR',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_multi, 'cdc')

        self.assertEqual([change1, change2], table_changes)

    def test_multi_add_statement_w_additional_params(self):
        add_multi = """ALTER TABLE employee_settings ADD zenefits_id INT DEFAULT NULL, ADD paylocity_id VARCHAR(255) 
        DEFAULT NULL, ALGORITHM=INPLACE, LOCK=NONE"""

        normalized = self.normalize_sql(add_multi)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='employee_settings',
                                    schema='cdc',
                                    column_name='zenefits_id',
                                    data_type='INT',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='employee_settings',
                                    schema='cdc',
                                    column_name='paylocity_id',
                                    data_type='VARCHAR(255)',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_multi, 'cdc')

        self.assertEqual([change1, change2], table_changes)

    def test_multi_add_statement_w_additional_params2(self):
        add_multi = """ALTER TABLE employee_settings ADD zenefits_id INT DEFAULT NULL, ALGORITHM=INPLACE, LOCK=NONE,
         ADD paylocity_id VARCHAR(255) DEFAULT NULL"""

        normalized = self.normalize_sql(add_multi)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='employee_settings',
                                    schema='cdc',
                                    column_name='zenefits_id',
                                    data_type='INT',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='employee_settings',
                                    schema='cdc',
                                    column_name='paylocity_id',
                                    data_type='VARCHAR(255)',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_multi, 'cdc')

        self.assertEqual([change1, change2], table_changes)

    def test_multi_drop_statement_w_additional_params(self):
        drop_multi = """ALTER   TABLE      TableName
            DROP ColuMN Column1,
            DROP COLUMN Column2,
            DROP column_3, ALGORITHM=INPLACE, LOCK=NONE;"""

        normalized = self.normalize_sql(drop_multi)

        change1 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='Column1',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='Column2', query=normalized)
        change3 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='column_3',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(drop_multi, 'cdc')

        self.assertEqual([change1, change2, change3], table_changes)

    def test_multi_drop_statement_w_additional_params2(self):
        drop_multi = """ALTER   TABLE      TableName
            DROP ColuMN Column1, ALGORITHM=INPLACE, LOCK=NONE,
            DROP COLUMN Column2,
            DROP column_3;"""

        normalized = self.normalize_sql(drop_multi)

        change1 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='Column1',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='Column2',
                                    query=normalized)
        change3 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='column_3',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(drop_multi, 'cdc')

        self.assertEqual([change1, change2, change3], table_changes)

    def test_multi_drop_statement_w_comments(self):
        drop_multi = """ /* some commmennts
          aaa */ ALTER   TABLE      TableName
            DROP ColuMN Column1,
            DROP COLUMN Column2,
            DROP column_3;"""
        normalized = self.normalize_sql(drop_multi)

        change1 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='Column1',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='Column2',
                                    query=normalized)
        change3 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='column_3',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(drop_multi, 'cdc')

        self.assertEqual([change1, change2, change3], table_changes)

    def test_single_add_statement_w_comments_use_schema(self):
        add_single = """ use `cdc`; /* ApplicationName=DataGrip 2021.1.3 */ ALTER TABLE customers_binary
            ADD COLUMN tests_col5 VARCHAR(255)"""
        normalized = self.normalize_sql(add_single)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='customers_binary',
                                    schema='cdc',
                                    column_name='tests_col5',
                                    data_type='VARCHAR(255)',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_single, '')

        self.assertEqual([change1], table_changes)

    def test_single_drop_statement_w_comments_use_schema(self):
        add_single = """ use `cdc`; /* ApplicationName=DataGrip 2021.1.3 */ ALTER TABLE customers_binary
            DROP COLUMN tests_col5"""
        normalized = self.normalize_sql(add_single)

        change1 = TableSchemaChange(TableChangeType.DROP_COLUMN,
                                    table_name='customers_binary',
                                    schema='cdc',
                                    column_name='tests_col5',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_single, '')

        self.assertEqual([change1], table_changes)

    def test_single_add_with_charset(self):
        add_single = """/* ApplicationName=DataGrip 2021.1.3 */ ALTER TABLE cdc.`customers_binary`
    ADD COLUMN charset_col VARCHAR(255) CHARACTER SET utf8 FIRST"""
        normalized = self.normalize_sql(add_single)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='customers_binary',
                                    schema='cdc',
                                    column_name='charset_col',
                                    data_type='VARCHAR(255)',
                                    charset_name='utf8',
                                    first_position=True,
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_single, 'cdc')

        self.assertEqual([change1], table_changes)

    def test_single_add_with_idenitifier_quotes(self):
        add_single = """/* ApplicationName=DataGrip 2021.1.3 */ ALTER TABLE cdc.`customers_binary`
    ADD COLUMN `charset_col` VARCHAR(255) CHARACTER SET utf8 FIRST"""
        normalized = self.normalize_sql(add_single)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='customers_binary',
                                    schema='cdc',
                                    column_name='charset_col',
                                    data_type='VARCHAR(255)',
                                    charset_name='utf8',
                                    first_position=True,
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_single, 'cdc')

        self.assertEqual([change1], table_changes)

    def test_multi_add_statement_w_comments_quotes(self):
        add_multi = """ /* some commmennts
          aaa */ ALTER   TABLE      `cdc`.`TableName`
            ADD COLUMN email VARCHAR(100) NOT NULL FIRST,
        ADD COLUMN hourly_rate decimal(10,2) NOT NULL AFTER some_col;"""
        normalized = self.normalize_sql(add_multi)

        change1 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='email',
                                    first_position=True,
                                    data_type='VARCHAR(100)',
                                    query=normalized)
        change2 = TableSchemaChange(TableChangeType.ADD_COLUMN,
                                    table_name='TableName',
                                    schema='cdc',
                                    column_name='hourly_rate',
                                    after_column='some_col',
                                    data_type='DECIMAL(10,2)',
                                    query=normalized)
        table_changes = self.parser.get_table_changes(add_multi, '')

        self.assertEqual([change1, change2], table_changes)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
