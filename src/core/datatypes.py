"""
Keboola base data types for manifest files.
"""
BASE_STRING = {'string', 'char', 'enum', 'longtext', 'mediumtext', 'tinytext', 'text', 'year',
               'varchar', 'set', 'json', 'binary', 'varbinary', 'blob', "tinyblob", "blob", "mediumblob", "longblob",
               "time"}
BASE_INTEGER = {'integer', 'tinyint', 'smallint', 'mediumint', 'int', 'bigint'}
BASE_NUMERIC = {'numeric', 'decimal'}
BASE_FLOAT = {'float', 'double'}
BASE_BOOLEAN = {'boolean', 'bit', 'bool'}
BASE_DATE = {'date'}
BASE_TIMESTAMP = {'timestamp', 'datetime'}
