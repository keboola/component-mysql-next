import ast
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class ColumnSchema:
    """
    Defines the name and type specifications of a single field in a table
    """
    name: str
    source_type: Optional[str] = None
    base_type: str = 'STRING'
    source_type_signature: Optional[str] = None
    description: Optional[str] = ""
    nullable: bool = False
    length: Optional[str] = None
    precision: Optional[str] = None
    default: Optional[str] = None


def is_type_with_length(source_type: str, types_with_length: list[str]):
    # remove length from type
    if '(' in source_type:
        source_type = source_type.split('(')[0]

    for t in types_with_length:
        if source_type.upper() in t.upper():
            return True
    return False


def column_metadata_to_schema(col_name: str, column_metadata: List[dict], types_with_length: list[str]):
    schema = ColumnSchema(col_name)
    for md in column_metadata:
        try:
            if md['key'] == 'KBC.datatype.type':
                schema.source_type = md['value']

                if is_type_with_length(md['value'], types_with_length):
                    size = ()
                    if len(split_parts := md['value'].split('(')) > 1:
                        # remove anything after ) e.g. int(12) unsigned)
                        size_str = split_parts[1].split(')')[0]
                        size = ast.literal_eval(f'({size_str})')

                    if size and isinstance(size, tuple):
                        schema.length = size[0]
                        schema.precision = size[1]
                    elif size:
                        schema.length = size

            if md['key'] == 'KBC.datatype.basetype':
                schema.base_type = md['value']
        except Exception as e:
            raise Exception(f'Failed to convert column datatype - {e}. Column: {col_name}, Metadata: {md}') from e

    return schema
