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


def column_metadata_to_schema(col_name: str, column_metadata: List[dict]):
    schema = ColumnSchema(col_name)

    for md in column_metadata:
        try:
            if md['key'] == 'KBC.datatype.type':
                schema.source_type = md['value']
                size = ()
                if len(split_parts := md['value'].split('(')) > 1:
                    size = ast.literal_eval(f'({split_parts[1]}')

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
