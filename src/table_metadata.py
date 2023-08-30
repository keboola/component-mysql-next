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
        if md['key'] == 'KBC.datatype.type':
            schema.source_type = md['value']

        if md['key'] == 'KBC.datatype.basetype':
            schema.base_type = md['value']

        if md['key'] == 'KBC.datatype.length':
            schema.length = md['value']
