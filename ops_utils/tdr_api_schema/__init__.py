"""
Constants and default values for interacting with TDR API.

@private
"""
from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional


class DataTypeEnum(str, Enum):
    """TDR data types.
    
    @private
    """

    string = "string"
    boolean = "boolean"
    bytes = "bytes"
    date = "date"
    datetime = "datetime"
    dirref = "dirref"
    fileref = "fileref"
    float = "float"
    float64 = "float64"
    integer = "integer"
    int64 = "int64"
    numeric = "numeric"
    record = "record"
    text = "text"
    time = "time"
    timestamp = "timestamp"


class Column(BaseModel):
    """TDR Colum schema.
    
    @private
    """

    name: str = Field(min_length=1, max_length=63)
    datatype: DataTypeEnum
    array_of: Optional[bool] = None
    required: Optional[bool] = None


class RelationshipTerm(BaseModel):
    """TDR Relathionship term schema.

    @private
    """

    table: str = Field(min_length=1, max_length=63)
    column: str = Field(min_length=1, max_length=63)


class Relationship(BaseModel):
    """TDR Relationship schema.
    
    @private
    """

    name: str = Field(min_length=1)
    from_table: RelationshipTerm = Field(alias="from")
    to: RelationshipTerm


class PartitionModeEnum(str, Enum):
    """TDR partition modes.
    
    @private
    """

    none = "none"
    date = "date"
    int = "int"


class DatePartition(BaseModel):
    """TDR date partition schema.
    
    @private
    """

    column: str = Field(min_length=1, max_length=63)


class IntPartition(BaseModel):
    """TDR Int partition schema.
    
    @private    
    """

    column: str = Field(min_length=1, max_length=63)
    min: int
    max: int
    interval: int


class Table(BaseModel):
    """TDR table schema.
    
    @private
    """
    
    name: str = Field(max_length=63, min_length=1)
    columns: list[Column]
    primaryKey: Optional[list[str]] = None
    partitionMode: Optional[PartitionModeEnum] = None
    datePartitionOptions: Optional[DatePartition] = None
    intPartitionOptions: Optional[IntPartition] = None
    row_count: Optional[int] = None
