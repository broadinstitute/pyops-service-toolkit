"""Update dataset schema data."""
from pydantic import BaseModel
from typing import Optional

from . import Column, Relationship, Table


class NewColumn(BaseModel):
    """New column data.
    
    @private
    """

    tableName: str
    columns: list[Column]


class Changes(BaseModel):
    """Changes data.
    
    @private
    """

    addTables: Optional[list[Table]] = None
    addColumns: Optional[list[NewColumn]] = None
    addRelationships: Optional[list[Relationship]] = None


class UpdateSchema(BaseModel):
    """Update schema data.
    
    @private
    """

    description: str
    changes: Changes
