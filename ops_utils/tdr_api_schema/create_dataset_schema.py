"""Module to create TDR dataset sechema."""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

from . import Relationship, Table


class AssetTable(BaseModel):
    """TDR asset table schema.
    
    @private
    """

    name: str = Field(min_length=1, max_length=63)
    columns: list[str]


class Asset(BaseModel):
    """TDR asset schema.
    
    @private
    """

    name: str = Field(min_length=1)
    tables: list[AssetTable]
    rootTable: str = Field(min_length=1, max_length=63)
    rootColumn: str = Field(min_length=1, max_length=63)
    follow: list[str]


class Schema(BaseModel):
    """TDR schema.
    
    @private
    """

    tables: list[Table]
    relationships: Optional[list[Relationship]] = None
    assets: Optional[list[Asset]] = None


class Policy(BaseModel):
    """TDR policy schema.
    
    @private
    """

    stewards: str
    custodians: str
    snapshotCreators: str


class CloudPlatformEnum(str, Enum):
    """Cloud platform enum.
    
    @private
    """

    gcp = "gcp"


class CreateDatasetSchema(BaseModel):
    """TDR dataset schema.
    
    @private
    """
    
    name: str = Field(max_length=511, min_length=1)
    description: Optional[str] = None
    defaultProfileId: str
    tdr_schema: Schema = Field(alias="schema")
    region: Optional[str] = None
    cloudPlatform: Optional[CloudPlatformEnum] = CloudPlatformEnum.gcp
    enableSecureMonitoring: Optional[bool] = None
    phsId: Optional[str] = None
    experimentalSelfHosted: Optional[bool] = None
    properties: Optional[dict] = None
    dedicatedIngestServiceAccount: Optional[bool] = None
    experimentalPredictableFileIds: Optional[bool] = None
    policies: Optional[Policy] = None
    tags: Optional[list[str]] = None
