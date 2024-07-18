from pydantic import BaseModel, Field, ValidationError, field_validator
from typing import List, Union
from datetime import datetime


class Coordinates(BaseModel):
    latitude:Union[str,float] 
    longitude: Union[str,float]
    altitud : Union[str,float]
    satelites : Union[str,int]
    timestamp : str
    date : str
    
    
# Esquema para enviar los datos en el nuevo formato
class OutputCoordinates(BaseModel):
    latitude: float
    longitude: float
    altitud: float
    satelites: int
    date_time: datetime

    @field_validator('latitude', 'longitude', 'altitud', mode='before')
    def str_to_float(cls, v):
        return float(v)

    @field_validator('satelites', mode='before')
    def str_to_int(cls, v):
        return int(v)

    
    