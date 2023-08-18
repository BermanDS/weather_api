from pydantic import BaseModel
from typing import Optional, List


class WeatherRequestHistory(BaseModel):

    city: str
    dates: str
    country: Optional[str] = None
    
    @property
    def get_dict(self) -> dict:
        
        return {
            'geo_name':self.city,
            'geo_country': self.country,
            'date_range':self.dates.split(','),
            'dates': "','".join(self.dates.split(',')),
        }


class WeatherRequestForecast(BaseModel):

    city: str
    days: Optional[int] = 3
    country: Optional[str] = None

    @property
    def get_dict(self) -> dict:
        
        return {
            'geo_name': self.city,
            'geo_country': self.country,
            'days': min(3,self.days) if self.days > 0 else 3,
        }


class WeatherResponse(BaseModel):
    
    current_time: str
    metainfo: dict
    data: list
    
