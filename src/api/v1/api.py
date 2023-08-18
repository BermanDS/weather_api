from fastapi import APIRouter

from .routes.geo import router as geo_router

router = APIRouter()

router.include_router(geo_router, prefix="/weather")