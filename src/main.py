import structlog
import uvicorn
from fastapi import FastAPI
from fastapi_utils.timing import add_timing_middleware

from src.api.common import router as common_router
from src.api.v1.api import router as v1_router
from src.settings import configs, main_dirs
from src.logger import *


app = FastAPI(
    tittle="Weather API: history and forecast",
)
app.include_router(common_router, tags=["common"])
app.include_router(v1_router, tags=["v1"])

add_timing_middleware(app, record=logger.debug, prefix="v1", exclude="common")


if __name__ == '__main__':
    log(logger, 'initialization', 'info', \
        f"Start API at entrypoint : {configs['APP__HOST']}:80, with {configs['APP__WORKERS']} workers")
    uvicorn.run("__main__:app", host=configs['APP__HOST'], port=80, workers=configs['APP__WORKERS'])