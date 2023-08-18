FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-2021-10-01
WORKDIR /src

ENV PYTHONPATH /src:/
ENV MODULE_NAME="src.main"

# copy requirements and install (so that changes to files do not mean rebuild cannot be cached)
## pyrthon env
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip
COPY build/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install structlog

COPY src /src
COPY test /src/test

# needs to be set else Celery gives an error (because docker runs commands inside container as root)
ENV C_FORCE_ROOT=1

# run supervisord
CMD ["python", "main.py"]