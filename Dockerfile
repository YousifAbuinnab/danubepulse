FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    FIXTURE_DIR=/app/data/fixtures
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations
ARG PACKAGE_SPEC=.
RUN pip install --no-cache-dir "$PACKAGE_SPEC"
COPY dashboard ./dashboard
COPY data/fixtures ./data/fixtures
EXPOSE 8000 8501
CMD ["uvicorn", "danubepulse.main:app", "--host", "0.0.0.0", "--port", "8000"]
