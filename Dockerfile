# Phase-1 pipeline image (no secrets baked)
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps (add as needed for lxml/ssl/timezones)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata curl build-essential \
  && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -m appuser
WORKDIR /app

# Install Python deps
# Assumes requirements.txt exists and is authoritative
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip \
 && pip install -r /app/requirements.txt

# Copy source and install as editable module if pyproject/setup present
COPY . /app
# This is safe even if it's a pure module layout; if no pyproject, it no-ops
RUN python -c "import pathlib, sys; p=pathlib.Path('pyproject.toml'); sys.exit(0 if p.exists() else 0)" && \
    pip install -e .

USER appuser

# Default shell; command is supplied by the host wrapper / compose
ENTRYPOINT ["bash","-lc"]
CMD ["echo 'Supply-Signals image ready. Use scripts/phase1_docker.sh or docker compose.'"]
