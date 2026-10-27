# syntax=docker/dockerfile:1
FROM python@sha256:b3575dcf816cdacc5292df77a6e3e59b8941631078373a045efa2ac1e9c5c3b2

RUN --mount=type=secret,id=ca_bundle,required=true \
    apt-get -o Acquire::https::CaInfo=/run/secrets/ca_bundle update \
    && apt-get -o Acquire::https::CaInfo=/run/secrets/ca_bundle install -y --no-install-recommends git libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
