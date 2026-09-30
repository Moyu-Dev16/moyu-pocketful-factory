# Pocketful — Stage 4 Run Guide

## Build

Build the isolated container image:

```bash
docker build -t pocketful-stage-4 .
```

## Run

Run the container listening on port 8080 without external network dependencies:

```bash
docker run --rm -p 8080:8080 -e PORT=8080 pocketful-stage-4
```

## Health Verification

```bash
curl -i http://localhost:8080/health
```
