# Pocketful — Stage 1 Run Guide

## Build

Build the isolated container image:

```bash
docker build -t pocketful-stage-1 .
```

## Run

Run the container listening on port 8080 without external network dependencies:

```bash
docker run --rm -p 8080:8080 -e PORT=8080 pocketful-stage-1
```

## Health Verification

```bash
curl -i http://localhost:8080/health
```

## Contract Tests

Run the Stage 1 double-entry, concurrency, idempotency, and bitemporal tests:

```powershell
python -m unittest discover -s stage-1 -p "test_*.py" -v
```
