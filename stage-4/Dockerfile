# Vendor every dependency at build time: zero outbound network at runtime.
FROM python:3.12-alpine
WORKDIR /app
COPY app.py passwords.py ./
ENV PORT=8080
EXPOSE 8080
CMD ["python", "-u", "app.py"]
