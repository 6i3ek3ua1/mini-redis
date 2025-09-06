FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY mini_redis/errors.py mini_redis/protocol.py mini_redis/server.py mini_redis/client.py ./

EXPOSE 31337
CMD ["python", "server.py"]