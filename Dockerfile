FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY errors.py protocol.py server.py client.py ./

EXPOSE 31337
CMD ["python", "server.py"]