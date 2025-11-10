FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r workers/kairos/requirements.txt
CMD ["python","workers/napoleon/main.py"]
