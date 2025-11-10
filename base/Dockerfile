FROM python:3.11-slim
WORKDIR /app
COPY base/requirements.txt /tmp/       # install layer is cached
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY . .
CMD ["python","ENTRYPOINT_WILL_BE_REPLACED"]
