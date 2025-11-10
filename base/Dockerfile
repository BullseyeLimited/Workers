FROM python:3.11-slim
WORKDIR /app

# Copy requirements from the repo (context must be repo root)
COPY base/requirements.txt /app/.base-requirements.txt
RUN pip install --no-cache-dir -r /app/.base-requirements.txt

# Copy full repo context after deps for better layering
COPY . .

ARG ENTRYPOINT_WILL_BE_REPLACED
CMD python "$ENTRYPOINT_WILL_BE_REPLACED"
