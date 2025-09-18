FROM python:3.12-slim-bullseye


# Set data directory
RUN mkdir /data
# Set app directory
RUN mkdir /app

WORKDIR /app

# Install packages
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy app codes
COPY . .

# Exec
ENTRYPOINT ["python3", "-m", "src.main"]

