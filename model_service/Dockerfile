FROM apache/spark:3.5.0
WORKDIR /app

USER root
RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH="${PYTHONPATH}:/opt/spark/python/"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "inference:app", "--host", "0.0.0.0", "--port", "8000"]