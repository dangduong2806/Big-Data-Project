FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

# Mở cổng 8060 bên trong container
EXPOSE 8060
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8060"]