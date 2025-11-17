FROM python:3.9-slim
WORKDIR /app

# Cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ dự án API
COPY . .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]