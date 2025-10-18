FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Executa o script de ETL e, se for bem-sucedido, inicia o dashboard
CMD ["/bin/sh", "-c", "python scripts/processa_dados.py && streamlit run dashboard/app.py --server.port=8501 --server.address=0.0.0.0"]