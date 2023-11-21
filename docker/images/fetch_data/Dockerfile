FROM python:3.11-slim
RUN apt-get update && apt-get install -y dos2unix
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY scripts/fetch_data.py /usr/local/bin/fetch-data
RUN dos2unix /usr/local/bin/fetch-data && chmod +x /usr/local/bin/fetch-data
COPY kaggle.json /usr/local/kaggle/
ENV KAGGLE_CONFIG_DIR=/usr/local/kaggle/
ENV PATH="/usr/local/bin:${PATH}"       