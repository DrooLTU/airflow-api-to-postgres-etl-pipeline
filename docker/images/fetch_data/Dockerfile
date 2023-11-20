FROM python:3.11-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY scripts/fetch_data.py /usr/bin/local/fetch-data
RUN chmod +x /usr/bin/local/fetch-data
COPY kaggle.json /usr/local/kaggle/
ENV KAGGLE_CONFIG_DIR=/usr/local/kaggle/
ENV PATH="/usr/local/bin:${PATH}"       