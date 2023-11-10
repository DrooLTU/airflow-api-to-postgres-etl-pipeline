FROM python:3.11-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY scripts/fetch_data.py /usr/bin/local/fetch-data
RUN chmod +x /usr/bin/local/fetch-data
ENV PATH="/usr/local/bin:${PATH}"       