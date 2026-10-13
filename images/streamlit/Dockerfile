FROM de-labs-spark-base

WORKDIR /app
COPY streamlit_app/requirements.txt /app/requirements.txt
COPY streamlit_app/app.py /app/app.py

RUN pip3 install --no-cache-dir -r /app/requirements.txt

ENV STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
