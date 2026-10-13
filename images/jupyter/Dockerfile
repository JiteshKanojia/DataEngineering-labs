FROM de-labs-spark-base

# JupyterLab workspace - notebooks live in ./notebooks on the host
# ipykernel: the Python notebook kernel; nbconvert: headless notebook execution (make notebooks)
RUN pip3 install --no-cache-dir "jupyterlab>=4.1" ipykernel nbconvert

EXPOSE 8888 4040

# JUPYTER_TOKEN (set via .env / compose) controls the access token; defaults to "study".
# Shell form so the env var is interpolated at container start.
ENTRYPOINT ["sh", "-c", "exec jupyter lab \
            --ip=0.0.0.0 \
            --port=8888 \
            --no-browser \
            --allow-root \
            --ServerApp.token=${JUPYTER_TOKEN:-study} \
            --ServerApp.password= \
            --notebook-dir=/workspace"]
