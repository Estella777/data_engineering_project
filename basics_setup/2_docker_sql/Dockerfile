FROM python:3.9

RUN pip install pandas

#When this container runs, treat /app as the home base. If the /app folder doesn’t exist, Docker will create it automatically.
WORKDIR /app
COPY pipeline.py pipeline.py

#Launch the Python interpreter (python), then Execute the script (script.py).
ENTRYPOINT [ "python", "pipeline.py" ]