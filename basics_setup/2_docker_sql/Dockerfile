#image
FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

#When this container runs, treat /app as the home base. If the /app folder doesn’t exist, Docker will create it automatically.
WORKDIR /app
# copy a file into a destination 
COPY ingest_data.py ingest_data.py 


#Launch the Python interpreter (python), then Execute the script (script.py).
ENTRYPOINT [ "python", "ingest_data.py" ]