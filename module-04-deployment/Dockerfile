FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

# do stuff here

RUN pip install -U pip
RUN pip install pipenv

WORKDIR /app

COPY ["Pipfile", "Pipfile.lock", "./"]

RUN pipenv install --system --deploy 

COPY ["homework4.py", "homework4.py"]

ENTRYPOINT ["python", "homework4.py"]
