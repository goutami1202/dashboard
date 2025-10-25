
FROM python:3.11-slim

# create app user
RUN groupadd -r app && useradd -r -g app app

WORKDIR /home/app

# copy requirements if present
COPY requirements.txt /home/app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy project
COPY . /home/app
RUN chown -R app:app /home/app
USER app

ENV FLASK_APP=web_app.py
EXPOSE 5000

# create runtime folders
RUN mkdir -p /home/app/uploads /home/app/outputs

#ENTRYPOINT ["python", "web_app.py"]
CMD ["sh", "-c", "gunicorn -w 1 -b 0.0.0.0:${PORT:5000} web_app:app --timeout 300 --log-level info"]