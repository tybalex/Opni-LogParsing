FROM rancher/opni-python-base:3.8

COPY *.py /app/
COPY requirements.txt /app/
RUN chmod a+rwx -R /app
WORKDIR /app
RUN pip install -r requirements.txt

CMD [ "python", "parsing_service.py" ]
