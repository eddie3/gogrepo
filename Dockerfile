FROM alpine:3.11

ENV PYTHONUNBUFFERED=1

RUN apk add --no-cache python2
RUN python -m ensurepip && \
    pip install --upgrade pip setuptools && \
    rm -r /root/.cache

RUN pip install html5lib html2text
ADD gogrepo.py /
ENTRYPOINT ["/gogrepo.py"]
WORKDIR /srv
