FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
default-jre wget \
&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# RUN useradd -u 1001 --badname -g 0 -M -d /home/appuser 1001
# RUN mkdir -p /app/logs
# RUN mkdir -p /home/appuser
# RUN chown -R 1001:0 /app /home/appuser
# RUN chmod -R 755 /app /home/appuser

# ENV HOME=/home/appuser
# USER 1001

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
