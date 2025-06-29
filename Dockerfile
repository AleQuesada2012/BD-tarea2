FROM openjdk:14-alpine

ENV SPARK_HOME=/usr/lib/python3.7/site-packages/pyspark

RUN apk add bash && \
  apk add nano && \
  apk add python3 && \
  pip3 install --upgrade pip && \
  pip3 install pyspark && \
  pip3 install pytest && \
  pip3 install faker && \
  ln /usr/bin/python3.7 /usr/bin/python


#RUN ln -s ${SPARK_HOME}/bin/spark-submit /usr/bin/spark-submit

WORKDIR /src 
COPY . /src

#RUN chmod +x run-program.sh