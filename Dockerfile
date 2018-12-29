FROM openjdk:8
COPY target/triplestore-dump-1.0.0.jar .
RUN mkdir /var/log/TripleStoreDumper
ENV LOG_HOME /var/log/TripleStoreDumper
EXPOSE 8081/tcp
CMD java -jar /triplestore-dump-1.0.0.jar
