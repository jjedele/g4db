FROM openjdk:8-jre-slim

COPY *.jar /usr/src/myapp/
WORKDIR /usr/src/myapp/

ENTRYPOINT ["java", "-jar", "ms3-server.jar"]
CMD ["50000", "1000", "FIFO"]