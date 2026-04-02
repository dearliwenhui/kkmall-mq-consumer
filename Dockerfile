FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

FROM eclipse-temurin:21-jre-alpine
ARG ARTHAS_VERSION=4.1.8
WORKDIR /app
RUN apk add --no-cache wget curl bash unzip
RUN wget -O /tmp/arthas-bin.zip \
    https://github.com/alibaba/arthas/releases/download/arthas-all-${ARTHAS_VERSION}/arthas-bin.zip \
    && mkdir -p /opt/arthas \
    && unzip /tmp/arthas-bin.zip -d /opt/arthas \
    && rm -f /tmp/arthas-bin.zip
RUN addgroup -S spring && adduser -S spring -G spring \
    && chown -R spring:spring /app /opt/arthas
COPY --from=builder /app/target/*.jar /app/app.jar
USER spring:spring
EXPOSE 38082
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:38082/actuator/health || exit 1
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app/app.jar"]
