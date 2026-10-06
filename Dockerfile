FROM maven:3.9.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml .

RUN --mount=type=secret,id=maven_settings \
    mvn -s /run/secrets/maven_settings dependency:go-offline

COPY src ./src

RUN --mount=type=secret,id=maven_settings \
    mvn -s /run/secrets/maven_settings clean package

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /app/target/*.jar ./app.jar

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "./app.jar"]