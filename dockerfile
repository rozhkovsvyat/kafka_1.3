FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /project
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /project
COPY --from=build /project/target/*.jar app.jar
ENV MAIN_CLASS=""

ENTRYPOINT ["sh", "-c", "java -cp app.jar ${MAIN_CLASS}"]
