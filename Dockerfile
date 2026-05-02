FROM eclipse-temurin:17-jdk-alpine AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src

# Build without running tests — tests run in GitLab CI separately
RUN apk add --no-cache maven && mvn clean package -DskipTests -q

FROM eclipse-temurin:17-jre-alpine AS runner
WORKDIR /app

RUN addgroup -g 1001 -S javagroup && adduser -S javauser -u 1001 -G javagroup

COPY --from=builder --chown=javauser:javagroup /app/target/*.jar app.jar

USER javauser

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8081/actuator/health || exit 1

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+ExitOnOutOfMemoryError", \
  "-jar", "app.jar"]
