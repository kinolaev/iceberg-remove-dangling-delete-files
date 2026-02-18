FROM gradle:9.3.1-jdk21-alpine AS build

USER gradle
WORKDIR /home/gradle/project

COPY --chown=gradle:gradle --parents \
    app/build.gradle.kts \
    core/build.gradle.kts \
    gradle/libs.versions.toml \
    gradle.properties \
    settings.gradle.kts ./
RUN gradle --no-daemon :app:installDist --dry-run

COPY --chown=gradle:gradle --parents \
    app/src \
    core/src ./
RUN gradle --no-daemon :app:installDist --offline

FROM eclipse-temurin:21-jre-alpine

COPY --from=build /home/gradle/project/app/build/install/app /app

ENTRYPOINT ["/app/bin/app"]
