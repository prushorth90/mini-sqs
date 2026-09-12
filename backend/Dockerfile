FROM gcc:14 AS build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends cmake git libasio-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY CMakeLists.txt ./
COPY src ./src
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF \
    && cmake --build build --parallel

FROM debian:trixie-slim
WORKDIR /app
COPY --from=build /app/build/mini-sqs ./mini-sqs
EXPOSE 8080
ENTRYPOINT ["./mini-sqs"]
