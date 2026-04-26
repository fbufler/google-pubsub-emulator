FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /emulator ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /emulator /emulator

EXPOSE 8085

ENTRYPOINT ["/emulator"]
