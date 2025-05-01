# TenkiCloud Cache Server Dockerfile
 # Multi-stage build: compile Go binary then package in minimal image

 FROM golang:1.20-alpine AS builder
 RUN apk add --no-cache git
 WORKDIR /workspace
 COPY go.mod  ./
 RUN go mod download
 COPY . .
 # Embed requires swagger.json present in build context
 RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o tenkicloud-cache-server main.go

 FROM alpine:3.17
 RUN apk add --no-cache ca-certificates
 WORKDIR /app
 COPY --from=builder /workspace/tenkicloud-cache-server .
 # Expose default server port
 EXPOSE 8080
 ENTRYPOINT ["/app/tenkicloud-cache-server"]