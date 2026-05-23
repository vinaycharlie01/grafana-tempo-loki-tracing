

# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install git
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod ./
COPY go.sum ./

# Download dependencies
RUN go mod download

# Copy source
COPY . .

# Build app
RUN go build -o api-gateway .

# Runtime stage
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/api-gateway .

EXPOSE 5000

CMD ["./api-gateway"]
