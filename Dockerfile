ARG GO_VERSION=1.15.6

# STAGE 1: building the executable
FROM golang:1.22-alpine AS build
RUN apk add --no-cache git
RUN apk --no-cache add ca-certificates

# add a user here because addgroup and adduser are not available in scratch
RUN addgroup -S myapp \
	&& adduser -S -u 10000 -g myapp myapp

COPY . .
RUN go mod download

# Build the executable
RUN CGO_ENABLED=0 go build \
	-o /app ./cmd/dashboards

# STAGE 2: build the container to run
FROM scratch AS final
COPY --from=build /app /app

# copy ca certs
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=0 /etc/passwd /etc/passwd

USER myapp

ENTRYPOINT ["/app"]