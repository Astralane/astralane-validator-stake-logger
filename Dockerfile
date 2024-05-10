FROM golang:1.22.3-bullseye
RUN apt update -y
WORKDIR /root
COPY . .
RUN go mod tidy
RUN go build
CMD ["/root/tpu-traffic-classes"]