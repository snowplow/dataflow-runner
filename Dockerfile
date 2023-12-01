FROM alpine:3.18.5

LABEL maintainer="Snowplow Analytics <support@snowplow.io>"

COPY build/bin/linux/dataflow-runner /usr/local/bin/dataflow-runner

ENTRYPOINT [ "dataflow-runner" ]

CMD [ "--help" ]
