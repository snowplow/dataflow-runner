FROM alpine:3.16.2

LABEL maintainer="Snowplow Analytics <support@snowplow.io>"

ARG DF_RUNNER_HOME=.

ENV DF_RUNNER_HOME ${DF_RUNNER_HOME}

COPY $DF_RUNNER_HOME/build/bin/linux/dataflow-runner /usr/local/bin/dataflow-runner

ENTRYPOINT [ "dataflow-runner" ]

CMD [ "--help" ]
