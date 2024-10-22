# syntax=docker/dockerfile:1
# escape=\

# -- ARM64 builder
FROM debian:buster-slim AS builder-arm64

ARG HADOOP_VERSION=3.4.0
ARG HADOOP_CHECKSUM=sha256:416c732ad372c3f03370732fd2fee48fdd69c351ceed500df4c1cef3871a164f

ONBUILD ADD --checksum=${HADOOP_CHECKSUM} --link --chmod=0600 \
        https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-aarch64.tar.gz \
        /hadoop.tar.gz

# -- AMD64 builder
FROM debian:buster-slim AS builder-amd64

ARG HADOOP_VERSION=3.4.0
ARG HADOOP_CHECKSUM=sha256:e311a78480414030f9ec63549a5d685e69e26f207103d9abf21a48b9dd03c86c

ONBUILD ADD --checksum=${HADOOP_CHECKSUM} --link --chmod=0600 \
        https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
        /hadoop.tar.gz

# -- Pick builder for the target architecture and complete the package installation
FROM builder-${TARGETARCH} AS builder-env

RUN <<-"EOT" bash
  set -euC
  [[ ! -e /hadoop ]]
  tar xf /hadoop.tar.gz
  mv hadoop-${HADOOP_VERSION} /hadoop
  find /hadoop \( -iname '*example*' -o -iname 'doc' -o -iname 'sources' \) -type d -exec rm -rf \{\} \; || true
EOT

# -- Platform agnostic builder
FROM --platform=${BUILDPLATFORM} debian:buster-slim AS builder-generic

ARG HIVE_VERSION=4.0.1
ARG HIVE_CHECKSUM=sha256:2bf988a1ed17437b1103e367939c25a13f64d36cf6d1c3bef8c3f319f0067619
ARG MYSQL_VERSION=9.1.0
ARG MYSQL_CHECKSUM=sha256:f5198db93f529206d7f13f613c38753fec0b698be9f5410391aed1d62c57acb7

ADD --checksum=${HIVE_CHECKSUM} --chmod=0600 --link \
	https://dlcdn.apache.org/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
        /hive-bin-${HIVE_VERSION}.tar.gz

RUN <<-"EOT" bash
  set -euC
  [[ ! -e /hive ]]
  tar xf /hive-bin-${HIVE_VERSION}.tar.gz
  mv apache-hive-${HIVE_VERSION}-bin /hive
  find /hive \( -iname '*example*' -o -iname 'doc' -o -name 'jdbc' -name 'test' \) -type d -exec rm -rf \{\} \; || true
  rm -f /hive/lib/guava*
EOT

ADD --checksum=${MYSQL_CHECKSUM} --chmod=0600 --link \
        https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-${MYSQL_VERSION}.tar.gz \
        /mysql-connector-j-${MYSQL_VERSION}.tar.gz

RUN <<-"EOT" bash
  set -euC
  tar xf /mysql-connector-j-${MYSQL_VERSION}.tar.gz mysql-connector-j-${MYSQL_VERSION}/mysql-connector-j-${MYSQL_VERSION}.jar
  rm -f /hive/lib/mysql-connector-j-*;
  mv -f mysql-connector-j-${MYSQL_VERSION}/mysql-connector-j-${MYSQL_VERSION}.jar /hive/lib/
EOT

#ADD --chmod=0600 --link \
        #https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.767/aws-java-sdk-bundle-1.12.767.jar \
        #/hive/lib/aws-java-sdk-bundle-1.12.767.jar

#ADD --chmod=0600 --link \
#        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.767/aws-java-sdk-s3-1.12.767.jar \
#        /hive/lib/aws-java-sdk-s3-1.12.767.jar

ADD --checksum=sha256:eed653a88c339abbdc4f0d37f4439bef24209245f162e94f0c7eae34000d8d02 --chmod=0600 --link \
        https://github.com/Esri/spatial-framework-for-hadoop/releases/download/v2.2.0/spatial-sdk-json-2.2.0.jar \
        /hive/lib/spatial-sdk-json-2.2.0.jar

# -- Final stage
FROM debian:buster-slim AS final

ARG HIVE_VERSION=4.0.1
ARG HADOOP_VERSION=3.4.0

ENV HIVE_VER=${HIVE_VERSION}
ENV HIVE_HOME=/opt/hive
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${HIVE_HOME}/bin:${HADOOP_HOME}/bin:${PATH}"

USER root

COPY --from=builder-generic --link /hive ${HIVE_HOME}
COPY --from=builder-env     --link /hadoop ${HADOOP_HOME}
COPY --from=eclipse-temurin:21-jre --link ${JAVA_HOME} ${JAVA_HOME}
COPY --chmod=0555 <<-"EOT" /entrypoint.sh
#!/bin/bash
# http://www.apache.org/licenses/LICENSE-2.0
set -x

: ${DB_DRIVER:=derby}

SKIP_SCHEMA_INIT="${IS_RESUME:-false}"
[[ $VERBOSE = "true" ]] && VERBOSE_MODE="--verbose" || VERBOSE_MODE=""

function initialize_hive {
  COMMAND="-initOrUpgradeSchema"
  $HIVE_HOME/bin/schematool -dbType $DB_DRIVER $COMMAND $VERBOSE_MODE
  if [ $? -eq 0 ]; then
    echo "Initialized schema successfully.."
  else
    echo "Schema initialization failed!"
    exit 1
  fi
}

export HIVE_CONF_DIR=$HIVE_HOME/conf
if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec \
    ln -sfn {} "${HIVE_CONF_DIR}"/ \;
  export HADOOP_CONF_DIR=$HIVE_CONF_DIR
fi

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"
if [[ "${SKIP_SCHEMA_INIT}" == "false" ]]; then
  initialize_hive
fi

export METASTORE_PORT=${METASTORE_PORT:-9083}

if [ -e /opt/hive/certs/public.crt ]; then
  export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Djavax.net.ssl.trustStore=/opt/hive/mycerts.jks -Djavax.net.ssl.trustStorePassword=changeit"
  export METASTORE_OPTS="${METASTORE_OPTS} -Djavax.net.ssl.trustStore=/opt/hive/mycerts.jks -Djavax.net.ssl.trustStorePassword=changeit"
  keytool -importcert -alias mycerts -keystore /opt/hive/mycerts.jks -storepass changeit -file /opt/hive/certs/public.crt -noprompt
fi

exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp $VERBOSE_MODE --service $SERVICE_NAME
EOT

RUN <<-"EOT" bash
  set -euC
  useradd -l -M -U -u 1000 -s /bin/bash hive
  chown hive:hive ${HIVE_HOME}
  chown hive:hive -R ${HIVE_HOME}/conf
  find ${HADOOP_HOME} \( -name 'guava-*.jar' -o -name 'hadoop-aws-*.jar' -o -name 'bundle-*.jar' \) -exec ln -fs \{\} ${HIVE_HOME}/lib/ \;
EOT

USER hive
WORKDIR /opt/hive
EXPOSE 9083

ENTRYPOINT [ "/entrypoint.sh" ]

