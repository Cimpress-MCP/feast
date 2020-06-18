#!/usr/bin/env bash

set -e
set -o pipefail

print_usage() {
  printf "Usage: ./test-end-to-end -m pytest_mark"
}
PYTEST_MARK=''
while getopts 'm:' flag; do
  case "${flag}" in
    m) PYTEST_MARK="${OPTARG}" ;;
    *) print_usage
       exit 1 ;;
  esac
done

test -z ${GOOGLE_APPLICATION_CREDENTIALS} && GOOGLE_APPLICATION_CREDENTIALS="/etc/service-account/service-account.json"
test -z ${SKIP_BUILD_JARS} && SKIP_BUILD_JARS="false"
test -z ${GOOGLE_CLOUD_PROJECT} && GOOGLE_CLOUD_PROJECT="kf-feast"
test -z ${TEMP_BUCKET} && TEMP_BUCKET="feast-templocation-kf-feast"
test -z ${JOBS_STAGING_LOCATION} && JOBS_STAGING_LOCATION="gs://${TEMP_BUCKET}/staging-location"

# Get the current build version using maven (and pom.xml)
export FEAST_BUILD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo Building version: $FEAST_BUILD_VERSION

# Get Feast project repository root and scripts directory
export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export SCRIPTS_DIR=${PROJECT_ROOT_DIR}/infra/scripts

echo "
This script will run end-to-end tests for Feast Core and Online Serving.

1. Install Redis as the store for Feast Online Serving.
2. Install Postgres for persisting Feast metadata.
3. Install Kafka and Zookeeper as the Source in Feast.
4. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from
   tests/e2e via pytest.
"

source ${SCRIPTS_DIR}/setup-common-functions.sh

install_test_tools
install_and_start_local_redis
install_and_start_local_postgres
install_and_start_local_zookeeper_and_kafka

if [[ ${SKIP_BUILD_JARS} != "true" ]]; then
  build_feast_core_and_serving
else
  echo "[DEBUG] Skipping building jars"
fi

echo "
============================================================
Starting Feast Core
============================================================
"
# Start Feast Core with auth if enabled
cat <<EOF > /tmp/core.warehouse.application.yml
feast:
  jobs:
    polling_interval_milliseconds: 30000
    job_update_timeout_seconds: 240
    active_runner: direct
    runners:
      - name: direct
        type: DirectRunner
        options: {}
  stream:
    type: kafka
    options:
      topic: feast-features
      bootstrapServers: "kafka:9092,localhost:9094"

  security:
    authentication:
      enabled: true
      provider: jwt
    authorization:
      enabled: false
      provider: none
EOF

if [[ ${PYTEST_MARK} = "auth" ]]; 
  then
    print_banner "Starting 'Feast core with auth'."
    start_feast_core /tmp/core.warehouse.application.yml
  else
    print_banner "Starting 'Feast core without auth'."
    start_feast_core
fi

start_feast_serving 
install_python_with_miniconda_and_feast_sdk

print_banner "Running end-to-end tests with pytest at 'tests/e2e'"

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

ORIGINAL_DIR=$(pwd)
cd tests/e2e

set +e
if [[ ${PYTEST_MARK} = "auth" ]]; 
then
	pytest redis/* -m ${PYTEST_MARK} --enable_auth=True --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
else
	pytest redis/* --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
fi
TEST_EXIT_CODE=$?

if [[ ${TEST_EXIT_CODE} != 0 ]]; then
  echo "[DEBUG] Printing logs"
  ls -ltrh /var/log/feast*
  cat /var/log/feast-serving-online.log /var/log/feast-core.log

  echo "[DEBUG] Printing Python packages list"
  pip list
fi

cd ${ORIGINAL_DIR}
exit ${TEST_EXIT_CODE}
