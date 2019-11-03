#!/bin/bash

bucket="$1"

. environment.sh

java -jar ../target/s3pt.jar \
  --endpointUrl "$APP_CONFIG_ENDPOINT_URL" \
  --accessKey "$APP_CONFIG_ACCESS_KEY" \
  --secretKey "$APP_CONFIG_SECRET_KEY" \
  --operation CREATE_KEY_FILE \
  --bucketName "${bucket}" \
  --keyFileName "${bucket}.txt" \
  --number 1000000 \
  $APP_CONFIG_OPTIONS