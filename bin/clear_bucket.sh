#!/bin/bash

bucket="$1"

. environment.sh

java -jar ../target/s3pt.jar \
  --endpointUrl "$APP_CONFIG_ENDPOINT_URL" \
  --accessKey "$APP_CONFIG_ACCESS_KEY" \
  --secretKey "$APP_CONFIG_SECRET_KEY" \
  --operation CLEAR_BUCKET_PARALLEL \
  --bucketName "${bucket}" \
  --keyFileName "${bucket}.txt" \
  --number 1000000 \
  --threads 3 \
  $APP_CONFIG_OPTIONS
