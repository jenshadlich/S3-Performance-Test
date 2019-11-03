#!/bin/bash

bucket="$1"

. environment.sh

java -jar ../target/s3pt.jar \
  --endpointUrl "$APP_CONFIG_ENDPOINT_URL" \
  --accessKey "$APP_CONFIG_ACCESS_KEY" \
  --secretKey "$APP_CONFIG_SECRET_KEY" \
  --operation RANDOM_READ \
  --bucketName "${bucket}" \
  --keyFileName "${bucket}.txt" \
  --number 1000 \
  --threads 8 \
  --size 10K \
  --keepAlive
