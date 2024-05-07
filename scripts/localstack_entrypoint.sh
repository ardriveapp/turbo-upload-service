#!/bin/bash

# Start the localstack service in the background
/usr/local/bin/docker-entrypoint.sh &

# Wait for the service to become available
while ! curl -s http://localhost:4566/_localstack/health | grep -q '"available"'; do
  echo "Waiting for LocalStack to become available..."
  sleep 5
done

echo "LocalStack is available."

# Now run the provisioning script
/opt/code/provision_localstack.sh

# Keep the container running
wait
