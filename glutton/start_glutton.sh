#!/bin/bash

# Wait for jobmanager
echo "Waiting for JobManager to be available..."
until curl -s jobmanager:8081 > /dev/null; do
  echo "Waiting..."
  sleep 5
done

# Submit job
echo "Submitting Flink job..."
/opt/flink/bin/flink run -d -m jobmanager:8081 -c org.genkg.glutton.DataStreamKafkaJob /opt/flink/usrlib/glutton.jar

# Keep container running
echo "Job submitted, keeping container alive"
tail -f /dev/null