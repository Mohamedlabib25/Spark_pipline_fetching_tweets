#!/bin/bash

# Run the continuous listener in the background in the default queue
/opt/spark3/bin/spark-submit  01_continous_listener.py &

# Wait for 10 seconds to ensure that the continuous listener is up and running
sleep 10

# Run the structured streaming job in cluster mode
/opt/spark3/bin/spark-submit --deploy-mode cluster --queue streaming 01_structure_stream.py
