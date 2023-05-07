#!/bin/bash
/opt/spark3/bin/spark-submit --queue streaming 01_continous_listener.py &
sleep 10
/opt/spark3/bin/spark-submit --deploy-mode cluster --queue streaming 01_structure_stream.py

