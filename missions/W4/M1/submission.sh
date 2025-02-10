#!/bin/bash

docker exec -it spark-master spark-submit /home/spark/spark_input/monte_carlo_pi.py /home/spark/spark_output 1000000