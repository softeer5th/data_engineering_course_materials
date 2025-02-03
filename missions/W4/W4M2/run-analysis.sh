#!/bin/bash

echo "Starting NYC TLC Trip Analysis..."

# Jupyter Notebook 파일 실행 (Papermill 사용)
papermill /app/NYC_TLC_trip_analysis.ipynb /app/data/output/NYC_TLC_trip_analysis_output.ipynb

echo "Analysis complete. Output saved to /app/data/output/"