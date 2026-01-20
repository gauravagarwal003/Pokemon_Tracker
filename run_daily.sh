#!/bin/bash
# Pokemon Tracker Daily Update Script

# Navigate to the project directory
cd /Users/gaurav/Downloads/Projects/Pokemon/Pokemon_Tracker

# Run the python script using the virtual environment python
/Users/gaurav/Downloads/Projects/Pokemon/Pokemon_Tracker/venv/bin/python daily_run.py >> daily_run.log 2>&1

# Optional: Add a timestamp to the log
echo "Run completed at $(date)" >> daily_run.log
