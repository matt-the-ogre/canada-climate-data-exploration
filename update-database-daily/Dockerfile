# Use a base image with Python installed
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the local script and any dependencies into the container
COPY main.py /app/
COPY update_daily_data_bc_stations.py /app/
COPY get_daily_data_by_station.py /app/
COPY requirements.txt /app/

# Install any required Python packages
RUN mkdir data
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the script
CMD ["python3", "main.py"]
