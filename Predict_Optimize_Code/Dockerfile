# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /usr/src/app
COPY ./code /app

# Install any needed packages specified in requirements.txt
COPY code/requirements.txt ./app
RUN pip install --no-cache-dir -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 8080

# Run app.py when the container launches
CMD ["python", "predict_optimize_script.py"]
