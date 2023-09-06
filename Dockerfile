# Use an official Python runtime as the base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /kinoki_ml

# Copy the requirements file into the container at /app
COPY ./kinoki_ml/requirements.txt /kinoki_ml/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY ./kinoki_ml/ /kinoki_ml/

# Make port 80 available to the world outside this container
#EXPOSE 8080

# Run FastAPI when the container launches
CMD ["python", "service/api.py"]