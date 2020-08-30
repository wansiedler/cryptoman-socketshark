FROM python:3.8-slim-buster

# Create project directory (workdir)
WORKDIR /app
RUN apt-get update
RUN buildDeps='gcc' \
    && apt-get install $buildDeps -y

# Add requirements.txt to WORKDIR and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Add source code files to WORKDIR
ADD . .

# Application port (optional)
EXPOSE 9011

# Container start command
# It is also possible to override this in devspace.yaml via images.*.cmd
#CMD ["python", "socketshark.py"]
CMD ["python", "socketshark2.py"]
