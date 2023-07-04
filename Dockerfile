FROM --platform=linux/amd64 python:3.7

# Set up Python behavior
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV VIRTUAL_ENV=/opt/venv

# Switch on virtual environment
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install system dependencies
RUN apt-get update && apt-get clean

# Install Python dependencies
RUN pip install --upgrade pip
COPY requirements.txt ./
RUN pip3 install -r requirements.txt

# Copy all files
WORKDIR /opt
COPY . .

WORKDIR /opt/src

EXPOSE 8080

# Start up the app server
CMD python main.py