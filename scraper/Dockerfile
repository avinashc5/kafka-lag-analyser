FROM python:3.10

# Set working directory
WORKDIR /app

RUN mkdir /app/scraper

# Copy scraper source code
COPY . /app/scraper

RUN pip install -r /app/scraper/requirements.txt

# Run the scraper
CMD ["python3", "-m", "scraper.run"]
