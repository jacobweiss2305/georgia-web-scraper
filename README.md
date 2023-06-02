# georgia-web-scraper
Scrapy project to scrape active alcohol licenses

Steps:
1. Create and activate virtual environement `python -m venv venv && venv\Scripts\activate.bat`
2. Install reqs `pip install -r requirements.txt`
3. Edit .env-template file with S3_BUCKET_NAME and rename the file to .env
4. Run scraper `scrapy runspider main.py`
