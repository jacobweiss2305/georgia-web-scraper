import scrapy
import os
import datetime
import boto3
import re
from PyPDF2 import PdfReader
import pandas as pd

class AlcoholLicensesSpider(scrapy.Spider):
    name = "georgia"
    start_urls = ['https://dor.georgia.gov/active-alcohol-licenses']

    def __init__(self, *args, **kwargs):
        super(AlcoholLicensesSpider, self).__init__(*args, **kwargs)
        self.pdf_links = []

    def parse(self, response):
        # Extract the PDF links
        pdf_links = response.css('a[href^="/document"][href$="pdf/download"]::attr(href)').getall()
        self.pdf_links.extend(pdf_links)

        for link in pdf_links:
            absolute_url = response.urljoin(link)
            yield scrapy.Request(absolute_url, callback=self.save_pdf)


    def save_pdf(self, response):
        # Extract the filename from the URL using regex
        pattern = r'alcohol-accounts-active-\d{1,2}-\d{1,2}-\d{4}'
        match = re.search(pattern, response.url)
        if match:
            filename = match.group()
        else:
            # Use a default filename if the pattern is not found
            filename = 'unknown'

        # Create the 'pdfs' folder if it doesn't exist
        folder = 'pdfs'
        os.makedirs(folder, exist_ok=True)

        # Save the PDF file locally
        pdf_filename = f"{filename}.pdf"
        pdf_filepath = os.path.join(folder, pdf_filename)
        with open(pdf_filepath, 'wb') as f:
            f.write(response.body)

        self.log(f'Saved PDF file: {pdf_filepath}')

        # Parse the PDF into a DataFrame
        with open(pdf_filepath, 'rb') as pdf_file:
            pdf = PdfReader(pdf_file)
            lines = []
            for page in pdf.pages:
                text = page.extract_text()
                # Process the extracted text and append it to the list of lines
                # (You may need to customize this part based on the structure of your PDF)
                lines.extend(text.split('\n'))

            # Create a DataFrame from the lines
            df = pd.DataFrame(lines, columns=['line'])

        # Save the DataFrame as Parquet file
        parquet_filename = f"{filename}.parquet"
        parquet_filepath = os.path.join(folder, parquet_filename)
        df.to_parquet(parquet_filepath)

        self.log(f'Parsed PDF file to Parquet: {parquet_filepath}')

        # Upload the PDF and Parquet files to S3
        bucket_name = os.getenv('S3_BUCKET_NAME')
        s3_client = boto3.client('s3')
        script_name = os.path.splitext(os.path.basename(__file__))[0]
        s3_parquet_key = f'alcohol_license_{script_name}/{parquet_filename}'  # Define the S3 object key for Parquet with sub-folders
        s3_client.upload_file(parquet_filepath, bucket_name, s3_parquet_key)

        self.log(f'Uploaded Parquet file to S3 bucket: {bucket_name}/{s3_parquet_key}')


    def closed(self, reason):
        if self.pdf_links:
            # Save the PDF links to a text file
            pdf_file = 'pdf_links.txt'
            with open(pdf_file, 'w') as f:
                for link in self.pdf_links:
                    f.write(f'{link}\n')

            # Upload the PDF links file to S3
            bucket_name = os.getenv('S3_BUCKET_NAME')
            s3_client = boto3.client('s3')
            script_name = os.path.splitext(os.path.basename(__file__))[0]
            today = datetime.date.today().strftime("%Y-%m-%d")
            key = f'{script_name}/{today}/{pdf_file}'  # Define the S3 object key with sub-folders
            s3_client.upload_file(pdf_file, bucket_name, key)

            self.log(f'Uploaded PDF links file to S3 bucket: {bucket_name}/{key}')
        else:
            self.log('No PDF links found.')

        super().closed(reason)
