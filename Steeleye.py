#!/usr/bin/env python
# coding: utf-8

# ## Satvik Nagar
# ## Unit Testing module for Steel Eye Assigment
# 

# ### Importing necessary modules

# In[1]:


import xml.etree.ElementTree as ET   # module for parsing XML data
import requests    # module for sending HTTP requests
import urllib.request   # module for fetching URLs
import boto3   # module for working with AWS services
import csv   # module for working with CSV files
import zipfile   # module for working with ZIP files
import logging  #module for making a log file


# ### Checking if the response status code is 200 (OK)

# In[2]:


# configure the logger
logging.basicConfig(filename='log.txt', level=logging.DEBUG)

# Define the URL for the API request
url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"

# Define the parameters for the API request
params = {
    "q": "*",
    "fq": "publication_date:[2021-01-17T00:00:00Z TO 2021-01-19T23:59:59Z]",
    "wt": "xml",
    "indent": "true",
    "start": "0",
    "rows": "100"
}
# log the start of the API request task
logging.info('Starting API request task')

# Send the API request and get the response
response = requests.get(url, params=params)

# Check if the response status code is 200 (OK)
if response.status_code == 200:
    # If the response status code is 200, open a file in binary mode and write the response content to it
    with open("file.xml", "wb") as f:
        f.write(response.content)
        # Print a success message
        print("XML file is downloaded successfully.")
        # log the successful completion of the task
        logging.info('API request task completed successfully')
else:
    # If the response status code is not 200, print an error message
    print("Error in downloading XML file download failed.")
    # If the response status code is not 200, log an error message
    logging.error(f'Error in API request task: {error}')


# ### Finding the first download link whose file type is DLTINS

# In[3]:


# Parse the XML file and get the root element
tree = ET.parse('file.xml')
root = tree.getroot()

# log the successful completion of the task
logging.info('XML parsing task completed successfully')

# Find the first download link whose file type is DLTINS
download_link = None
for doc in root.findall(".//doc"):
    # Get the file type for the current document
    file_type = doc.find("./str[@name='file_type']").text
    
    # Check if the file type is DLTINS
    if file_type == 'DLTINS':
        # Get the download link for the current document
        download_link = doc.find("./str[@name='download_link']").text
        
        # Stop searching once the first download link with file type DLTINS is found
        break
# log the start of the file download task
logging.info('Starting file download task')

# Print the download link, if found
if download_link:
    print(f"The download link for DLTINS file is: {download_link}")
    
     # log the successful completion of the task
    logging.info('File download task completed successfully')
    
else:
    print("No download link found for DLTINS file {error}.")
    
    # log the error message
    logging.error(f'Error in file download task: {error}')


# ### Extracting the XML file from the downloaded zip file

# In[4]:


response = requests.get(download_link)
with open("DLTINS.zip", "wb") as y:
    y.write(response.content)
    logging.info('DLTINS.zip file is downloaded successfully.')

# extract the XML file
with zipfile.ZipFile("DLTINS.zip", "r") as zipreference:
    zipreference.extractall()
    logging.info('XML file extracted successfully from DLTINS.zip file.')

# parse the XML file
with open("DLTINS_20210117_01of01.xml", "rb") as y:
    xml_data = y.read().decode("utf-8")
print(xml_data)


# ### Creating a list to store the extracted data

# In[5]:


# parse the XML data
root1 = ET.fromstring(xml_data)

# create a list to store the extracted data
logging.info('Creating a list to store the extracted data...')
data = []

# iterate over the FinInstrmGnlAttrbts elements and extract the required data
logging.info('Extracting data from the XML...')
for instrm in root1.iter("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts"):
    id = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id").text
    full_nm = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm").text
    clssfctn_tp = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp").text
    cmmdty_deriv_ind = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd").text
    ntnl_ccy = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy").text
    issr_elem = instrm.find("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr")
    issr = issr_elem.text if issr_elem is not None else "False"

    # add the extracted data to the list
    data.append([id, full_nm, clssfctn_tp, cmmdty_deriv_ind, ntnl_ccy, issr])

# write the extracted data to a csv file with the given headers
logging.info('Writing the extracted data to a CSV file...')
headers = ['Id', 'Full Name', 'Classification Type', 'Commodity Derivative Indicator', 'National Currency', 'Issuer']
with open('extracted_data.csv', 'w', newline='') as o:
    writer = csv.writer(o)
    writer.writerow(headers)
    writer.writerows(data)
logging.info('Data extraction and writing to CSV completed successfully.')


# ### Creating a csv file for the extracteed file

# In[6]:


# Define the output file name
output_file = "steelyesatvikresultoutput.csv"

# Define the header row of the CSV file
header = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]

# Open the output file for writing
with open(output_file, "w", newline="") as y:
    try:
    
        # Create a CSV writer object
        writer = csv.writer(y)

        # Write the header row to the CSV file
        writer.writerow(header)
        logging.info('Header row written to CSV file')
    
        # Write the data to the CSV file
        writer.writerows(data)
        logging.info('Data written to CSV file')
    except Exception as e:
        logging.error('Error occurred while writing to CSV file: {}'.format(str(e)))   


# ### Uploading the csv file to the AWS Bucket

# In[7]:


# set the name of the S3 bucket and file to upload
bucket_name = 'steelyesatvik12017696'
file_name = 'steelyesatvikresultoutput.csv'

# set your AWS credentials and region
aws_access_key_id = 'AKIA4EXU2B63V5WNW2X7'
aws_secret_access_key = '9KqR9v7yO5RdX3c9oWBre4HxYz/OZ58SnGi/DMQN'
region_name = 'ap-south-1'

# create an S3 resource object and specify your credentials and region
s3 = boto3.resource(
    service_name="s3",
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)
try:
    # upload the file to S3 with the specified bucket name and key (file name in S3)
    s3.Bucket(bucket_name).upload_file(Filename=file_name, Key='finaluplaod.csv')
    logging.info(f'Successfully uploaded {file_name} to S3 bucket {bucket_name}.')
except Exception as e:
    logging.error(f'Error uploading {file_name} to S3 bucket {bucket_name}: {e}')

