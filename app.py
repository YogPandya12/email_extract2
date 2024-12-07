from flask import Flask, request, render_template, send_file
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

def find_url_column(columns):
    keywords = ['website', 'url', 'websites', 'urls']
    for col in columns:
        if any(keyword in col.lower() for keyword in keywords):
            return col
    return None

def extract_emails_from_url(url):
    """Fetch emails from the given URL with better error handling."""
    if pd.isna(url) or not isinstance(url, str):
        return ""
    
    if not url.startswith(('http://', 'https://')):
        url = f"http://{url}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        text = ' '.join(soup.stripped_strings)
        
        raw_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
        
        valid_emails = [email for email in raw_emails if not email[0].isdigit()]
        
        return ', '.join(set(valid_emails)) if valid_emails else "No email ID found"
    except requests.exceptions.RequestException:
        return "URL not working"
    except Exception as e:
        return f"Error: {str(e)}"

def get_optimal_workers(file_size):
    """Return optimal number of workers based on the file size."""
    # if file_size <= 100:
    #     return 5  
    # elif file_size <= 300:
    #     return 10  
    # else:
    #     return 20 
    if file_size:
        return 1

def process_urls_in_parallel(df, url_column, num_workers):
    """Process all URLs in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        return list(executor.map(extract_emails_from_url, df[url_column]))

@app.route('/')
def upload_file():
    return render_template('upload.html')

@app.route('/process', methods=['POST'])
def process_file():
    file = request.files['file']
    if not file or not file.filename.endswith('.xlsx'):
        return "Invalid file type. Please upload an Excel file.", 400

    try:
        df = pd.read_excel(file)
        
        url_column = find_url_column(df.columns)
        if not url_column:
            return "No column found that likely contains URLs.", 400
        
        num_workers = get_optimal_workers(len(df))
        
        df['Emails'] = process_urls_in_parallel(df, url_column, num_workers)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)

        original_filename = file.filename
        processed_filename = f"processed_{original_filename}"

        return send_file(output, as_attachment=True, download_name=processed_filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return f"An error occurred: {e}", 500

if __name__ == '__main__':
    app.run(debug=True)
