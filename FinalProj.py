import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from openpyxl import load_workbook
from selenium import webdriver
import csv
import time
import os
import re

# Directories for storing data
data_dir = "data/"
os.makedirs(data_dir, exist_ok=True)

# Task 1: Data Acquisition

# 1. Load Demographics Data (Excel)
def load_demographics_data(file_path):
    try:
        print("Loading demographics data...")
        df = pd.read_excel(file_path)
        df = df.drop([1, 2, 4])

        zip_codes = [
            "Unnamed: 0", "ZCTA5 46201", "ZCTA5 46202", "ZCTA5 46203", "ZCTA5 46204", "ZCTA5 46205", "ZCTA5 46206",
            "ZCTA5 46208", "ZCTA5 46214", "ZCTA5 46216", "ZCTA5 46217", "ZCTA5 46218", "ZCTA5 46219",
            "ZCTA5 46220", "ZCTA5 46221", "ZCTA5 46222", "ZCTA5 46224", "ZCTA5 46225", "ZCTA5 46226",
            "ZCTA5 46227", "ZCTA5 46228", "ZCTA5 46229", "ZCTA5 46231", "ZCTA5 46234", "ZCTA5 46235",
            "ZCTA5 46236", "ZCTA5 46237", "ZCTA5 46239", "ZCTA5 46240", "ZCTA5 46241", "ZCTA5 46250",
            "ZCTA5 46254", "ZCTA5 46256", "ZCTA5 46259", "ZCTA5 46260", "ZCTA5 46268", "ZCTA5 46278",
            "ZCTA5 46280", "ZCTA5 46290"
        ]
        median_incomes = [
            "Median Income", "48,183", "61,082", "55,375", "88,081", "65,756", "-", "55,435", "58,863", "56,838", "88,326",
            "34,635", "57,811", "103,735", "58,316", "45,198", "53,731", "47,917", "47,086", "50,993", "95,574",
            "62,982", "76,823", "83,137", "56,160", "117,530", "76,676", "92,160", "70,728", "51,598", "69,591",
            "57,825", "86,529", "129,615", "70,642", "63,322", "153,930", "84,561", "-"
        ]

        indiana_demog = dict(zip(zip_codes, median_incomes))

        condition = ~df.columns.str.startswith('Unnamed') | (df.columns == df.columns[0])
        df = df.loc[:, condition]
        df = df.drop([0])
        df = df.drop(df.index[10:])

        income_df = pd.DataFrame([indiana_demog])
        df = pd.concat([df, income_df], ignore_index=True)

        df_transposed = df.transpose()
        df_transposed.columns = df_transposed.iloc[0]
        df_transposed = df_transposed.drop(df_transposed.index[0])
        df_transposed = df_transposed.reset_index()

        new_column_names = {
            'index': 'zip_code',
            'White': 'white_households',
            'Black or African American': 'black_or_african_american_households',
            'American Indian and Alaska Native': 'american_indian_alaska_native_households',
            'Asian': 'asian_households',
            'Native Hawaiian and Other Pacific Islander': 'pacific_islander_households',
            'Some other race': 'other_race_households',
            'Two or more races': 'mixed_race_households',
            'Hispanic or Latino origin (of any race)': 'hispanic_or_latino_households',
            'Median Income': 'median_income'
        }
        df_transposed = df_transposed.rename(columns=new_column_names)
        df_transposed['zip_code'] = df_transposed['zip_code'].str[6:]
        df_transposed['zip_code'] = pd.to_numeric(df_transposed['zip_code'], errors='coerce')
        print("Demographics data loaded successfully.")
        return df_transposed
    except Exception as e:
        print(f"Error loading and transforming demographics data: {e}")
        return None

# 2. Load Housing Prices and Rental Costs (CSV)
def load_csv_data(file_path):
    try:
        print(f"Loading CSV data from {file_path}...")
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        if 'zip_code' in df.columns:
            df['zip_code'] = pd.to_numeric(df['zip_code'], errors='coerce')
        print(f"CSV data from {file_path} loaded successfully.")
        return df
    except Exception as e:
        print(f"Error loading CSV data: {e}")
        return None

# 3. Scrape Rental Listings (Web Scraping using Selenium)
def apartments_scrape(url: str, pages: int = None) -> pd.DataFrame:
    try:
        print(f"Scraping rental listings from {url}...")
        driver = webdriver.Chrome()
        driver.get(url)
        total_pages = int(BeautifulSoup(driver.page_source, 'html.parser').find('span', class_='pageRange').text.split(' ')[-1])
        print(f"{total_pages} pages found for rental listings.")

        # Adjust the number of pages to scrape if a limit is set
        if pages is not None:
            total_pages = min(pages, total_pages)
            print(f"Scraping up to {total_pages} pages.")

        headers = ['name', 'address', 'zipcode', 'price_low', 'price_high', 'layout', 'amenities', 'link', 'phone_number']
        rows = []

        for page_number in range(1, total_pages + 1):
            url_ = f'{url}{page_number}/'
            driver.get(url_)
            time.sleep(1)

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            listings_ul = soup.find('div', class_='placardContainer').find_all('li', class_="mortar-wrapper")
            for li in listings_ul:
                try:
                    name = li.find('span', class_='js-placardTitle title').text

                    address_raw = li.find('div', class_='property-address js-url')
                    if address_raw:
                        address_raw_text = re.sub(r'\n', '', address_raw.text).strip()
                        address = address_raw_text[:-6]
                        zipcode = address_raw_text[-5:]
                    else:
                        address = None
                        zipcode = None

                    prices_raw = li.find('p', class_='property-pricing')
                    if prices_raw:
                        prices = prices_raw.text.split(' - ')
                        price_low = prices[0] if len(prices) >= 1 else None
                        price_high = prices[1] if len(prices) == 2 else prices[0]
                    else:
                        price_low = price_high = None

                    layout_raw = li.find('p', class_='property-beds')
                    layout = layout_raw.text.strip() if layout_raw else None

                    amenities_raw = li.find('p', class_='property-amenities')
                    amenities = re.sub(r'\n', ', ', amenities_raw.text.strip()) if amenities_raw else None

                    link = li.find('a', class_='property-link').get('href')

                    property_actions = li.find('div', 'property-actions')
                    phone_number = property_actions.find('a').text.strip() if property_actions else None

                    row = [name, address, zipcode, price_low, price_high, layout, amenities, link, phone_number]
                    rows.append(row)
                except AttributeError:
                    continue

        driver.quit()
        df = pd.DataFrame(rows, columns=headers)
        if 'zipcode' in df.columns:
            df['zipcode'] = pd.to_numeric(df['zipcode'], errors='coerce')
        print("Rental listings scraped successfully.")

        # Save scraped data
        output_file = os.path.join(data_dir, "scraped_rental_listings.csv")
        df.to_csv(output_file, index=False)
        print(f"Scraped rental listings saved to {output_file}.")

        return df
    except Exception as e:
        print(f"Error scraping rental listings: {e}")
        return None

# 4. Fetch Housing Trends from Census API
def fetch_housing_trends_census(zip_codes):
    try:
        print("Fetching housing trends from Census API...")
        table_data = []
        headers = None

        for zip_code in zip_codes:
            base_url = (
                f"https://api.census.gov/data/2022/acs/acs5/profile?"
                f"get=NAME,DP04_0001E,DP04_0003E,DP04_0004E,DP04_0005E"
                f"&for=zip%20code%20tabulation%20area:{zip_code}&key=ec274c4d126c681dd7359bde3ee46e4e5a684c02"
            )
            response = requests.get(base_url)
            response.raise_for_status()
            data = response.json()

            if data and len(data) > 1:
                if headers is None:
                    headers = data[0]
                table_data.append(data[1])
            else:
                print(f"No valid data for ZIP code: {zip_code}")

            time.sleep(1)

        if headers:
            output_file = "data/indiana_housing_trends.csv"
            with open(output_file, "w", newline="", encoding="utf-8") as csvFile:
                csvWriter = csv.writer(csvFile)
                csvWriter.writerow(headers)
                csvWriter.writerows(table_data)
            print(f"Census data saved to {output_file} successfully!")
            return pd.read_csv(output_file)
        else:
            print("No data collected. Check for API or ZIP code issues.")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print("An error occurred:", e)
        return pd.DataFrame()

# 5. Extract Data from PDF (Policy Reports)
def extract_pdf_data(file_path):
    try:
        print(f"Extracting data from PDF file: {file_path}...")
        reader = PdfReader(file_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        print("PDF data extracted successfully.")
        return text
    except Exception as e:
        print(f"Error extracting PDF data: {e}")
        return ""

# Task 2: Data Cleaning and Integration
def clean_and_merge_data(demographics, housing_prices, rental_costs, rental_listings, housing_trends):
    try:
        print("Cleaning and merging data...")
        if demographics is not None:
            demographics.columns = demographics.columns.astype(str).str.lower().str.replace(' ', '_')
        if housing_prices is not None:
            housing_prices.columns = housing_prices.columns.astype(str).str.lower().str.replace(' ', '_')
        if rental_costs is not None:
            rental_costs.columns = rental_costs.columns.astype(str).str.lower().str.replace(' ', '_')
        if housing_trends is not None:
            housing_trends.columns = housing_trends.columns.astype(str).str.lower().str.replace(' ', '_')

        merged_data = demographics if demographics is not None else pd.DataFrame()

        if housing_prices is not None and 'zip_code' in housing_prices.columns:
            merged_data = pd.merge(merged_data, housing_prices, on="zip_code", how="left")

        if rental_costs is not None and 'zip_code' in rental_costs.columns:
            merged_data = pd.merge(merged_data, rental_costs, on="zip_code", how="left")

        if rental_listings is not None and 'zipcode' in rental_listings.columns:
            merged_data = pd.merge(merged_data, rental_listings, left_on="zip_code", right_on="zipcode", how="left")

        if housing_trends is not None and 'zip_code_tabulation_area' in housing_trends.columns:
            merged_data = pd.merge(merged_data, housing_trends, left_on="zip_code", right_on="zip_code_tabulation_area", how="left")

        if 'median_home_price' in merged_data.columns and 'median_income' in merged_data.columns:
            merged_data['price_to_income_ratio'] = merged_data['median_home_price'] / merged_data['median_income']

        print("Data cleaning and merging completed successfully.")
        return merged_data
    except Exception as e:
        print(f"Error during data cleaning and merging: {e}")
        return pd.DataFrame()

# Task 3: Save Final Dataset
def save_clean_data(data, output_file):
    try:
        print(f"Saving cleaned data to {output_file}...")
        data.to_csv(output_file, index=False)
        print(f"Cleaned data saved to {output_file}.")
    except Exception as e:
        print(f"Error saving cleaned data: {e}")

# Main Execution
if __name__ == "__main__":
    demographics_file = os.path.join(data_dir, "indiana_demographics.xlsx")
    housing_prices_file = os.path.join(data_dir, "indiana_housing_prices.csv")
    rental_costs_file = os.path.join(data_dir, "indiana_rental_costs.csv")
    rental_listings_url = "https://www.apartments.com/indianapolis-in/"
    pdf_file = os.path.join(data_dir, "indiana_housing_policy.pdf")

    indiana_zip_codes = [
        46201, 46202, 46203, 46204, 46205, 46206, 46208, 46214, 46216, 46217
    ]

    demographics = load_demographics_data(demographics_file)
    housing_prices = load_csv_data(housing_prices_file)
    rental_costs = load_csv_data(rental_costs_file)
    rental_listings = apartments_scrape(rental_listings_url, pages=1)
    housing_trends = fetch_housing_trends_census(indiana_zip_codes)
    policy_text = extract_pdf_data(pdf_file)

    if any([demographics is not None, housing_prices is not None, rental_costs is not None, rental_listings is not None, housing_trends is not None]):
        final_data = clean_and_merge_data(demographics, housing_prices, rental_costs, rental_listings, housing_trends)
        if not final_data.empty:
            save_clean_data(final_data, os.path.join(data_dir, "final_clean_data.csv"))
        else:
            print("No data to save.")
