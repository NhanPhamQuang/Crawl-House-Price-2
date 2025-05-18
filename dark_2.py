import pandas as pd
import numpy as np
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient


def extract_property_info(driver, url):
    driver.get(url)
    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "re__pr-specs-content-item-value")))

    info = {
        'Link': url,
        'Tiêu đề': np.nan,
        'Địa chỉ': np.nan,
        'Mức giá': np.nan,
        'Giá/m²': np.nan,
        'Số phòng ngủ': np.nan,
        'Huyện': np.nan,
        'Diện tích': np.nan,
        'Mặt tiền': np.nan,
        'Đường vào': np.nan,
        'Hướng nhà': np.nan,
        'Hướng ban công': np.nan,
        'Số tầng': np.nan,
        'Số toilet': np.nan,
        'Pháp lý': np.nan,
        "Latitude": np.nan,
        "Longitude": np.nan,
    }

    try:
        title_element = driver.find_element(By.CLASS_NAME, "re__pr-title")
        info['Tiêu đề'] = title_element.text.strip()
    except:
        pass

    try:
        address_element = driver.find_element(By.CLASS_NAME, "re__pr-short-description")
        info['Địa chỉ'] = address_element.text.strip()
    except:
        pass

    try:
        price_element = driver.find_element(By.CSS_SELECTOR, ".re__pr-short-info-item .value")
        info['Mức giá'] = price_element.text.strip()

        price_per_m2_element = driver.find_element(By.CSS_SELECTOR, ".re__pr-short-info-item .ext")
        info['Giá/m²'] = price_per_m2_element.text.strip()
    except:
        pass

    try:
        rooms_element = driver.find_element(By.XPATH,
                                            "//div[contains(@class, 're__pr-short-info-item') and .//span[contains(text(), 'Phòng ngủ')]]//span[@class='value']")
        info['Số phòng ngủ'] = rooms_element.text.strip()
    except:
        pass

    try:
        breadcrumbs = driver.find_elements(By.CSS_SELECTOR, ".re__breadcrumb .re__link-se")
        huyen = next((b.text.strip() for b in breadcrumbs if b.get_attribute("level") == "3"), np.nan)
        info['Huyện'] = huyen
    except:
        pass

    try:
        iframe_element = driver.find_element(By.CSS_SELECTOR, "iframe.lazyload")
        data_src = iframe_element.get_attribute("data-src")
        coords = data_src.split("q=")[1].split(",")
        latitude = coords[0].strip()
        longitude = coords[1].split("&")[0].strip()
        info['Latitude'] = latitude
        info['Longitude'] = longitude
    except:
        pass

    specs = driver.find_elements(By.CLASS_NAME, "re__pr-specs-content-item")
    for spec in specs:
        try:
            title = spec.find_element(By.CLASS_NAME, "re__pr-specs-content-item-title").text.strip()
            value = spec.find_element(By.CLASS_NAME, "re__pr-specs-content-item-value").text.strip()
            if title in info:
                info[title] = value
        except:
            pass

    return info


def create_driver():
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.page_load_strategy = 'normal'
    return uc.Chrome(options=options)


# Connect to MongoDB
client = MongoClient(
    "_")
db = client['Bigdata']
href_collection = db['hrefs_1']
details_collection = db['details']

# Define index range variables
start_index = 18000
end_index = 20000

# Query documents with dynamic index range
urls = []
for doc in href_collection.find({"index": {"$gte": start_index, "$lte": end_index}}).sort("index", 1):
    urls.append(doc['href'])

# Initialize driver
driver = create_driver()
total_urls = len(urls)

# Crawl and save data to MongoDB
for index, url in enumerate(urls, start=1):
    try:
        print(f"Đang xử lý link thứ {index}/{total_urls}: {url}")
        property_info = extract_property_info(driver, url)

        # Insert the property info into the details collection
        details_collection.insert_one(property_info)

        print(f"Đã lưu thông tin của link thứ {index} vào collection details")
        # time.sleep(1)
    except Exception as e:
        print(f"Lỗi khi xử lý link thứ {index}/{total_urls} - {url}: {str(e)}")
        driver.quit()
        driver = create_driver()

# Clean up
driver.quit()
client.close()
