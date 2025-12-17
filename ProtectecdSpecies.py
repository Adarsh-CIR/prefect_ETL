import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Function to download the image from a webpage
def download_image(image_url, save_folder, search_term):
    try:
        img_response = requests.get(image_url)
        img_filename = os.path.join(save_folder, f"{search_term}.jpg")
        with open(img_filename, 'wb') as f:
            f.write(img_response.content)
        print(f"Image for {search_term} saved successfully.")
    except Exception as e:
        print(f"Error downloading {search_term}: {e}")

# Function to automate Google image search and download high-quality image
def fetch_image_from_google(search_term, save_folder):
    # Set up the Chrome WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    # Open Google Images
    url = f"https://www.google.com/search?hl=en&tbm=isch&q={search_term}"
    driver.get(url)
    
    # Wait for images to load
    time.sleep(3)

    # Click on the first image result
    first_image = driver.find_element(By.XPATH, '//*[@id="islrg"]/div[1]/div[1]/a[1]')
    first_image.click()

    # Wait for the full-size image to load
    time.sleep(3)

    # Click the "Visit" button to go to the original webpage
    visit_button = driver.find_element(By.XPATH, '//a[@class="ZINbbc xpd O9g5cc uUPGi"]')
    visit_button.click()

    # Wait for the page to load
    time.sleep(3)

    # Get the image URL from the webpage (this could vary depending on the website)
    img_tag = driver.find_element(By.XPATH, '//img[@class="main-image"]')  # Modify according to the image tag
    img_url = img_tag.get_attribute('src')

    # Download the image
    download_image(img_url, save_folder, search_term)

    # Close the browser
    driver.quit()

# Main function
def main():
    # Folder where images will be saved
    save_folder = r'D:\CIR\taes_images'
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # Search term
    search_term = "Houston Toad"  # Replace with the term you want to search for

    # Fetch and download image
    fetch_image_from_google(search_term, save_folder)

if __name__ == '__main__':
    main()
