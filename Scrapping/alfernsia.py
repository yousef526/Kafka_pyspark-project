from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time

# step 1 import needed drivers for making the my browser env
PATH = "C:\Devoplment_selenium\msedgedriver.exe"
options = webdriver.EdgeOptions()
options.add_experimental_option("detach",True)
service = webdriver.EdgeService(executable_path=PATH)
driver = webdriver.Edge(options=options,service=service)

# step 2 create the file with header
with open("file1.csv","a",encoding="utf-8") as f1:
    f1.write("Product_category,Product_name,Product_price"+"\n")

url = "https://alfrensia.com/en/product-category/"
categories_list = ["ram","laptops/","processor","graphics-card",
                   "monitors","motherboard","cases","power-supply"]

# step 3 looping on needed categories to save thier data
for item in categories_list:
    driver.get(f"{url+item}")
    driver.maximize_window()
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(5)
    descriptions = driver.find_elements(By.CLASS_NAME,"title-wrapper")
    prices = driver.find_elements(By.CSS_SELECTOR,"div.price-wrapper span.price")

    with open("file1.csv","a",encoding="utf-8") as f1:
        for description,price in zip(descriptions,prices):

            f1.write(
                item.capitalize()+","+description.text+","+price.text.split(" ")[0]+"\n"
                )

# closing the driver
driver.quit()



# sending the output to as input for kafka producer
import sys
import os

module_path = os.path.abspath(os.path.join('D:\iti items\Study\Kafka\Project\Kafka'))
sys.path.append(module_path)

import producer


with open(r"D:\iti items\Study\Kafka\Project\Scrapping\file1.txt","r") as f1:
    producer.main()




