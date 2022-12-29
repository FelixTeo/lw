from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import time
import datetime
import re
import schedule

pd.set_option('display.max_columns', None)

def run_on_schedule():
    url= "****"

    chrome_option = webdriver.ChromeOptions()
    chrome_option.add_argument("--incognito")
    chrome_option.add_argument("--headless")
    chrome_option.add_argument("window-size=1920,1080")

    driver = webdriver.Chrome(chrome_options = chrome_option)
    #driver.maximize_window()
    driver.get(url)

    driver.find_element(By.XPATH, '//span[@class="ms-Button-flexContainer flexContainer-45"]').click()


    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="i0116"]'))).send_keys("****")
    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="idSIButton9"]'))).click()
    #driver.find_element(By.XPATH, '//input[@id="i0116"]').send_keys("marcus.garay010@outlook.com")
    #driver.find_element(By.XPATH, '//input[@id="idSIButton9"]').click()
    time.sleep(8)
    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="i0118"]'))).send_keys("****")
    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="idSIButton9"]'))).click()
    #driver.find_element(By.XPATH, '//input[@id="i0118"]').send_keys("****")
    #driver.find_element(By.XPATH, '//input[@id="idSIButton9"]').click()


    #WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="iLandingViewAction"]')))
    time.sleep(5)
    #WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="iLandingViewAction"]'))).click()
    #driver.find_element(By.XPATH, '//input[@id="iLandingViewAction"]').click()

    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="idSIButton9"]'))).click()
    #driver.find_element(By.XPATH, '//input[@id="idSIButton9"]').click()

    time.sleep(5)

    category = []

    cards = driver.find_elements(By.XPATH, '//div[@class="category-card-title"]')
    for card in cards:
        category.append(card.text)

    fields = []

    for item in category:
        try:
            time.sleep(2)
            driver.find_element(By.XPATH, '//div[normalize-space()="' + str(item) + '"]').click()
            time.sleep(2)

            content = driver.page_source.encode('utf-8').strip()
            soup = BeautifulSoup(content, "lxml")

            grid = soup.find("div", {"class": 'tasks-as-grid-grid'})
            total_per_category = soup.find("span", {"class": "breadcrumbs__title__tasks-count"})


            for row in grid:
                field={}

                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                id = row.find('div', {"class": 'task-card__footer__buttons'})
                id = str(id)[str(id).find('id="task-start-') + len('id="task-start-'):str(id).rfind('" role="button"')]


                job_posted = row.find('div', {"class": 'task-card__meta'})
                job_posted = str(job_posted)[str(job_posted).find('<div class="task-card__meta"><span class="task-card__meta__item"><div class="ms-TooltipHost task-tooltip-host root-99" role="none">') +
                                             len('<div class="task-card__meta"><span class="task-card__meta__item"><div class="ms-TooltipHost task-tooltip-host root-99" role="none">'):str(job_posted).rfind('<div hidden=""', 0, 170)]
                job_posted = job_posted.replace('Today', '0').replace(' day ago', '').replace(' days ago', '').replace('+ year ago', '')


                job_posted_unit = row.find('div', {"class": 'task-card__meta'})
                job_posted_unit = str(job_posted_unit)[str(job_posted_unit).find('<div class="task-card__meta"><span class="task-card__meta__item"><div class="ms-TooltipHost task-tooltip-host root-99" role="none">') +
                                                       len('<div class="task-card__meta"><span class="task-card__meta__item"><div class="ms-TooltipHost task-tooltip-host root-99" role="none">'):str(job_posted_unit).rfind('<div hidden=""', 0, 170)]
                job_posted_unit = job_posted_unit.replace('+', '')
                job_posted_unit = re.sub("^\d+\s|\s\d+\s|\s\d+$", " ", job_posted_unit)
                job_posted_unit = job_posted_unit.lstrip()
                job_posted_unit = job_posted_unit.replace('Today', 'day').replace('day ago', 'day').replace('days ago', 'day').replace('year ago', 'year')


                category = row.find('div', {"class": 'ms-TooltipHost task-tooltip-host root-99'}).text


                task_name = row.find('div', {"class": 'task-card__title__caption'})
                task_name = str(task_name)[str(task_name).find('<div class="task-card__title__caption"><div class="ms-TooltipHost task-tooltip-host root-99" role="none">') +
                                           len('<div class="task-card__title__caption"><div class="ms-TooltipHost task-tooltip-host root-99" role="none">'):str(task_name).rfind('<div hidden="" id="tooltip')]


                task_name_hidden = row.find('div', {"class": 'task-card__title__caption'})
                task_name_hidden = str(task_name_hidden)[str(task_name_hidden).find('white-space: nowrap;">') +
                                                         len('white-space: nowrap;">'):str(task_name_hidden).rfind('</div></div></div>')]


                price = row.find('span', {"class": "task-card__body__badges-row__badge"})
                price = str(price)[str(price).find('$') + len('$'):str(price).rfind(' / ', 0, 150)]


                uom = row.find('span', {"class": "task-card__body__badges-row__badge"})
                uom = str(uom)[str(uom).find(' / ', 120, 150) + len(' / '):str(uom).rfind('<div hidden=""', 0, 170)]

                try:
                    uom = re.findall(r'\d+',uom)[0]
                except:
                    uom

                uom = uom.replace('HIT', '1')


                volume = row.find('div', {"class": "task-card__body__badges-row"})
                volume = str(volume)[str(volume).find('nowrap;">Estimated ') + len('nowrap;">Estimated '):str(volume).rfind(' HITs available</div></div></span></div>')]


                description = row.find('div', {"class": 'task-card__description__text'}).text


                estimated_productivity = row.find('div', {"class": 'task-card__meta'})
                estimated_productivity = str(estimated_productivity)[str(estimated_productivity).find('class="ms-TooltipHost task-tooltip-host root-99" role="none">', 300, 465) +
                                                                     len('class="ms-TooltipHost task-tooltip-host root-99" role="none">'):str(estimated_productivity).rfind(' / ')]
                estimated_productivity = estimated_productivity.replace(" min ", '.').replace(" sec", '')


                estimated_productivity_unit = row.find('div', {"class": 'task-card__meta'})
                estimated_productivity_unit = str(estimated_productivity_unit)[str(estimated_productivity_unit).find(
                    'class="ms-TooltipHost task-tooltip-host root-99" role="none">', 300, 465) +
                                                                               len('class="ms-TooltipHost task-tooltip-host root-99" role="none">'):str(estimated_productivity_unit).rfind(' / ')]
                estimated_productivity_unit = re.sub("^\d+\s|\s\d+\s|\s\d+$", " ", estimated_productivity_unit)
                estimated_productivity_unit = estimated_productivity_unit.lstrip()[0:3]


                status = row.find('div', {"class": 'ms-TooltipHost root-99'})
                status = str(status)[str(status).find('<div class="ms-TooltipHost root-99" role="none">') + len('<div class="ms-TooltipHost root-99" role="none">'):str(status).rfind('<div hidden=""')]

                total = str(total_per_category)
                total = str(total)[str(total).find('<span class="breadcrumbs__title__tasks-count">') + len(
                    '<span class="breadcrumbs__title__tasks-count">'):str(total).rfind(" total</span")]


                field['id'] = id
                field['current_time'] = current_time
                field['job_posted'] = job_posted
                field['job_posted_unit'] = job_posted_unit
                field['category'] = category
                field['task_name'] = task_name
                field['task_name_hidden'] = task_name_hidden
                field['price'] = price
                field['uom'] = uom
                field['volume'] = volume
                field['description'] = description
                field['estimated_productivity'] = estimated_productivity
                field['estimated_productivity_unit'] = estimated_productivity_unit
                field['status'] = status
                field['total'] = total


                fields.append(field)


            time.sleep(2)
            driver.back()


        except:
            pass

    df = pd.DataFrame(fields)
    print(df)



    try:
        conn = mysql.connect(host='localhost', database='****', user='****', password='')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()

            #loop through the data frame
            for i,row in df.iterrows():
                #%S are string values
                sql = "INSERT INTO uhrs.uhrs_updates VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                print("Record inserted")
                # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)

schedule.every().hour.at(":00").do(run_on_schedule)

while True:
    schedule.run_pending()
    time.sleep(1)

