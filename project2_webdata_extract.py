from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

import mysql.connector as mysql
from mysql.connector import Error

import pandas as pd
import time
from datetime import datetime, timedelta
import schedule

pd.set_option('display.max_columns', None)

def run_on_schedule():
    userid = ['****', '****', '****', '****']
    password = ['****', '****', '****', '****']
    username = ['****', '****', '****', '****']

    for id, pwd, name in zip(userid, password, username):

        url = "****"

        chrome_option = webdriver.ChromeOptions()
        chrome_option.add_argument("--incognito")
        chrome_option.add_argument("--headless")
        chrome_option.add_argument("window-size=1920,1080")

        driver = webdriver.Chrome(chrome_options=chrome_option)
        # driver.maximize_window()

        driver.get(url)

        driver.find_element(By.XPATH, '//span[@class="ms-Button-flexContainer flexContainer-45"]').click()

        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="i0116"]'))).send_keys(id + "@****")
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="idSIButton9"]'))).click()

        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="i0118"]'))).send_keys(pwd)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="idSIButton9"]'))).click()

        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//input[@id="idSIButton9"]'))).click()

        time.sleep(1)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//a[normalize-space()='Report']"))).click()

        time.sleep(1)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//div[@aria-label='Command bar']//div[3]"))).click()

        time.sleep(1)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Select date range']"))).click()

        time.sleep(1)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,"//body[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/ul[1]/li[2]/button[1]"))).click()
        # WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//body[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/div[1]/div[1]/ul[1]/li[6]/button[1]"))).click()

        time.sleep(5)

        content = driver.page_source.encode('utf-8').strip()
        soup = BeautifulSoup(content, "lxml")

        table = soup.find("div", {"class": 'report-list__body__tasks-column tasks-column'})
        table2 = soup.find("div", {"class": 'report-list__body--inner-horizontal-scroll-container'})
        table3 = soup.find("div", {"class": 'report-list__header__scroll-content'})

        headers = []
        for row3 in table3:
            header = row3.find('span', {"class": 'report-list__header__cell__content-span'})
            header = str(header)[str(header).find('span">') + len('span">'):str(header).rfind('</span>')]
            header = header.replace(" ", "_").replace("/", "per").lower()
            headers.append(header)
        print(headers)

        fields = []
        for row in table:
            field = {}

            hit_app = row.find('span', {"class": 'report-list__table-row__cell__task-name'})
            hit_app = str(hit_app)[str(hit_app).find('report-list__table-row__cell__task-name--ellipsis task-tooltip-host root-99" role="none">') + len('report-list__table-row__cell__task-name--ellipsis task-tooltip-host root-99" role="none">'):str(hit_app).rfind('<div hidden="" id="tooltip')]

            category = row.find('img', {"class": 'category-icon report-list__tasks-column__row__icon'})
            category = str(category)[str(category).find('<img alt="Category icon for ') + len('<img alt="Category icon for '):str(category).rfind('" class="category-icon report-list__tasks-column__row__icon"')]

            user_account = name

            extracted_time = datetime.now() - timedelta(days=1)
            extracted_time = extracted_time.strftime("%Y-%m-%d %H:%M:%S")

            field['hit_app'] = hit_app
            field['category'] = category
            field['user'] = user_account
            field['extracted_time'] = extracted_time

            fields.append(field)

        new_df = pd.DataFrame(fields)

        if len(headers) == 10:
            try:
                col1 = []
                col2 = []
                col3 = []
                col4 = []
                col5 = []
                col6 = []
                col7 = []
                col8 = []
                col9 = []
                col10 = []

                print("Extracting report for ", name)
                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::10]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::10]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::10]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                    for f4 in value[3::10]:
                        field_1 = {}
                        f4 = str(f4)[str(f4).find('"><span>') + len('"><span>'):str(f4).rfind('</span></div>')]
                        field_1['column4'] = f4
                        col4.append(field_1)

                    for f5 in value[4::10]:
                        field_1 = {}
                        f5 = str(f5)[str(f5).find('"><span>') + len('"><span>'):str(f5).rfind('</span></div>')]
                        field_1['column5'] = f5
                        col5.append(field_1)

                    for f6 in value[5::10]:
                        field_1 = {}
                        f6 = str(f6)[str(f6).find('"><span>') + len('"><span>'):str(f6).rfind('</span></div>')]
                        field_1['column6'] = f6
                        col6.append(field_1)

                    for f7 in value[6::10]:
                        field_1 = {}
                        f7 = str(f7)[str(f7).find('"><span>') + len('"><span>'):str(f7).rfind('</span></div>')]
                        field_1['column7'] = f7
                        col7.append(field_1)

                    for f8 in value[7::10]:
                        field_1 = {}
                        f8 = str(f8)[str(f8).find('"><span>') + len('"><span>'):str(f8).rfind('</span></div>')]
                        field_1['column8'] = f8
                        col8.append(field_1)

                    for f9 in value[8::10]:
                        field_1 = {}
                        f9 = str(f9)[str(f9).find('"><span>') + len('"><span>'):str(f9).rfind('</span></div>')]
                        field_1['column9'] = f9
                        col9.append(field_1)

                    for f10 in value[9::10]:
                        field_1 = {}
                        f10 = str(f10)[str(f10).find('"><span>') + len('"><span>'):str(f10).rfind('</span></div>')]
                        field_1['column10'] = f10
                        col10.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)
                new_df4 = pd.DataFrame(col4)
                new_df5 = pd.DataFrame(col5)
                new_df6 = pd.DataFrame(col6)
                new_df7 = pd.DataFrame(col7)
                new_df8 = pd.DataFrame(col8)
                new_df9 = pd.DataFrame(col9)
                new_df10 = pd.DataFrame(col10)

                print(new_df1)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3, new_df4, new_df5, new_df6, new_df7, new_df8, new_df9, new_df10], axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str'),
                                          'col4': pd.Series(dtype='str'),
                                          'col5': pd.Series(dtype='str'),
                                          'col6': pd.Series(dtype='str'),
                                          'col7': pd.Series(dtype='str'),
                                          'col8': pd.Series(dtype='str'),
                                          'col9': pd.Series(dtype='str'),
                                          'col10': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass

        elif len(headers) == 9:
            try:
                col1 = []
                col2 = []
                col3 = []
                col4 = []
                col5 = []
                col6 = []
                col7 = []
                col8 = []
                col9 = []

                print("Extracting report for ", name)
                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::9]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::9]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::9]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                    for f4 in value[3::9]:
                        field_1 = {}
                        f4 = str(f4)[str(f4).find('"><span>') + len('"><span>'):str(f4).rfind('</span></div>')]
                        field_1['column4'] = f4
                        col4.append(field_1)

                    for f5 in value[4::9]:
                        field_1 = {}
                        f5 = str(f5)[str(f5).find('"><span>') + len('"><span>'):str(f5).rfind('</span></div>')]
                        field_1['column5'] = f5
                        col5.append(field_1)

                    for f6 in value[5::9]:
                        field_1 = {}
                        f6 = str(f6)[str(f6).find('"><span>') + len('"><span>'):str(f6).rfind('</span></div>')]
                        field_1['column6'] = f6
                        col6.append(field_1)

                    for f7 in value[6::9]:
                        field_1 = {}
                        f7 = str(f7)[str(f7).find('"><span>') + len('"><span>'):str(f7).rfind('</span></div>')]
                        field_1['column7'] = f7
                        col7.append(field_1)

                    for f8 in value[7::9]:
                        field_1 = {}
                        f8 = str(f8)[str(f8).find('"><span>') + len('"><span>'):str(f8).rfind('</span></div>')]
                        field_1['column8'] = f8
                        col8.append(field_1)

                    for f9 in value[8::9]:
                        field_1 = {}
                        f9 = str(f9)[str(f9).find('"><span>') + len('"><span>'):str(f9).rfind('</span></div>')]
                        field_1['column9'] = f9
                        col9.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)
                new_df4 = pd.DataFrame(col4)
                new_df5 = pd.DataFrame(col5)
                new_df6 = pd.DataFrame(col6)
                new_df7 = pd.DataFrame(col7)
                new_df8 = pd.DataFrame(col8)
                new_df9 = pd.DataFrame(col9)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3, new_df4, new_df5, new_df6, new_df7, new_df8, new_df9],axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str'),
                                          'col4': pd.Series(dtype='str'),
                                          'col5': pd.Series(dtype='str'),
                                          'col6': pd.Series(dtype='str'),
                                          'col7': pd.Series(dtype='str'),
                                          'col8': pd.Series(dtype='str'),
                                          'col9': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass

        if len(headers) == 8:
            try:
                col1 = []
                col2 = []
                col3 = []
                col4 = []
                col5 = []
                col6 = []
                col7 = []
                col8 = []

                print("Extracting report for ", name)
                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::8]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::8]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::8]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                    for f4 in value[3::8]:
                        field_1 = {}
                        f4 = str(f4)[str(f4).find('"><span>') + len('"><span>'):str(f4).rfind('</span></div>')]
                        field_1['column4'] = f4
                        col4.append(field_1)

                    for f5 in value[4::8]:
                        field_1 = {}
                        f5 = str(f5)[str(f5).find('"><span>') + len('"><span>'):str(f5).rfind('</span></div>')]
                        field_1['column5'] = f5
                        col5.append(field_1)

                    for f6 in value[5::8]:
                        field_1 = {}
                        f6 = str(f6)[str(f6).find('"><span>') + len('"><span>'):str(f6).rfind('</span></div>')]
                        field_1['column6'] = f6
                        col6.append(field_1)

                    for f7 in value[6::8]:
                        field_1 = {}
                        f7 = str(f7)[str(f7).find('"><span>') + len('"><span>'):str(f7).rfind('</span></div>')]
                        field_1['column7'] = f7
                        col7.append(field_1)

                    for f8 in value[7::8]:
                        field_1 = {}
                        f8 = str(f8)[str(f8).find('"><span>') + len('"><span>'):str(f8).rfind('</span></div>')]
                        field_1['column8'] = f8
                        col8.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)
                new_df4 = pd.DataFrame(col4)
                new_df5 = pd.DataFrame(col5)
                new_df6 = pd.DataFrame(col6)
                new_df7 = pd.DataFrame(col7)
                new_df8 = pd.DataFrame(col8)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3, new_df4, new_df5, new_df6, new_df7, new_df8], axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str'),
                                          'col4': pd.Series(dtype='str'),
                                          'col5': pd.Series(dtype='str'),
                                          'col6': pd.Series(dtype='str'),
                                          'col7': pd.Series(dtype='str'),
                                          'col8': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass

        if len(headers) == 7:
            try:
                col1 = []
                col2 = []
                col3 = []
                col4 = []
                col5 = []
                col6 = []
                col7 = []

                print("Extracting report for ", name)
                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::7]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::7]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::7]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                    for f4 in value[3::7]:
                        field_1 = {}
                        f4 = str(f4)[str(f4).find('"><span>') + len('"><span>'):str(f4).rfind('</span></div>')]
                        field_1['column4'] = f4
                        col4.append(field_1)

                    for f5 in value[4::7]:
                        field_1 = {}
                        f5 = str(f5)[str(f5).find('"><span>') + len('"><span>'):str(f5).rfind('</span></div>')]
                        field_1['column5'] = f5
                        col5.append(field_1)

                    for f6 in value[5::7]:
                        field_1 = {}
                        f6 = str(f6)[str(f6).find('"><span>') + len('"><span>'):str(f6).rfind('</span></div>')]
                        field_1['column6'] = f6
                        col6.append(field_1)

                    for f7 in value[6::7]:
                        field_1 = {}
                        f7 = str(f7)[str(f7).find('"><span>') + len('"><span>'):str(f7).rfind('</span></div>')]
                        field_1['column7'] = f7
                        col7.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)
                new_df4 = pd.DataFrame(col4)
                new_df5 = pd.DataFrame(col5)
                new_df6 = pd.DataFrame(col6)
                new_df7 = pd.DataFrame(col7)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3, new_df4, new_df5, new_df6, new_df7], axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str'),
                                          'col4': pd.Series(dtype='str'),
                                          'col5': pd.Series(dtype='str'),
                                          'col6': pd.Series(dtype='str'),
                                          'col7': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass

        if len(headers) == 6:
            try:
                col1 = []
                col2 = []
                col3 = []
                col4 = []
                col5 = []
                col6 = []

                print("Extracting report for ", name)
                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::6]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::6]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::6]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                    for f4 in value[3::6]:
                        field_1 = {}
                        f4 = str(f4)[str(f4).find('"><span>') + len('"><span>'):str(f4).rfind('</span></div>')]
                        field_1['column4'] = f4
                        col4.append(field_1)

                    for f5 in value[4::6]:
                        field_1 = {}
                        f5 = str(f5)[str(f5).find('"><span>') + len('"><span>'):str(f5).rfind('</span></div>')]
                        field_1['column5'] = f5
                        col5.append(field_1)

                    for f6 in value[5::6]:
                        field_1 = {}
                        f6 = str(f6)[str(f6).find('"><span>') + len('"><span>'):str(f6).rfind('</span></div>')]
                        field_1['column6'] = f6
                        col6.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)
                new_df4 = pd.DataFrame(col4)
                new_df5 = pd.DataFrame(col5)
                new_df6 = pd.DataFrame(col6)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3, new_df4, new_df5, new_df6], axis=1)
                print(concat_df)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str'),
                                          'col4': pd.Series(dtype='str'),
                                          'col5': pd.Series(dtype='str'),
                                          'col6': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass

        if len(headers) == 5:
            try:
                col1 = []
                col2 = []
                col3 = []
                col4 = []
                col5 = []

                print("Extracting report for ", name)

                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::5]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::5]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::5]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                    for f4 in value[3::5]:
                        field_1 = {}
                        f4 = str(f4)[str(f4).find('"><span>') + len('"><span>'):str(f4).rfind('</span></div>')]
                        field_1['column4'] = f4
                        col4.append(field_1)

                    for f5 in value[4::5]:
                        field_1 = {}
                        f5 = str(f5)[str(f5).find('"><span>') + len('"><span>'):str(f5).rfind('</span></div>')]
                        field_1['column5'] = f5
                        col5.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)
                new_df4 = pd.DataFrame(col4)
                new_df5 = pd.DataFrame(col5)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3, new_df4, new_df5], axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str'),
                                          'col4': pd.Series(dtype='str'),
                                          'col5': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass

        if len(headers) == 4:
            try:
                col1 = []
                col2 = []
                col3 = []
                col4 = []

                print("Extracting report for ", name)

                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::4]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::4]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::4]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                    for f4 in value[3::4]:
                        field_1 = {}
                        f4 = str(f4)[str(f4).find('"><span>') + len('"><span>'):str(f4).rfind('</span></div>')]
                        field_1['column4'] = f4
                        col4.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)
                new_df4 = pd.DataFrame(col4)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3, new_df4], axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str'),
                                          'col4': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass


        elif len(headers) == 3:
            try:
                col1 = []
                col2 = []
                col3 = []

                print("Extracting report for ", name)

                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::3]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::3]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                    for f3 in value[2::3]:
                        field_1 = {}
                        f3 = str(f3)[str(f3).find('"><span>') + len('"><span>'):str(f3).rfind('</span></div>')]
                        field_1['column3'] = f3
                        col3.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)
                new_df3 = pd.DataFrame(col3)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2, new_df3], axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str'),
                                          'col3': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass


        elif len(headers) == 2:
            try:
                col1 = []
                col2 = []

                print("Extracting report for ", name)

                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::2]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                    for f2 in value[1::2]:
                        field_1 = {}
                        f2 = str(f2)[str(f2).find('"><span>') + len('"><span>'):str(f2).rfind('</span></div>')]
                        field_1['column2'] = f2
                        col2.append(field_1)

                new_df1 = pd.DataFrame(col1)
                new_df2 = pd.DataFrame(col2)

                # merge all DataFrames into one
                concat_df = pd.concat([new_df1, new_df2], axis=1)
                concat_df.columns = headers
                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str'),
                                          'col2': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass


        elif len(headers) == 1:
            try:
                col1 = []

                print("Extracting report for ", name)

                for row1 in table2:

                    value = row1.findAll('div', {"class": 'report-list__table-row__cell'})

                    for f1 in value[::1]:
                        field_1 = {}
                        f1 = str(f1)[str(f1).find('"><span>') + len('"><span>'):str(f1).rfind('</span></div>')]
                        field_1['column1'] = f1
                        col1.append(field_1)

                new_df1 = pd.DataFrame(col1)

                # merge all DataFrames into one
                new_df1.columns = headers
                final_df = pd.merge(new_df, new_df1, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)

            except:
                concat_df = pd.DataFrame({'col1': pd.Series(dtype='str')})

                final_df = pd.merge(new_df, concat_df, left_index=True, right_index=True)
                print('Extracted Information for ', name)
                print(final_df)
                print("empty data for ", name)
                pass

        try:
            conn = mysql.connect(host='localhost', database='****', user='****', password='')
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()

                if len(headers) == 10:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]
                        col_add8 = row.index[7]
                        col_add9 = row.index[8]
                        col_add10 = row.index[9]
                        col_add_11 = row.index[10]
                        col_add_12 = row.index[11]
                        col_add_13 = row.index[12]
                        col_add_14 = row.index[13]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{},{},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7, col_add8, col_add9, col_add10, col_add_11, col_add_12, col_add_13, col_add_14)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)

                elif len(headers) == 9:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]
                        col_add8 = row.index[7]
                        col_add9 = row.index[8]
                        col_add10 = row.index[9]
                        col_add_11 = row.index[10]
                        col_add_12 = row.index[11]
                        col_add_13 = row.index[12]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7, col_add8, col_add9, col_add10, col_add_11, col_add_12, col_add_13)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)

                elif len(headers) == 8:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]
                        col_add8 = row.index[7]
                        col_add9 = row.index[8]
                        col_add10 = row.index[9]
                        col_add_11 = row.index[10]
                        col_add_12 = row.index[11]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7, col_add8, col_add9, col_add10, col_add_11, col_add_12)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)


                elif len(headers) == 7:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]
                        col_add8 = row.index[7]
                        col_add9 = row.index[8]
                        col_add10 = row.index[9]
                        col_add_11 = row.index[10]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7, col_add8, col_add9, col_add10, col_add_11)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)


                elif len(headers) == 6:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]
                        col_add8 = row.index[7]
                        col_add9 = row.index[8]
                        col_add10 = row.index[9]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7, col_add8, col_add9, col_add10)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)

                if len(headers) == 5:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]
                        col_add8 = row.index[7]
                        col_add9 = row.index[8]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7, col_add8, col_add9)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)


                elif len(headers) == 4:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]
                        col_add8 = row.index[7]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7, col_add8)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)


                elif len(headers) == 3:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]
                        col_add7 = row.index[6]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6, col_add7)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)


                elif len(headers) == 2:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]
                        col_add6 = row.index[5]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{},{}) VALUES (%s,%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5, col_add6)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)


                elif len(headers) == 1:
                    # loop through the data frame
                    count = 0
                    for i, row in final_df.iterrows():
                        col_add1 = row.index[0]
                        col_add2 = row.index[1]
                        col_add3 = row.index[2]
                        col_add4 = row.index[3]
                        col_add5 = row.index[4]

                        # %S are string values
                        sql = "INSERT INTO uhrs_user ({},{},{},{},{}) VALUES (%s,%s,%s,%s,%s)".format(col_add1, col_add2, col_add3, col_add4, col_add5)
                        cursor.execute(sql, tuple(row))
                        count += 1
                        # the connection is not auto committed by default, so we must commit to save our changes
                        conn.commit()
                    print("Total inserted", count)
                    time.sleep(2)

        except Error as e:
            print("Error while connecting to MySQL", e)

schedule.every().day.at("23:40").do(run_on_schedule)

while True:
    schedule.run_pending()
    time.sleep(1)