from datetime import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import logging
import os

CHROME_DRIVER_PATH = '{}/resources/chromedriver.exe'.format(os.getcwd())

class Blossom:
    def __init__(self, headless=False):
        self.driver = None
        
        if headless:
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            options.add_argument('window-size=1920x1080')
            options.add_argument("disable-gpu")
            options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36")
            options.add_argument("lang=ko_KR")  # 한국어!
            self.driver = webdriver.Chrome(CHROME_DRIVER_PATH, chrome_options=options)
        else:
            self.driver = webdriver.Chrome(CHROME_DRIVER_PATH)

    def login(self, id, pwd):
        # 로그인
        print('id, pwd'.format(id, pwd))
        self.driver.get('http://blossom.shinsegae.com')
        el_form_id = self.driver.find_element_by_name('txtPC_LoginID')
        el_form_id.send_keys(id)
        el_form_pw = self.driver.find_element_by_name('txtPC_LoginPW')
        el_form_pw.send_keys(pwd)
        self.driver.find_element_by_id('btnLoginCall').click()

        try:
            # 사용중 페이지가 나온다면 '계속하기'버튼 클릭
            if '사용중' == self.driver.find_element_by_class_name('point_color').text:
                self.driver.find_element_by_class_name('overlap_btn02').click()
        except Exception as e:
            pass

    def reserve_lounge(self, people, date, lounge, refresh):
        """인재개발원 예약
        people 이용인원
        date 이용기간
        lounge 라운지선택
        """
        # 인재개발원 예약 페이지 이동
        url = 'http://blossom.shinsegae.com/WebSite/Custom/doService/Lounge/write.aspx'
        if self.driver.current_url != url or refresh:
            self.driver.get(url)
        
        # 이용인원 선택
        element_people = self.driver.find_element_by_css_selector('#cphContent_ddlMan')
        people_option = element_people.find_elements_by_tag_name('option')
        for option in people_option:
            if str(option.get_attribute('value')) == people:
                option.click()

        # 라운지 선택
        self.driver.find_element_by_css_selector('input[name="ctl00$cphContent$rblLounge"][value="{}"]'.format(lounge)).click()
        
        # 바베큐장시간 변경
        self.driver.find_element_by_css_selector('input[name="ctl00$cphContent$rbBTime"][value="{}"]'.format('2')).click()
        
        # 이용기간 선택
        element_date = self.driver.find_element_by_css_selector('#cphContent_ddlDate')
        date_option = element_date.find_elements_by_tag_name('option')
        
        if date_option[-1].get_attribute('value') == date:
            date_option[-1].click()
        else:
            logging.info('이용기간 선택: {}'.format(date_option[-1].get_attribute('value')))
            return 'NO_DATE'

        # 등록요청
        self.driver.find_element_by_css_selector('#cphContent_lnkWrite').click()

        # confirm
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, 'popup_ok'))
            )
            self.driver.find_element_by_id('popup_ok').click()
        except Exception as e:
            logging.error('can\'t find confirm button')
            return 'EXCEPTION'

        # 결과 확인
        
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, 'popup_message'))
            )
            self.save_screenshot()
            msg = self.driver.find_element_by_id('popup_message').text
            self.driver.find_element_by_id('popup_ok').click()
            logging.info('결과메시지: {}'.format(msg))

            if '해당시간대는 이미 예약이 완료되었습니다. 다른 시간대를 예약 해 주세요' in msg:
                return 'ALREADY_BOOKED'
            elif '해당 임직원은 예약이 불가합니다.[년 최대 1회 사용가능]' in msg:
                return 'BOOKED_SUCC_ALREADY'
            
        except Exception as e:
            logging.error('can\'t find popup_message')
            self.save_screenshot()

            if self.driver.current_url == 'http://blossom.shinsegae.com/WebSite/Custom/doService/Lounge/list.aspx':
                return 'SUCC'
        
        return 'FAIL'

    def save_screenshot(self):
        screenshot_filename = 'screenshot_{}.png'.format(datetime.now().strftime('%Y%m%d%H%M%S'))
        self.driver.save_screenshot(screenshot_filename)
        logging.info('save screenshot: {}'.format(screenshot_filename))
        
    def logout(self):
        self.driver.get('http://blossom.shinsegae.com/WebSite/LogOut.aspx')
