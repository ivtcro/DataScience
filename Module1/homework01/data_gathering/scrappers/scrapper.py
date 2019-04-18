import logging
import requests
import re
from bs4 import BeautifulSoup
import json

logger = logging.getLogger(__name__)


class Scrapper(object):
    def __init__(self, skip_objects=None):
        self.skip_objects = skip_objects

    def scrap_process(self, storage):

        # You can iterate over ids, or get list of objects
        # from any API, or iterate throught pages of any site
        # Do not forget to skip already gathered data
        # Here is an example for you
        storage.clear_storage()
        
        for page in range(1,11):
            url = 'https://results.runc.run/event/absolute_moscow_marathon_2018/finishers/distance/1/page/' + str(page) + '/page_size/1000/'
            response = requests.get(url)            

            if not response.ok:
                logger.error(response.text)
                # then continue process, or retry, or fix your code

            else:
                print ("Page " + str(page) + " downloaded")
                # Note: here json can be used as response.json
                soup = BeautifulSoup(response.text, 'html.parser')
                results = soup.find('div', {'class': ['results-table', 'results-table--race-results']})                
                runners = results.find_all('a', {'class': 'results-table__values'})
                #runners = ['//results.runc.run/event/absolute_moscow_marathon_2018/result/54231/']
                # save scrapped objects here
                # you can save url to identify already scrapped objects
                for runner in runners:
                    print ("Downloading next runner...")
                    runner_json = dict()
                    url = 'https:' + runner.get('href')
                    #url = 'https:' + runner
                    response = requests.get(url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    runner_json['surname'], runner_json['name'] = soup.find('div', {'class': 'results-race-detail__top-racer-name'}).get_text().split('\xa0')
                    runner_json['country'], runner_json['sex'], runner_json['age'] = re.match('(\w+).+(\D)(\d+)',soup.find('div', {'class': 'results-race-detail__top-country'}).get_text()).groups()
                    runner_json['start-number'] = re.match('\D+(\d+)', soup.find('div', {'class': 'results-race-detail__top-start-number'}).get_text()).group(1)
                    summary = soup.select_one('.results-race-detail__body > .results-race-detail__body-results').find_all('div', {'results-race-detail__body-results-item'})
                    for item in summary:
                        if item.find('div', {'class': 'results-indicator__label'}) .get_text()== 'Результат от общего старта':
                            runner_json['mass_start_result'] = item.find('div', {'class': 'results-indicator__value'}).get_text()
                        
                        if item.find('div', {'class': 'results-indicator__label'}) .get_text()== 'Результат от общего старта':
                            runner_json['personal_start_result'] = item.find('div', {'class': 'results-indicator__value'}).get_text()
                        
                        if item.find('div', {'class': 'results-indicator__label'}) .get_text()== 'Средний темп':
                            runner_json['avg_pace'] = item.find('div', {'class': 'results-indicator__value'}).get_text().split()[0]
                        
                        if item.find('div', {'class': 'results-indicator__label'}) .get_text()== 'Дистанция (км)':
                            runner_json['distance'] = item.find('div', {'class': 'results-indicator__value'}).get_text()
                        
                    runner_json['milestones'] = {}
                    try:
                        laps = soup.select_one('.results-race-detail__body-time-points > .results-table').find_all('div', {'results-table__values'})

                        for lap in laps:
                            milestone_str = None
                            time_str = None
                            for child in lap.children:
                                try:
                                    if(child.name == 'div'):
                                        milestone = re.match('((\d+)|(21,1)) км', child.string)
                                        if milestone: 
                                            milestone_str = child.string.split()[0]
                                        time = re.match('\d+:\d+:\d+', child.string)
                                        if time: 
                                            time_str = child.string

                                except NavigableString: 
                                    pass

                            if time_str and milestone_str:
                                runner_json['milestones'][milestone_str]= time_str
                    except AttributeError:
                        pass
                                
                    
                    print ("Runners info scrapped: " + str(runner_json))
                    storage.append_data(json.dumps(runner_json))
                        
                    
