"""
ЗАДАНИЕ

Выбрать источник данных и собрать данные по некоторой предметной области.

Цель задания - отработать навык написания программ на Python.
В процессе выполнения задания затронем области:
- организация кода в виде проекта, импортирование модулей внутри проекта
- unit тестирование
- работа с файлами
- работа с протоколом http
- работа с pandas
- логирование

Требования к выполнению задания:

- собрать не менее 1000 объектов

- в каждом объекте должно быть не менее 5 атрибутов
(иначе просто будет не с чем работать.
исключение - вы абсолютно уверены что 4 атрибута в ваших данных
невероятно интересны)

- сохранить объекты в виде csv файла

- считать статистику по собранным объектам


Этапы:

1. Выбрать источник данных.

Это может быть любой сайт или любое API

Примеры:
- Пользователи vk.com (API)
- Посты любой популярной группы vk.com (API)
- Фильмы с Кинопоиска
(см. ссылку на статью ниже)
- Отзывы с Кинопоиска
- Статьи Википедии
(довольно сложная задача,
можно скачать дамп википедии и распарсить его,
можно найти упрощенные дампы)
- Статьи на habrahabr.ru
- Объекты на внутриигровом рынке на каком-нибудь сервере WOW (API)
(желательно англоязычном, иначе будет сложно разобраться)
- Матчи в DOTA (API)
- Сайт с кулинарными рецептами
- Ebay (API)
- Amazon (API)
...

Не ограничивайте свою фантазию. Это могут быть любые данные,
связанные с вашим хобби, работой, данные любой тематики.
Задание специально ставится в открытой форме.
У такого подхода две цели -
развить способность смотреть на задачу широко,
пополнить ваше портфолио (вы вполне можете в какой-то момент
развить этот проект в стартап, почему бы и нет,
а так же написать статью на хабр(!) или в личный блог.
Чем больше у вас таких активностей, тем ценнее ваша кандидатура на рынке)

2. Собрать данные из источника и сохранить себе в любом виде,
который потом сможете преобразовать

Можно сохранять страницы сайта в виде отдельных файлов.
Можно сразу доставать нужную информацию.
Главное - постараться не обращаться по http за одними и теми же данными много раз.
Суть в том, чтобы скачать данные себе, чтобы потом их можно было как угодно обработать.
В случае, если обработать захочется иначе - данные не надо собирать заново.
Нужно соблюдать "этикет", не пытаться заддосить сайт собирая данные в несколько потоков,
иногда может понадобиться дополнительная авторизация.

В случае с ограничениями api можно использовать time.sleep(seconds),
чтобы сделать задержку между запросами

3. Преобразовать данные из собранного вида в табличный вид.

Нужно достать из сырых данных ту самую информацию, которую считаете ценной
и сохранить в табличном формате - csv отлично для этого подходит

4. Посчитать статистики в данных
Требование - использовать pandas (мы ведь еще отрабатываем навык использования инструментария)
То, что считаете важным и хотели бы о данных узнать.

Критерий сдачи задания - собраны данные по не менее чем 1000 объектам (больше - лучше),
при запуске кода командой "python3 -m gathering stats" из собранных данных
считается и печатается в консоль некоторая статистика

Код можно менять любым удобным образом
Можно использовать и Python 2.7, и 3

Зачем нужны __init__.py файлы
https://stackoverflow.com/questions/448271/what-is-init-py-for

Про документирование в Python проекте
https://www.python.org/dev/peps/pep-0257/

Про оформление Python кода
https://www.python.org/dev/peps/pep-0008/


Примеры сбора данных:
https://habrahabr.ru/post/280238/

Для запуска тестов в корне проекта:
python3 -m unittest discover

Для запуска проекта из корня проекта:
python3 -m gathering gather
или
python3 -m gathering transform
или
python3 -m gathering stats


Для проверки стиля кода всех файлов проекта из корня проекта
pep8 .

"""

import logging
import json
import sys
import csv
import pandas as pd

from scrappers.scrapper import Scrapper
from storages.file_storage import FileStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRAPPED_FILE = 'scrapped_data.txt'
TABLE_FORMAT_FILE = 'data.csv'

def timestr_to_seconds(time):
    try:        
        h, m, s = time.split(':')
        return int(h)*3600 + int(m)*60 + int(s)
    except:
        return 0

def speed(time2, time1, distance=5):
    time1 = timestr_to_seconds(time1)
    time2 = timestr_to_seconds(time2)
    return (distance/(time2 - time1))*3600
    
def gather_process():
    logger.info("gather")
    storage = FileStorage(SCRAPPED_FILE)

    # You can also pass a storage
    scrapper = Scrapper()
    scrapper.scrap_process(storage)


def convert_data_to_table_format():
    logger.info("transform")

    # Your code here
    # transform gathered data from txt file to pandas DataFrame and save as csv
    storage = FileStorage(SCRAPPED_FILE)
    fileds_to_save = ['start-number', 'surname', 'name', 'country', 'sex', 'age', 'mass_start_result', 'personal_start_result', 'avg_pace', 'distance', 'speed_5',  'speed_10', 'speed_15', 'speed_21,1', 'speed_25', 'speed_30', 'speed_35', 'speed_40', 'speed_42,2']
    with open(TABLE_FORMAT_FILE, 'w', newline='\n') as csvfile:        
        runnerswriter = csv.DictWriter(csvfile, fileds_to_save, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL, extrasaction='ignore', restval='')    
        runnerswriter.writeheader()
        for line in storage.read_data():
            runner=json.loads(line)
            milestones=runner['milestones']
            milestones['0']='00:00:00'            
            milestones['42,2']=runner['personal_start_result']            
            cutoff = 0            
            for key in ['5','10','15','21,1','25','30','35','40','42,2']:
                if key == '42,2':
                    distance = 2.2
                elif key == '21,1':
                    distance = 6.1
                else:
                    distance = 5
                try:
                    runner['speed_' + str(key)] = round(speed(milestones[key], milestones[cutoff], distance), 4)
                    # to filter out incorrect values:
                    if runner['speed_' + str(key)] > 30 or runner['speed_' + str(key)] < 0: runner['speed_' + str(key)] = ""
                except:
                    runner['speed_' + str(key)] =""
                finally:
                    cutoff = key

            if runner['personal_start_result'] != 'DQ' and runner['personal_start_result'] != 'DNF':
                runner['personal_start_result'] = timestr_to_seconds(runner['personal_start_result'])
                runner['mass_start_result'] = timestr_to_seconds(runner['mass_start_result'])
                runner['avg_pace'] = timestr_to_seconds(runner['avg_pace'])
                    
            runnerswriter.writerow(runner)
        
    


def stats_of_data():
    logger.info("stats")
    
    df = pd.read_csv(TABLE_FORMAT_FILE, index_col="start-number")
    #print(df.info())
    #print(df.describe(include = 'all'))
    print("Данные об участниках Московского Марафона 2018 ")
    print("Участников забега: " + str(df.shape[0]))
    print("Мужчин: " + str(df[df['sex'] == 'М'].shape[0]))
    print("Женщин: " + str(df[df['sex'] == 'Ж'].shape[0]))
    print("Средний возраст мужчин:" + "%.2f" % df[df['sex'] == 'М']['age'].mean())
    print("Средний возраст женщин:" + "%.2f" % df[df['sex'] == 'Ж']['age'].mean())
    print("Количество дисквалифицированных:"  + str(df[df['personal_start_result'] == 'DQ'].shape[0]))
    print("Количество сошедших с дистанции:"  + str(df[df['personal_start_result'] == 'DNF'].shape[0]))
    print("Самый старший участник:"  + str(df['age'].max()))
    print("Самый молодой участник:"  + str(df['age'].min()))
    print("Максимальная средняя скорость:"  + str(df[['speed_5',  'speed_10', 'speed_15', 'speed_21,1', 'speed_25', 'speed_30', 'speed_35', 'speed_40', 'speed_42,2']].max().max()))
    print("Минимальная средняя скорость:"  + str(df[['speed_5',  'speed_10', 'speed_15', 'speed_21,1', 'speed_25', 'speed_30', 'speed_35', 'speed_40', 'speed_42,2']].min().min()))
    print("Медиана времени (сек): " + "%.0f" % df[(df['personal_start_result'] != 'DNF') & (df['personal_start_result'] != 'DQ')]['mass_start_result'].astype(int).mean())
    print("Top 10 стран участниц")
    print(str(df.groupby(['country']).size().nlargest(10)))    
    
    # Your code here
    # Load pandas DataFrame and print to stdout different statistics about the data.
    # Try to think about the data and use not only describe and info.
    # Ask yourself what would you like to know about this data (most frequent word, or something else)


if __name__ == '__main__':
    """
    why main is so...?
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    """
    logger.info("Work started")

    if sys.argv[1] == 'gather':
        gather_process()

    elif sys.argv[1] == 'transform':
        convert_data_to_table_format()

    elif sys.argv[1] == 'stats':
        stats_of_data()

    logger.info("work ended")
