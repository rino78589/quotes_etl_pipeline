import requests
import logging
import json
import csv
import os


# настраиваем систему логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# первый шаг: EXTRACT
def extract_random_quote() -> dict | None:
    """
    Получение одной случайной цитаты из интернета
    """
    logging.info('Extract | Начало извлечения случайной цитаты.')
    API_URL = "https://api.quotable.io/random"

    try:
        response = requests.get(API_URL, timeout=5, verify=False) 
        response.raise_for_status()  # проеверка на ошибку
        quote_data = response.json()
        logging.info("Extract | Цитата успешно получена от API.")
        return quote_data
    
    except requests.exceptions.RequestException as e: 
        logging.error(f"Extract | Ошибка при запросе к API: {e}") 
        return None

# второй шаг: EXTRACT
def transform_quote_data(raw_quote_data: dict) -> dict | None:
    """
    Преобразование 'сырых' данных в чистый вид. 
    """
    logging.info('Transform | Начало преобразования данных цитаты.')

    if not raw_quote_data:
        logging.warning('Transform | Нет данных для преобразования.')
        return None
    
    transformed_data = {
        'content': raw_quote_data.get('content'), 
        'author': raw_quote_data.get('author'),    
        'tags': raw_quote_data.get('tags'),        
    }

    if not transformed_data['author'] or not transformed_data['content']:
        logging.error('Transform | Не удалось получить автора или текст цитаты.')
        return None
    
    logging.info('Transform | Данные цитаты успешно преобразованы.')
    return transformed_data

# третий шаг: LOAD
def load_quote_data(data: dict, filename: str = 'quotes.csv') -> bool:
    """
    Загружаем преобразованные данные цитаты в CSV-файл.
    """
    logging.info(f'Load | Начало загрузки данных в файл: {filename}')

    fieldnames = ['author', 'content', 'tags']
    file_exists = os.path.isfile(filename)  # проверяем: есть ли уже файл?

    try:
        # открываем файл для добавления
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # если файл не существовал, записываем заголовки
            if not file_exists:
                writer.writeheader()
                logging.info(f'Load | Заголовки записаны в новый файл: {filename}')

            # записываем данные одной строкой
            writer.writerow(data)
            logging.info(f'Load | Данные успешно записаны в {filename}.')
            return True

    except Exception as e:
        logging.error(f'Load | Ошибка при записи данных в файл {filename}: {e}')
        return False





if __name__ == "__main__":
    logging.info('--- Начинаем проверку нашего ETL-пайплайна ---')
    
    # шаг 1: Extract (проверка)
    extracted_quote_data = extract_random_quote() 
    
    # если извлечение прошло успешно
    if extracted_quote_data:
        print('\n[+] Шаг Extract УСПЕШЕН! Сырые данные извлечены:')
        print(json.dumps(
            extracted_quote_data, 
            indent=4,           
            ensure_ascii=False  
        ))
        
        # шаг 2: Transform (проверка)
        transformed_quote_data = transform_quote_data(extracted_quote_data)
        
        # если преобразование прошло успешно
        if transformed_quote_data:
            print('\n[+] Шаг Transform УСПЕШЕН! Данные преобразованы:')
            print(json.dumps(
                transformed_quote_data, 
                indent=4,           
                ensure_ascii=False  
            ))

            # шаг 3: Load (проверка)
            load_success = load_quote_data(transformed_quote_data, 'quotes.csv')
            if load_success:
                print(f'\n[+] Шаг Load УСПЕШЕН! Данные сохранены в файл quotes.csv.')
            else:
                print('\n[-] Шаг Load НЕУДАЧЕН. Не получилось сохранить данные.')
        else:
            print('\n[-] Шаг Transform НЕУДАЧЕН. Не получилось преобразовать цитату.')
            
    else: # eсли Extract не успешен, то Transform не запускается
        print('\n[-] Шаг Extract НЕУДАЧЕН. Не получилось извлечь цитату.')

    logging.info('--- Проверка ETL-пайплайна завершена ---')








