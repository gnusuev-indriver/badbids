import json
import pandas as pd
import numpy as np
import math

def compute_available_prices(start_price, bidding_steps, round_value, multiplier=100):
    """
    Вычисляет доступные цены на основе начальной цены и шагов торгов
    Args:
        start_price: начальная цена
        bidding_steps: массив процентов [15, 25, 40] -> [0.15, 0.25, 0.40]
        round_value: значение для округления
        multiplier: делитель для нормализации цен (по умолчанию 100)
    Returns:
        list: массив доступных цен
    """
    prices = [start_price * (1 + step/100) for step in bidding_steps]
    # Округляем вверх до round_value и делим на multiplier
    rounded_prices =  [math.ceil(price / round_value) * round_value / multiplier for price in prices]
    return list(dict.fromkeys(rounded_prices))  # Сохраняем порядок и убираем дубликаты

def compute_new_prices(start_price, max_bid, bidding_steps_count, round_value, multiplier=100):
    """
    Вычисляет новые цены на основе алгоритма из no_badbids_bidding_steps.py
    Args:
        start_price: начальная цена
        max_bid: максимальная ставка
        bidding_steps_count: количество шагов
        round_value: значение для округления
        multiplier: делитель для нормализации цен (по умолчанию 100)
    Returns:
        list: массив уникальных новых цен
    """
    step = (max_bid - start_price) / bidding_steps_count
    prices = [start_price + n * step for n in range(1, bidding_steps_count + 1)]
    # Округляем вверх до round_value, делим на multiplier и оставляем только уникальные значения
    rounded_prices = [math.ceil(price / round_value) * round_value / multiplier for price in prices]
    return list(dict.fromkeys(rounded_prices))  # Сохраняем порядок и убираем дубликаты

def compute_new_prices_no_round(start_price, max_bid, bidding_steps_count, multiplier=100):
    """
    Вычисляет новые цены без округления
    Args:
        start_price: начальная цена
        max_bid: максимальная ставка
        bidding_steps_count: количество шагов
        multiplier: делитель для нормализации цен (по умолчанию 100)
    Returns:
        list: массив новых цен
    """
    step = (max_bid - start_price) / bidding_steps_count
    prices = [start_price + n * step for n in range(1, bidding_steps_count + 1)]
    return [price / multiplier for price in prices]

def calculate_max_bid(rec_price, start_price, distance, eta, t_param=0.0, alpha_param=0.0):
    """
    Расчет максимальной ставки max_bid из no_badbids_bidding_steps.py
    """
    return (1 + alpha_param) * max(rec_price, start_price) * (distance + eta) / (distance + t_param)

def parse_logs(file_path):
    # Читаем JSON файл
    with open(file_path, 'r') as f:
        logs = json.load(f)
    
    # Подготовим список для хранения данных
    data = []
    
    for log in logs:
        try:
            # Парсим строку лога
            log_dict = json.loads(log['line'])
            
            # Извлекаем параметры
            params = json.loads(log_dict['params'])
            
            # Получаем multiplier из currency
            multiplier = log_dict['available_prices'][0]['currency']['multiplier']
            
            # Извлекаем available_prices из JSON
            available_prices = [price['value'] / multiplier for price in log_dict['available_prices']]
            
            # Создаем запись для DataFrame
            record = {
                'timestamp': log_dict['@timestamp'],
                'algorithm_name': log_dict['algorithm_name'],
                'city_id': log_dict.get('city_id'),
                'start_price': params['StartPrice'],
                'rec_price': params['Recprice'],
                'bidding_steps': params['BiddingSteps'],
                'distance': params['Distance'],
                'duration': params['Duration'],
                'eta': params['ETA'],
                'round_value': params['RoundValue'],
                'max_bidding_price': params['MaxBiddingPrice'],
                'multiplier': multiplier,
                'available_prices': available_prices,
                'span_id': log_dict['span_id'],
                'trace_id': log_dict['trace_id'],
            }
            
            # Вычисляем default_prices с округлением
            record['default_prices'] = compute_available_prices(
                record['start_price'],
                record['bidding_steps'],
                record['round_value'],
                multiplier
            )
            
            # Вычисляем max_bid
            max_bid = calculate_max_bid(
                record['rec_price'],
                record['start_price'],
                record['duration'],
                record['eta']
            )
            
            # Вычисляем exp_prices с округлением
            record['exp_prices'] = compute_new_prices(
                record['start_price'],
                max_bid,
                len(record['bidding_steps']),
                record['round_value'],
                multiplier
            )
            
            # Вычисляем exp_prices без округления
            record['exp_prices_no_round'] = compute_new_prices_no_round(
                record['start_price'],
                max_bid,
                len(record['bidding_steps']),
                multiplier
            )
            
            data.append(record)
            
        except Exception as e:
            print(f"Ошибка при парсинге записи: {e}")
            continue
    
    # Создаем DataFrame
    df = pd.DataFrame(data)
    return df
