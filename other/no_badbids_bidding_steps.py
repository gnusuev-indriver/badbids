def calculate_default_bid_buttons():
    return "Default bidding algorithm"

def calculate_max_bid(rec_price, distance, eta, t_param, alpha_param):
    """
    Расчет максимальной ставки max_bid
    Args:
        rec_price: рекомендованная цена
        distance: расстояние от А до Б (t1)
        eta: время подачи машины (t2)
        t_param: параметр t
        alpha_param: параметр alpha
    """
    return (1 + alpha_param) * rec_price * (distance + eta) / (distance + t_param)

def calculate_bid_buttons(start_price, max_bid, biding_steps, last_step):
    """
    Расчет значений для кнопок бидования
    Args:
        start_price: стартовая цена
        max_bid: максимальная ставка
        biding_steps: количество кнопок бидования (len([0.1,0.2,0.3]) (пример))
        last_step: наибольший шаг торга (максимальное значение из списка c процентными шагами торга [0.1,0.2,0.3] (пример) )
    """
    if (1 + last_step) * start_price <= max_bid:
        return calculate_default_bid_buttons() # Default bidding algorithm
    else:
        bid_buttons = []
        step = (max_bid - start_price) / biding_steps
        
        for n in range(1, biding_steps + 1):
            bid_n = start_price + n * step
            bid_buttons.append(bid_n)
        
        return bid_buttons

def process_order(rec_price, start_price, distance, eta, t_param, alpha_param, biding_steps, last_step):
    """
    Основная функция обработки заказа
    Args:
        rec_price: рекомендованная цена
        distance: расстояние от А до Б (t1)
        eta: время подачи машины (t2)
        t_param: параметр t
        alpha_param: параметр alpha
        biding_steps: количество кнопок бидования
        last_step: наибольший шаг торга
    Returns:
        dict: словарь с результатами расчетов
    """

    # Расчет max_bid
    max_bid = calculate_max_bid(rec_price, distance, eta, t_param, alpha_param)

    # Расчет кнопок бидования
    bid_buttons = calculate_bid_buttons(start_price, max_bid, biding_steps, last_step)

    return bid_buttons