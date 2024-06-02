import my_token
import pandas as pd
from tinkoff.invest import Client
from tinkoff.invest.schemas import GetAssetFundamentalsRequest
from class_db import *
import time
import sqlite3


TOKEN = my_token.token


def quotation_count(quot):
    return quot.units + quot.nano / 1e9


def name_brief(i):
    with Client(TOKEN) as client:
        get_asset_by = client.instruments.get_asset_by(id=i)
        return get_asset_by.asset.name_brief


def ticker(i):
    with Client(TOKEN) as client:
        get_asset_by = client.instruments.get_asset_by(id=i)
        asseti_instrument = get_asset_by.asset.instruments
        print_ticker = asseti_instrument[0].ticker
        return print_ticker


def main(i):
    with Client(TOKEN) as client:
        request = GetAssetFundamentalsRequest(
            assets=i
        )
        res = client.instruments.get_asset_fundamentals(request=request)
        df_res = pd.DataFrame([{
            'Тикер': ticker(s.asset_uid),
            'Название': name_brief(s.asset_uid),
            'ROE': s.roe,
            'P/E': s.pe_ratio_ttm,
            'free_float': str(int(s.free_float * 100))+'%',
            'Выручка': s.revenue_ttm,
            'Чистая прибыль': s.net_income_ttm,
            'Валюта': s.currency,
            'Капитализация': s.market_capitalization,
            'Максимум за год': s.high_price_last_52_weeks,
            'Минимум за год': s.low_price_last_52_weeks,
            'Средний объём торгов за 10 дней': s.average_daily_volume_last_10_days,
            'Средний объём торгов за месяц': s.average_daily_volume_last_4_weeks,
            'beta': s.beta,
            'Процент форвардной дивидендной доходности по отношению к цене акций':
                str(int(s.forward_annual_dividend_yield * 100))+'%',
            'Количество акций в обращении': s.shares_outstanding,
            'EBITDA': s.ebitda_ttm,
            'EPS': s.eps_ttm,
            'EPS компании, все конвертируемые ценные бумаги компании были сконвертированы в обыкновенные акции.':
                s.diluted_eps_ttm,
            'Свободный денежный поток.': s.free_cash_flow_ttm,
            'Среднегодовой рocт выручки за 5 лет.': s.five_year_annual_revenue_growth_rate,
            'Среднегодовой рocт выручки за 3 года.': s.three_year_annual_revenue_growth_rate,
            'Соотношение рыночной капитализации компании к её чистой прибыли.': s.pe_ratio_ttm,
            'Соотношение рыночной капитализации компании к её выручке.': s.price_to_sales_ttm,
            'Соотношение рыночной капитализации компании к её балансовой стоимости.': s.price_to_book_ttm,
            'Соотношение рыночной капитализации компании к её свободному денежному потоку.':
                s.price_to_free_cash_flow_ttm,
            'Рыночная стоимость компании.': s.total_enterprise_value_mrq,
            'Соотношение EV и EBITDA.': s.ev_to_ebitda_mrq,
            'Маржа чистой прибыли.': s.net_margin_mrq,
            'Рентабельность чистой прибыли.': s.net_interest_margin_mrq,
            'Рентабельность собственного капитала': s.roe,
            'Рентабельность активов 1': s.roa,
            'Рентабельность активов 2': s.roic,
            'Сумма краткосрочных и долгосрочных обязательств компании': s.total_debt_mrq,
            'Соотношение долга к собственному капиталу': s.total_debt_to_equity_mrq,
            'Total Debt/EBITDA': s.total_debt_to_ebitda_mrq,
            'Отношение свободногоо кэша к стоимости': s.free_cash_flow_to_price,
            'Отношение чистого долга к EBITDA': s.net_debt_to_ebitda,
            'Коэффициент текущей ликвидности': s.current_ratio_mrq,
            'Коэффициент покрытия фиксированных платежей — FCCR': s.fixed_charge_coverage_ratio_fy,
            'Дивидендная доходность за 12 месяцев': s.dividend_yield_daily_ttm,
            'Выплаченные дивиденды за 12 месяцев': s.dividend_rate_ttm,
            'Значение дивидендов на акцию': s.dividends_per_share,
            'Средняя дивидендная доходность за 5 лет': s.five_years_average_dividend_yield,
            'Среднегодовой рост дивидендов за 5 лет': s.five_year_annual_dividend_growth_rate,
            'Процент чистой прибыли, уходящий на выплату дивидендов': s.dividend_payout_ratio_fy,
            'Деньги, потраченные на обратный выкуп акций': s.buy_back_ttm,
            'Рост выручки за 1 год': s.one_year_annual_revenue_growth_rate,
            'Код страны': s.domicile_indicator_code,
            'Соотношение депозитарной расписки к акциям': s.adr_to_common_share_ratio,
            'Количество сотрудников': s.number_of_employees,
            'ex_dividend_date': s.ex_dividend_date,
            'Начало фискального периода': s.fiscal_period_start_date,
            'Окончание фискального периода': s.fiscal_period_end_date,
            'Изменение общего дохода за 5 лет': s.revenue_change_five_years,
            'Изменение EPS за 5 лет': s.eps_change_five_years,
            'Изменение EBIDTA за 5 лет': s.ebitda_change_five_years,
            'Изменение общей задолжности за 5 лет': s.total_debt_change_five_years,
            'Отношение EV к выручке': s.ev_to_sales,
        } for s in res.fundamentals])
        return df_res


def work_db():
    asser_id_db = [x for x in DataShares.select()]
    list_asset = []
    for asser_id in asser_id_db:
        list_asset.append(asser_id.asset_uid)

    print(len(list_asset))
    time.sleep(65)
    table1 = (main(list_asset[:51]))
    time.sleep(65)
    table2 = (main(list_asset[51:101]))
    time.sleep(65)
    table3 = (main(list_asset[101:149]))
    final_table = (pd.concat([table1, table2, table3], ignore_index=True, axis=0))
    conn = sqlite3.connect('shares.db')
    final_table.to_sql('Fundamental', conn, if_exists='replace', index=False)

 
if __name__ == "__main__":
    work_db()
