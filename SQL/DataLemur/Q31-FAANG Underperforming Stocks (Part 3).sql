"""
https://datalemur.com/questions/sql-bloomberg-underperforming-stocks

As a trading analyst at Bloomberg, your task is to identify specific months when a majority of the FAANG stocks (Facebook, Amazon, Apple, Netflix, Google) experienced a gain in value compared to the previous month, while one stock lagged behind its peers by recording a decrease in value. This analysis involves comparing opening prices from the previous month.

In essence, you're seeking months where 5 out of 6 FAANG stocks demonstrated an increase in value with one stock experiencing a decline.

Write a query to display the month and year in 'Mon-YYYY' format along with the ticker symbol of the stock that underperformed relative to its peers, ordered by month and year (in 'Mon-YYYY' format).

stock_prices Schema:
Column Name	Type	Description
date	datetime	The specified date (mm/dd/yyyy) of the stock data.
ticker	varchar	The stock ticker symbol (e.g., AAPL) for the corresponding company.
open	decimal	The opening price of the stock at the start of the trading day.
high	decimal	The highest price reached by the stock during the trading day.
low	decimal	The lowest price reached by the stock during the trading day.
close	decimal	The closing price of the stock at the end of the trading day.
stock_prices Example Input:
Note that the table below displays data in June and July 2023.

date	ticker	open	high	low	close
date	ticker	open	high	low	close
06/30/2023 00:00:00	AAPL	191.26	191.63	194.48	193.97
06/30/2023 00:00:00	GOOG	123.50	129.55	116.91	120.97
06/30/2023 00:00:00	AMZN	120.69	131.49	119.93	130.36
06/30/2023 00:00:00	META	265.90	289.79	258.88	286.98
06/30/2023 00:00:00	MSFT	325.93	351.47	322.50	340.54
06/30/2023 00:00:00	NFLX	397.41	448.65	393.08	440.49
07/31/2023 00:00:00	AAPL	195.26	196.06	196.49	196.45
07/31/2023 00:00:00	GOOG	120.32	134.07	115.83	133.11
07/31/2023 00:00:00	AMZN	130.82	136.65	125.92	133.68
07/31/2023 00:00:00	META	286.65	326.11	284.85	318.60
07/31/2023 00:00:00	MSFT	339.19	366.78	327.00	335.92
07/31/2023 00:00:00	NFLX	439.76	485.00	411.88	438.97
  
Example Output:
mth_yr	underperforming_stock
Jul-2023	GOOG
In July 2023, the GOOG stock underperformed relative to the other five FAANG stocks which is exhibited by comparing their opening prices in June 2023. The remaining FAANG stocks performed better in July 2023, except for GOOG which experienced a decline in value.

The dataset you are querying against may have different input & output - this is just an example!
  
"""
with cte_stocks as (
SELECT
  ticker,
  to_char(date,'Mon-YYYY') as "mth_yr",
  open as "current_month_open",
  lag(open) over (partition by ticker order by date
    rows between unbounded preceding and current row) as "prev_month_open"
  from stock_prices
  )
  , monthly_gains as (
   select
      mth_yr,
      ticker,
      CASE WHEN current_month_open > prev_month_open THEN 1 ELSE 0 END AS "gain_count"
      from cte_stocks
      order by 1
      )
  , stock_summary as (
      SELECT
        mth_yr,
        ticker,
        sum(gain_count) over (partition by mth_yr) as "total_gains",
        case when sum(gain_count) over (partition by mth_yr) = 5
        and gain_count = 0 then ticker else null end as underperforming_stock
        from monthly_gains
      )
        select 
          mth_yr,
          underperforming_stock
          from stock_summary
          where total_gains = 5
          and underperforming_stock is not null
          order by mth_yr;
        
"""
Output

mth_yr	underperforming_stock
Feb-2020	META
Jul-2020	GOOG
Jul-2023	GOOG
May-2021	NFLX
May-2023	NFLX
Nov-2021	META
"""
