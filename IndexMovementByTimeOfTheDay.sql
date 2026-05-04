SELECT  (cast(high - low as int) / 10) * 10 'range',
        Count(round(high-low,-1)) 'Count',
        Count(
          Case  When time(date) >= '09:15:00' and time(date) < '09:30:00' then 1
              else null
          end
        ) '09:15-09:30',
        Count(
          Case  When time(date) >= '09:30:00' and time(date) < '10:00:00' then 1
              else null
          end
        ) '09:30-10:00',
        Count(
          Case  When time(date) >= '10:00:00' and time(date) < '10:15:00' then 1
              else null
          end
        ) '10:00-10:15',
        Count(
          Case  When time(date) >= '10:15:00' and time(date) < '10:30:00' then 1
              else null
          end
        ) '10:15-10:30',
        Count(
          Case  When time(date) >= '10:30:00' and time(date) < '10:45:00' then 1
              else null
          end
        ) '10:30-10:45',
        Count(
          Case  When time(date) >= '10:45:00' and time(date) < '11:00:00' then 1
              else null
          end
        ) '10:45-11:00', 
        Count(
          Case  When time(date) >= '11:00:00' and time(date) < '11:15:00' then 1
              else null
          end
        ) '11:00-11:15',
        Count(
          Case  When time(date) >= '11:15:00' and time(date) < '11:30:00' then 1
              else null
          end
        ) '11:15-11:30',
        Count(
          Case  When time(date) >= '11:30:00' and time(date) < '11:45:00' then 1
              else null
          end
        ) '11:30-11:45',
        Count(
          Case  When time(date) >= '11:45:00' and time(date) < '12:00:00' then 1
              else null
          end
        ) '11:45-12:00',  
        Count(
          Case  When time(date) >= '12:00:00' and time(date) < '12:15:00' then 1
              else null
          end
        ) '12:00-12:15',
        Count(
          Case  When time(date) >= '12:15:00' and time(date) < '12:30:00' then 1
              else null
          end
        ) '12:15-12:30',
        Count(
          Case  When time(date) >= '12:30:00' and time(date) < '12:45:00' then 1
              else null
          end
        ) '12:30-12:45',
        Count(
          Case  When time(date) >= '12:45:00' and time(date) < '13:00:00' then 1
              else null
          end
        ) '12:45-13:00',  
        Count(
          Case  When time(date) >= '13:00:00' and time(date) < '13:15:00' then 1
              else null
          end
        ) '13:00-13:15',
        Count(
          Case  When time(date) >= '13:15:00' and time(date) < '13:30:00' then 1
              else null
          end
        ) '13:15-13:30',
        Count(
          Case  When time(date) >= '13:30:00' and time(date) < '13:45:00' then 1
              else null
          end
        ) '13:30-13:45',
        Count(
          Case  When time(date) >= '13:45:00' and time(date) < '14:00:00' then 1
              else null
          end
        ) '13:45-14:00',  
        Count(
          Case  When time(date) >= '14:00:00' and time(date) < '14:15:00' then 1
              else null
          end
        ) '14:00-14:15',
        Count(
          Case  When time(date) >= '14:15:00' and time(date) < '14:30:00' then 1
              else null
          end
        ) '14:15-14:30',
        Count(
          Case  When time(date) >= '14:30:00' and time(date) < '14:45:00' then 1
              else null
          end
        ) '14:30-14:45',
        Count(
          Case  When time(date) >= '14:45:00' and time(date) < '15:00:00' then 1
              else null
          end
        ) '14:45-15:00',  
        Count(
          Case  When time(date) >= '15:00:00' and time(date) < '15:15:00' then 1
              else null
          end
        ) '15:00-15:15',
        Count(
          Case  When time(date) >= '15:15:00' and time(date) < '15:30:00' then 1
              else null
          end) '15:15-15:30'
FROM    instrument_history_minute
WHERE   tradingsymbol = 'NIFTY BANK' 
GROUP BY (cast(high - low as int) / 10) * 10


CASE
	When open - close = 0 Then 0
	When open - close > 0 and open - close <= 5 Then 5
	When open - close < 0 and open - close >= -5 Then -5
	When open - close > 5 and open - close <= 10 Then 10
	When open - close < -5 and open - close >= -10 Then -10
	When open - close > 10 and open - close <= 15 Then 15
	When open - close < -10 and open - close >= -15 Then -15
	When open - close > 15 and open - close <= 20 Then 20
	When open - close < -15 and open - close >= -20 Then -20
	When open - close > 20 and open - close <= 30 Then 30
	When open - close < -20 and open - close >= -30 Then -30	
	When open - close > 30 and open - close <= 40 Then 40
	When open - close < -30 and open - close >= -40 Then -40		
	When open - close > 40 and open - close <= 50 Then 50
	When open - close < -40 and open - close >= -50 Then -50	
	When open - close > 50 and open - close <= 75 Then 75
	When open - close < -50 and open - close >= -75 Then -75
	When open - close > 75 and open - close <= 100 Then 100
	When open - close < -75 and open - close >= -100 Then -100
	When open - close > 100 and open - close <= 150 Then 150
	When open - close < -100 and open - close >= -150 Then -150	
	When open - close > 150 and open - close <= 200 Then 200
	When open - close < -150 and open - close >= -200 Then -200	
	When open - close > 200 and open - close <= 300 Then 300
	When open - close < -200 and open - close >= -300 Then -300	
	When open - close > 300 and open - close <= 400 Then 400
	When open - close < -300 and open - close >= -400 Then -400	
	When open - close > 400 and open - close <= 500 Then 500
	When open - close < -400 and open - close >= -500 Then -500
	When open - close > 500 and open - close <= 500 Then 1000
	When open - close < -500 and open - close >= -500 Then -1000
END			
