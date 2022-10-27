# Databricks notebook source
# MAGIC %sql
# MAGIC use database thesis;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

spark.table("thesis.raw_2019").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view sales_grouped_tmp_train as (
# MAGIC with sales_aux as (
# MAGIC select 
# MAGIC   cast (date as date) date
# MAGIC   , sum(sales_qty) as grouped_sales
# MAGIC from (raw_2019)
# MAGIC group by date
# MAGIC order by date)
# MAGIC select 
# MAGIC   date
# MAGIC   , grouped_sales
# MAGIC   , sum(grouped_sales) over (order by date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as rolling_5days_sales
# MAGIC   , lag((sum(grouped_sales) over (order by date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)), -5, 0) over (order by date) as lagged_cum_sales
# MAGIC from sales_aux);
# MAGIC 
# MAGIC select * from sales_grouped_tmp_train
# MAGIC limit 10;

# COMMAND ----------

import numpy as np, pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import adfuller
import matplotlib.pyplot as plt
from matplotlib.pylab import rcParams

# COMMAND ----------

# plt.style.available
plt.style.use('fivethirtyeight')

# COMMAND ----------

pd_df = (
  spark
    .table('sales_grouped_tmp_train')
    ).toPandas()

# COMMAND ----------

pd_df = pd_df.set_index('date').dropna()
pd_df

# COMMAND ----------

# pd_df.loc['2019-01-02']

# COMMAND ----------

final_df = pd_df['rolling_5days_sales']
final_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the autoregressive and moving average model orders

# COMMAND ----------

# model_order = []
# for p in range(0,5):
#     for q in range(0,5):
#         try:
#             model = SARIMAX(final_df, order = (p, 1, q))
#             results = model.fit()
#             model_order.append((p,q,results.aic, results.bic))
#         except:
#             model_order.append((p,q,None, None))
            
# model_order

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order model orders by aic alue
# MAGIC 
# MAGIC This value is better than bic when the goal is to predict (bic is better to explain previous beahaviour of the model)

# COMMAND ----------

model_order_order = pd.DataFrame(model_order, columns = ['p', 'q', 'aic', 'bic'])
model_order_order

# COMMAND ----------

final_df

# COMMAND ----------

model = SARIMAX(final_df, order = (0, 1, 2), seasonal_order=(0, 0, 0, 7))
results = model.fit()

# COMMAND ----------

#Generate predictions
one_step_forecast = results.get_prediction(steps = 25)

#Extract predictions mean
mean_forecast = one_step_forecast.predicted_mean

#Confidence Intervals
confidence_intervals = one_step_forecast.conf_int()

# COMMAND ----------

help(results.get_prediction)

# COMMAND ----------

mean_forecast

# COMMAND ----------

#plt da data
plt.plot(final_df.index, final_df, label = 'Real Sales')

#plot predictions
plt.plot(mean_forecast.index, mean_forecast, color = 'r', label = 'Forecasted Sales')
plt.legend()
plt.title('Sales Forecasts Vs Real Values')
plt.yticks(visible = False)
plt.show()

# COMMAND ----------

curated_pd_df = pd.merge(pd_df, mean_forecast, how = 'left', left_index = True, right_index = True)
curated_pd_df.tail(10)

# COMMAND ----------

results.plot_diagnostics(figsize = (18, 10))
plt.show()

# COMMAND ----------

results.summary()

# COMMAND ----------

print(5.1+4)
