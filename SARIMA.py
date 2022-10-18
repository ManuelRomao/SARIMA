# Databricks notebook source
# MAGIC %sql
# MAGIC use database thesis;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

spark.table("grouped_sales").display()

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
    .table('grouped_sales')
    ).toPandas()

# COMMAND ----------

pd_df = pd_df.set_index('date').dropna()
pd_df

# COMMAND ----------

final_df = pd_df['lagged_cum_sales']
final_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the autoregressive and moving average model orders

# COMMAND ----------

model_order = []
for p in range(0,5):
    for q in range(0,5):
        try:
            model = SARIMAX(final_df, order = (p, 1, q))
            results = model.fit()
            model_order.append((p,q,results.aic, results.bic))
        except:
            model_order.append((p,q,None, None))
            
model_order

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order model orders by aic alue
# MAGIC 
# MAGIC This value is better than bic when the goal is to predict (bic is better to explain previous beahaviour of the model)

# COMMAND ----------

model_order_order = pd.DataFrame(model_order, columns = ['p', 'q', 'aic', 'bic'])
model_order_order

# COMMAND ----------

model = SARIMAX(final_df, order = (0, 1, 2), seasonal_order=(0, 0, 0, 7))
results = model.fit()

# COMMAND ----------


