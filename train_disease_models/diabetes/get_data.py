import pandas as pd

df = pd.read_csv("dataset/diabetes.csv")

# 会出现异常值的列：
cols_with_zero = ['Glucose', 'BloodPressure', 'SkinThickness', 'Insulin', 'BMI']

# 查看这些列中有多少“0”值
for col in cols_with_zero:
    zero_count = (df[col] == 0).sum()
    print(f"{col} 中有 {zero_count} 个 0 值")

import numpy as np

# 替换 0 为 NaN
for col in cols_with_zero:
    df[col] = df[col].replace(0, np.nan)

# 用该列的中位数填充 NaN
for col in cols_with_zero:
    median = df[col].median()
    df[col].fillna(median, inplace=True)

# 再次确认是否还有缺失值
print(df.isnull().sum())
