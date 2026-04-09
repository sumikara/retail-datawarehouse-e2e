# to test scd2 logic, create and extra table with same columns and change only transaction date and employee position, 
  # then check is_active columns in 3nf.nf_employees_scd table.
import pandas as pd

# Kaynak olarak bulk offline dosyasını oku
df = pd.read_csv('/content/data/01_empty_95_off.csv')

# Employee bilgisi boş olmayan bir satır seç

cand = df[
    df['employee_name'].notna() &
    df['employee_position'].notna() &
    df['employee_hire_date'].notna() &
    df['transaction_date'].notna()
].copy()

# İlk uygun satırı al
row = cand.iloc[[0]].copy()

# SCD2 değişimini göstermek için pozisyonu değiştir
old_position = str(row.iloc[0]['employee_position'])
row.loc[:, 'employee_position'] = old_position + '_senior'

# observed_ts mantığını tetiklemek için transaction_date'i ileri taşı
# veri formatın DD-MM-YYYY HH:MM veya DD/MM/YYYY HH:MM olabilir
old_trx = str(row.iloc[0]['transaction_date'])

if '-' in old_trx:
    dt = pd.to_datetime(old_trx, format='%d-%m-%Y %H:%M', errors='coerce')
else:
    dt = pd.to_datetime(old_trx, format='%d/%m/%Y %H:%M', errors='coerce')

new_dt = dt + pd.DateOffset(months=2)

# aynı formatı koru
if '-' in old_trx:
    row.loc[:, 'transaction_date'] = new_dt.strftime('%d-%m-%Y %H:%M')
else:
    row.loc[:, 'transaction_date'] = new_dt.strftime('%d/%m/%Y %H:%M')

# İstersen salary de değiştir
if 'employee_salary' in row.columns:
    try:
        row.loc[:, 'employee_salary'] = float(row.iloc[0]['employee_salary']) + 1000
    except:
        pass

output_path = '/content/data/src_offline_retail_employee_inc.csv'
row.to_csv(output_path, index=False)

print("Created:", output_path)
print(row[['employee_name', 'employee_position', 'employee_hire_date', 'transaction_date', 'employee_salary']])
