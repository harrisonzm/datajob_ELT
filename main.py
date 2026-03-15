import pandas as pd

df = pd.read_csv('./data/data_jobs.csv')

# Contar todas las ocurrencias
job_counts = df['job_via'].str.strip().value_counts()
job_rate = df['salary_rate'].str.isalnum().value_counts()
job_rate = df['salary_rate'].value_counts().reset_index()
job_year_pay = df['salary_year_avg'].count()
job_hourly_pay = df['salary_hour_avg'].count()


print('size: ', df.shape)
# Ver los 10 más comunes
print(job_counts.head(10))
print('size: ', len(job_counts))

# Contar un job específico
print('salary_rate')
print('size: ', job_rate['count'].sum())
print(job_rate)

# Contar filas que tienen salary_rate y también tienen salary_year o salary_hour
has_salary_rate = df['salary_rate'].notna()
has_year_or_hour = df['salary_year_avg'].notna() | df['salary_hour_avg'].notna()
salary_rate_with_pay = (has_salary_rate & has_year_or_hour)

# Para hacer value_counts, necesitas filtrar el DataFrame primero
df_with_rate_and_pay = df[salary_rate_with_pay]
print('salary_rate (con pay):')
print('size:', salary_rate_with_pay.sum())
print(df_with_rate_and_pay['salary_rate'].value_counts())

# Validar filas que tienen ambos salarios (year y hour) no nulos
has_both_salary = df['salary_year_avg'].notna() & df['salary_hour_avg'].notna()
hasBothSalary = has_both_salary.sum()


# DataFrame que tiene uno u otro pero no ambos
has_year_only = df['salary_year_avg'].notna() & df['salary_hour_avg'].isna()
has_hour_only = df['salary_hour_avg'].notna() & df['salary_year_avg'].isna()

# Contar cada uno
year_only_count = has_year_only.sum()
hour_only_count = has_hour_only.sum()

# Crear DataFrame con solo uno u otro (no ambos)
df_one_salary = df[has_year_only | has_hour_only].copy()
has_salary_rate = df['salary_rate'].notna()
no_year_avg = df['salary_year_avg'].isna()
no_hour_avg = df['salary_hour_avg'].isna()

# Filtrar: tiene salary_rate pero no tiene ninguno de los otros dos
df_rate_only = df[has_salary_rate & no_year_avg & no_hour_avg]

print('salary_rate sin year/hour avg:')
print('size:', len(df_rate_only))

# Value counts sobre salary_rate en este dataset filtrado
rate_counts = df_rate_only['salary_rate'].value_counts()
print(rate_counts)

print('hasYearOnly:', year_only_count)
print('hasHourOnly:', hour_only_count)
print('hasBothSalary:', hasBothSalary)
print('sumExceptHourWith: ',job_rate[job_rate['salary_rate'] != 'hour']['count'].sum())
print('hasPay: ', (df['salary_hour_avg'].notna() | df['salary_year_avg'].notna()).sum())
print('yearPayCount: ',job_year_pay)
print('hourlyPayCount: ', job_hourly_pay)