**数据库：**

`db文件夹下：health.sql`, 导入至本地mysql，并创建用户：

```sql
CREATE USER IF NOT EXISTS 'healthapp'@'127.0.0.1' IDENTIFIED BY 'HealthApp_2026!';
CREATE USER IF NOT EXISTS 'healthapp'@'localhost' IDENTIFIED BY 'HealthApp_2026!';
GRANT ALL PRIVILEGES ON health.* TO 'healthapp'@'127.0.0.1';
GRANT ALL PRIVILEGES ON health.* TO 'healthapp'@'localhost';
FLUSH PRIVILEGES;
```

****

**运行：**

```sql
cd HealthCenter/backend
python app.py
```

