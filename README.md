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

```powershell
# 环境配置
conda create -n healthcenter python=3.10 -y
conda activate healthcenter
pip install -r requirements.txt
```

```powershell
cd HealthCenter
python backend/app.py
```

```powershell
# 网页搜索
http://127.0.0.1:5000/
```

- `frontend/templates`：Flask 页面模板
- `frontend/static`：CSS、JS、图片、地图数据、上传文件等静态资源
- `backend`：Flask 接口、业务逻辑、模型文件和数据导入脚本

****

**登录：**

用户名：test

密码：testtest

****
