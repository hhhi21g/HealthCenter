import pandas as pd
from flask import Flask, jsonify, render_template, send_from_directory, request, session, flash, redirect, url_for
import pymysql
import os
import sys
from werkzeug.utils import secure_filename
from werkzeug.exceptions import HTTPException
from PIL import Image
import torch
import joblib
from torchvision import transforms, models
import torch.nn as nn


app = Flask(__name__)
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER', 'healthapp')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_NAME', 'health')
DB_UNIX_SOCKET = os.getenv('DB_UNIX_SOCKET')
APP_HOST = os.getenv('APP_HOST', '0.0.0.0')
APP_PORT = int(os.getenv('PORT', os.getenv('APP_PORT', '5000')))
APP_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'

UPLOAD_FOLDER = 'static\\uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev_default_key')

# 设置保存图片的静态目录
GENERATED_DIR = os.path.join("static", "generated")
os.makedirs(GENERATED_DIR, exist_ok=True)


def get_db_connection():
    connection_kwargs = {
        'user': DB_USER,
        'password': DB_PASSWORD,
        'database': DB_NAME,
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }

    if DB_UNIX_SOCKET:
        connection_kwargs['unix_socket'] = DB_UNIX_SOCKET
    else:
        connection_kwargs['host'] = DB_HOST
        connection_kwargs['port'] = DB_PORT

    return pymysql.connect(**connection_kwargs)


def build_static_url(relative_path):
    static_path = os.path.join(app.static_folder, *relative_path.split('/'))
    if os.path.exists(static_path):
        version = int(os.path.getmtime(static_path))
        return f"/static/{relative_path}?v={version}"

    return f"/static/{relative_path}"


def get_carousel_image_url(index):
    carousel_dir = os.path.join(app.static_folder, "carousel")
    base_name = f"a{index}"
    candidates = []

    for extension in (".jpg", ".jpeg", ".png", ".webp"):
        image_path = os.path.join(carousel_dir, f"{base_name}{extension}")
        if os.path.exists(image_path):
            candidates.append(image_path)

    if not candidates:
        return build_static_url("default.jpg")

    image_path = max(candidates, key=os.path.getmtime)
    image_filename = os.path.basename(image_path)
    return build_static_url(f"carousel/{image_filename}")


@app.errorhandler(HTTPException)
def handle_http_error(error):
    if request.path.startswith('/api/'):
        return jsonify({
            'error': error.description,
            'status': error.code,
            'path': request.path,
        }), error.code

    return error


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    if request.path.startswith('/api/'):
        app.logger.exception("API request failed: %s", request.path)
        return jsonify({
            'error': '服务器内部错误',
            'detail': str(error) if APP_DEBUG else '',
            'path': request.path,
        }), 500

    raise error


@app.route('/api/home/news')
def api_home_news():
    selected_ids = [184,142,181,46,17,160,189,344,486,]

    db = get_db_connection()
    cursor = db.cursor()

    cursor.execute(f"SELECT title, content, publish_time, url FROM news WHERE id IN ({','.join(map(str, selected_ids))}) ORDER BY FIELD(id, {','.join(map(str, selected_ids))})")
    result = cursor.fetchall()
    cursor.close()
    db.close()

    for index, item in enumerate(result):
        if item['content']:
            item['summary'] = item['content'][:100] + '...'
        else:
            item['summary'] = "（暂无正文内容）"

        # 根据新闻顺序分配轮播图，支持 a1.jpg/a1.png/a1.jpeg 等格式。
        item['image_url'] = get_carousel_image_url(index + 1)

    return jsonify(result)


@app.route('/api/home2/news')
def api_home2_news():
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT title, url FROM news ORDER BY publish_time DESC LIMIT 15")
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(result)


@app.route('/api/home/policies')
def api_home_policies():
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT title, url FROM policy ORDER BY publish_time DESC LIMIT 15")
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(result)


@app.route('/api/home/knowledges')
def api_home_knowledges():
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT title, url FROM knowledges ORDER BY publish_time DESC LIMIT 15")
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(result)


@app.route('/api/home/notices')
def api_home_notices():
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT title, url FROM notice ORDER BY publish_time DESC LIMIT 15")
    result = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(result)


@app.route('/api/knowledges')
def api_knowledges():
    page = int(request.args.get('page', 1))
    size = int(request.args.get('size', 10))
    offset = (page - 1) * size

    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) AS total FROM knowledges")
    total = cursor.fetchone()['total']

    # 获取当前页数据
    cursor.execute(
        "SELECT title, content, publish_time, url FROM knowledges ORDER BY publish_time DESC LIMIT %s OFFSET %s",
        (size, offset))

    result = cursor.fetchall()
    cursor.close()
    db.close()

    # 提取正文摘要（前60字）
    for item in result:
        if item['content']:
            item['summary'] = item['content'][:100] + '...'
        else:
            item['summary'] = "（暂无正文内容）"

    return jsonify({
        "total": total,
        "data": result
    })


@app.route('/api/news')
def api_news():
    page = int(request.args.get('page', 1))
    size = int(request.args.get('size', 10))
    offset = (page - 1) * size

    db = get_db_connection()
    cursor = db.cursor()

    # 获取总数量
    cursor.execute("SELECT COUNT(*) AS total FROM news")
    total = cursor.fetchone()['total']

    # 获取当前页数据
    cursor.execute("SELECT title, content, publish_time, url FROM news ORDER BY publish_time DESC LIMIT %s OFFSET %s",
                   (size, offset))
    result = cursor.fetchall()
    cursor.close()
    db.close()

    for item in result:
        item['summary'] = (item['content'][:100] + '...') if item['content'] else "（暂无正文内容）"

    return jsonify({
        "total": total,
        "data": result
    })


@app.route('/api/notice')
def api_notice():
    page = int(request.args.get('page', 1))
    size = int(request.args.get('size', 10))
    offset = (page - 1) * size

    db = get_db_connection()
    cursor = db.cursor()

    # 获取总数量
    cursor.execute("SELECT COUNT(*) AS total FROM notice")
    total = cursor.fetchone()['total']

    # 获取当前页数据
    cursor.execute("SELECT title, content, publish_time, url FROM notice ORDER BY publish_time DESC LIMIT %s OFFSET %s",
                   (size, offset))
    result = cursor.fetchall()
    cursor.close()
    db.close()

    for item in result:
        item['summary'] = (item['content'][:100] + '...') if item['content'] else "（暂无正文内容）"

    return jsonify({
        "total": total,
        "data": result
    })


@app.route('/api/policy')
def api_policy():
    page = int(request.args.get('page', 1))
    size = int(request.args.get('size', 10))
    offset = (page - 1) * size

    db = get_db_connection()
    cursor = db.cursor()

    # 获取总数量
    cursor.execute("SELECT COUNT(*) AS total FROM policy")
    total = cursor.fetchone()['total']

    # 获取当前页数据
    cursor.execute("SELECT title, content, publish_time, url FROM policy ORDER BY publish_time DESC LIMIT %s OFFSET %s",
                   (size, offset))
    result = cursor.fetchall()
    cursor.close()
    db.close()

    for item in result:
        item['summary'] = (item['content'][:100] + '...') if item['content'] else "（暂无正文内容）"

    return jsonify({
        "total": total,
        "data": result
    })


# 注册页面
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM user WHERE username=%s", (username,))
            if cursor.fetchone():
                flash("用户名已存在")
                return redirect(url_for('register'))
            cursor.execute(
                "INSERT INTO user (username, password, chest_result, diabetes_result, heart_result)"
                " VALUES (%s, %s, %s, 0, 0)",
                (username, password, '无病')
            )
            connection.commit()
        connection.close()
        flash("注册成功，请登录")
        return redirect(url_for('login'))
    return render_template('register.html')


# 登录页面
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM user WHERE username=%s AND password=%s", (username, password))
            user = cursor.fetchone()
        connection.close()

        if user:
            session['user_id'] = user['id']
            session['username'] = user['username']
            flash("登录成功")
            return redirect('/')
        else:
            flash("用户名或密码错误")
            return redirect(url_for('login'))
    return render_template('login.html')


# 退出登录
@app.route('/logout')
def logout():
    session.clear()
    flash("您已退出登录")
    return redirect('/')


# 页面全局变量注入器（健康预警）
@app.context_processor
def inject_user_status():
    if 'user_id' in session:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT chest_result, diabetes_result, heart_result FROM user WHERE id=%s",
                           (session['user_id'],))
            row = cursor.fetchone()
        connection.close()

        has_risk = (
                row['chest_result'] != '无病' or row['diabetes_result'] == 1 or row['heart_result'] == 1
        )
        return {'user_status': row, 'has_health_risk': has_risk}
    return {}


@app.route('/')
def home():
    return render_template('home.html', active_page='home')


@app.route('/news')
def news():
    return render_template('news.html', active_page='news')


@app.route('/notice')
def notice():
    return render_template('notice.html', active_page='notice')


@app.route('/policy')
def policy():
    return render_template('policy.html', active_page='policy')


@app.route('/knowledge')
def knowledge():
    return render_template('knowledge.html', active_page='knowledge')


@app.route('/application')
def application():
    return render_template('application.html', active_page='application')


@app.route('/about')
def about():
    return render_template('about.html', active_page='about')


@app.route('/bmi')
def bmi():
    return render_template('bmi.html', active_page='bmi')


@app.route('/water-intake')
def water_intake():
    return render_template('water-intake.html', active_page='water-intake')


@app.route('/sleep-quality')
def sleep_quality():
    return render_template('sleep-quality.html', active_page='sleep-quality')


@app.route('/emergency-guide')
def emergency_guide():
    return render_template('emergency-guide.html', active_page='emergency-guide')


@app.route('/info-guide')
def info_guide():
    return render_template('info-guide.html', active_page='info-guide')


@app.route('/chest-diagnosis', methods=['GET', 'POST'])
def chest_diagnosis():
    result = None

    # 14 类标签（请根据你的模型调整）
    class_names = [
        '肺不张／肺萎陷',
        '心脏肥大',
        '胸腔积液',
        '渗透',
        '肿块',
        '结节',
        '肺炎',
        '气胸',
        '实变',
        '浮肿／水肿',
        '气肿／肺气肿',
        '纤维化',
        '胸膜增厚',
        '疝气'
    ]

    # 图像预处理
    transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize([0.5] * 3, [0.5] * 3)
    ])

    # 加载模型
    global model_chest
    if 'model_chest' not in globals():
        model_chest = models.resnet18(num_classes=14)
        model_chest.load_state_dict(torch.load('models/chest_model.pth', map_location='cpu'))
        model_chest.eval()

    if request.method == 'POST':
        file = request.files.get('ct_image')
        if file:
            filename = secure_filename(file.filename)
            save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(save_path)

            # 图像预处理
            image = Image.open(save_path).convert('RGB')
            input_tensor = transform(image).unsqueeze(0)  # shape: [1, 3, 224, 224]

            # 模型推理
            with torch.no_grad():
                output = model_chest(input_tensor)
                prob = torch.softmax(output, dim=1)[0]
                pred_idx = torch.argmax(prob).item()
                pred_label = class_names[pred_idx]
                confidence = f"{prob[pred_idx].item() * 100:.2f}"

            result = {
                'label': pred_label,
                'confidence': confidence,
                'filename': filename
            }

            if 'user_id' in session:
                connection = get_db_connection()
                with connection.cursor() as cursor:
                    cursor.execute("UPDATE user SET chest_result=%s WHERE id=%s", (pred_label, session['user_id']))
                    connection.commit()
                connection.close()

    return render_template('chest-diagnosis.html', active_page='chest-diagnosis', result=result)


@app.route('/diabetes-diagnosis', methods=['GET', 'POST'])
def diabetes_diagnosis():
    result = None

    class DiabetesNet(nn.Module):
        def __init__(self):
            super(DiabetesNet, self).__init__()
            self.net = nn.Sequential(
                nn.Linear(8, 64),
                nn.ReLU(),
                nn.Dropout(0.3),

                nn.Linear(64, 32),
                nn.ReLU(),
                nn.Dropout(0.2),

                nn.Linear(32, 16),
                nn.ReLU(),

                nn.Linear(16, 1),

                nn.Sigmoid()
            )

        def forward(self, x):
            return self.net(x)

    # 加载模型
    global diabetes_model
    if 'diabetes_model' not in globals():
        diabetes_model = DiabetesNet()
        diabetes_model.load_state_dict(torch.load("models/diabetes_model.pth", map_location='cpu'))
        diabetes_model.eval()

    # 加载标准化器
    global diabetes_scaler
    if 'diabetes_scaler' not in globals():
        import joblib
        diabetes_scaler = joblib.load("models/diabetes_scaler.pkl")

    if request.method == 'POST':
        try:
            # 读取表单数据并转为 float
            user_input = [
                float(request.form.get("Pregnancies")),
                float(request.form.get("Glucose")),
                float(request.form.get("BloodPressure")),
                float(request.form.get("SkinThickness")),
                float(request.form.get("Insulin")),
                float(request.form.get("BMI")),
                float(request.form.get("DiabetesPedigreeFunction")),
                float(request.form.get("Age"))
            ]

            # 标准化
            input_scaled = diabetes_scaler.transform([user_input])  # shape: [1, 8]
            input_tensor = torch.tensor(input_scaled, dtype=torch.float32)

            # 模型预测
            with torch.no_grad():
                output = diabetes_model(input_tensor)
                prob = output.item()
                label = "患糖尿病" if prob >= 0.5 else "未患糖尿病"

            result = {
                "label": label,
                "confidence": f"{prob * 100:.2f}"
            }

        except Exception as e:
            result = {
                "label": "输入错误",
                "confidence": "0.00"
            }

            if 'user_id' in session:
                connection = get_db_connection()
                with connection.cursor() as cursor:
                    cursor.execute("UPDATE user SET diabetes_result=%s WHERE id=%s",
                                   (1 if prob >= 0.5 else 0, session['user_id']))
                    connection.commit()
                connection.close()

    return render_template("diabetes-diagnosis.html", active_page="diabetes-diagnosis", result=result)


@app.route('/heart-diagnosis', methods=['GET', 'POST'])
def heart_diagnosis():
    result = None

    # 模型加载（只加载一次）
    global heart_pipeline
    if 'heart_pipeline' not in globals():
        heart_pipeline = joblib.load('models/heart_disease_model.pkl')  # 路径按你实际情况修改

    if request.method == 'POST':
        try:
            # 1. 收集用户输入
            user_data = {
                "Age": float(request.form.get("Age")),
                "Sex": request.form.get("Sex"),
                "Chest pain type": request.form.get("Chest pain type"),
                "BP": float(request.form.get("BP")),
                "Cholesterol": float(request.form.get("Cholesterol")),
                "FBS over 120": int(request.form.get("FBS over 120")),
                "EKG results": request.form.get("EKG results"),
                "Max HR": float(request.form.get("Max HR")),
                "Exercise angina": request.form.get("Exercise angina"),
                "ST depression": float(request.form.get("ST depression")),
                "Slope of ST": request.form.get("Slope of ST"),
                "Number of vessels fluro": float(request.form.get("Number of vessels fluro")),
                "Thallium": request.form.get("Thallium")
            }

            # 2. 转为 DataFrame（模型接受 DataFrame 输入）
            df_input = pd.DataFrame([user_data])

            # 3. 推理
            prob = heart_pipeline.predict_proba(df_input)[0][1]
            label = "有心脏病风险" if prob >= 0.5 else "风险较低"

            result = {
                "label": label,
                "confidence": f"{prob * 100:.2f}"
            }

        except Exception as e:
            result = {
                "label": "输入错误或模型故障",
                "confidence": "0.00"
            }

            if 'user_id' in session:
                connection = get_db_connection()
                with connection.cursor() as cursor:
                    cursor.execute("UPDATE user SET heart_result=%s WHERE id=%s",
                                   (1 if prob >= 0.5 else 0, session['user_id']))
                    connection.commit()
                connection.close()

    return render_template("heart-diagnosis.html", active_page="heart-diagnosis", result=result)


if __name__ == '__main__':
    app.run(host=APP_HOST, port=APP_PORT, debug=APP_DEBUG)
