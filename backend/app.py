import pandas as pd
from flask import Flask, jsonify, render_template, send_from_directory, request, session, flash, redirect, url_for
import pymysql
import os
import sys
import json
from collections import Counter
from datetime import date, datetime, timedelta
from urllib.parse import urlparse
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
DISEASE_MAP_DATA_PATH = os.path.join(app.static_folder, 'data', 'disease_map.json')
DISEASE_MAP_UPDATED_AT = "2026-04-13"
DISEASE_MAP_NOTICE = (
    "本地图用于展示常见疾病防控知识和健康提示，不代表实时疫情、患病率或个人风险评估。"
    "具体疾病风险和防控要求请以国家及当地疾控、卫生健康部门发布的信息为准。"
)
DISEASE_MAP_SOURCES = [
    {"name": "国家卫生健康委员会", "url": "https://www.nhc.gov.cn/"},
    {"name": "中国疾病预防控制中心", "url": "https://www.chinacdc.cn/"},
    {"name": "中国疾控中心慢病中心", "url": "https://ncncd.chinacdc.cn/jkzt/"},
    {"name": "国家疾病预防控制局", "url": "https://www.ndcpa.gov.cn/"},
]
DISEASE_MAP_POPULATION_GROUPS = {
    "children": {
        "label": "儿童",
        "themes": ["疫苗接种", "手足口病", "近视防控", "意外伤害预防"],
    },
    "elderly": {
        "label": "老年人",
        "themes": ["高血压", "糖尿病", "跌倒预防", "流感疫苗"],
    },
    "workers": {
        "label": "上班族",
        "themes": ["久坐管理", "睡眠健康", "颈肩腰背痛", "心血管风险"],
    },
    "chronic": {
        "label": "慢病患者",
        "themes": ["规律服药", "指标监测", "饮食运动", "定期复诊"],
    },
}


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


def load_disease_map_file():
    with open(DISEASE_MAP_DATA_PATH, 'r', encoding='utf-8') as file:
        return json.load(file)


def parse_json_list(value):
    if not value:
        return []

    if isinstance(value, list):
        return value

    try:
        data = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []

    return data if isinstance(data, list) else []


def load_disease_map_from_db():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT name FROM disease_map_categories ORDER BY sort_order, id"
            )
            categories = [row["name"] for row in cursor.fetchall()]

            cursor.execute(
                """
                SELECT season_key, label, themes_json
                FROM disease_map_seasons
                ORDER BY sort_order, season_key
                """
            )
            seasons = {
                row["season_key"]: {
                    "label": row["label"],
                    "themes": parse_json_list(row["themes_json"]),
                }
                for row in cursor.fetchall()
            }

            cursor.execute(
                """
                SELECT name, category, summary, actions_json, links_json
                FROM disease_map_diseases
                ORDER BY sort_order, id
                """
            )
            diseases = [
                {
                    "name": row["name"],
                    "category": row["category"],
                    "summary": row["summary"],
                    "actions": parse_json_list(row["actions_json"]),
                    "links": parse_json_list(row["links_json"]),
                }
                for row in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT province_name, region, themes_json, actions_json, recommended_json, evidence_json
                FROM disease_map_province_profiles
                ORDER BY sort_order, province_name
                """
            )
            province_profiles = {
                row["province_name"]: {
                    "region": row["region"],
                    "themes": parse_json_list(row["themes_json"]),
                    "actions": parse_json_list(row["actions_json"]),
                    "recommended": parse_json_list(row["recommended_json"]),
                    "evidence": parse_json_list(row.get("evidence_json")),
                }
                for row in cursor.fetchall()
            }
    finally:
        connection.close()

    return {
        "updated_at": DISEASE_MAP_UPDATED_AT,
        "notice": DISEASE_MAP_NOTICE,
        "sources": DISEASE_MAP_SOURCES,
        "categories": categories,
        "seasons": seasons,
        "population_groups": DISEASE_MAP_POPULATION_GROUPS,
        "diseases": diseases,
        "province_profiles": province_profiles,
    }


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


CONTENT_SOURCES = (
    {"key": "news", "label": "新闻中心", "table": "news"},
    {"key": "policy", "label": "卫生政策", "table": "policy"},
    {"key": "knowledges", "label": "健康知识", "table": "knowledges"},
    {"key": "notice", "label": "通知公告", "table": "notice"},
)

SOURCE_LABEL_ALIASES = {
    "中国新闻网健康频道": "中国新闻网",
    "医学的温度": "中国新闻网",
    "大医生来了": "中国新闻网",
    "医药新观察": "中国新闻网",
    "神奇的中医药": "中国新闻网",
    "高质量发展看药企": "中国新闻网",
    "人民网健康频道": "人民网",
    "国家卫健委政务服务平台": "国家卫健委",
}

SOURCE_KEYWORD_LABELS = (
    ("中国新闻网", "中国新闻网"),
    ("人民网", "人民网"),
    ("国家卫健委", "国家卫健委"),
    ("国家卫生健康委员会", "国家卫健委"),
    ("健康资讯网", "健康资讯网"),
    ("健康时报网", "健康时报网"),
    ("中国疾病预防控制中心", "中国疾病预防控制中心"),
)

SOURCE_DOMAIN_LABELS = (
    ("chinanews.com", "中国新闻网"),
    ("people.com.cn", "人民网"),
    ("nhc.gov.cn", "国家卫健委"),
    ("chinacdc.cn", "中国疾病预防控制中心"),
    ("jkzx.org.cn", "健康资讯网"),
    ("jksb.com.cn", "健康时报网"),
)


def format_date_value(value):
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    return text[:10] if text else None


def build_dashboard_dates(days=30):
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return start_date, end_date, dates


def normalize_source_site(source, url):
    source_text = str(source).strip() if source else ""

    if source_text in SOURCE_LABEL_ALIASES:
        return SOURCE_LABEL_ALIASES[source_text]

    for keyword, label in SOURCE_KEYWORD_LABELS:
        if keyword in source_text:
            return label

    host = ""
    if url:
        parsed_url = urlparse(str(url))
        host = parsed_url.netloc.lower()
        if host.startswith("www."):
            host = host[4:]

    for domain, label in SOURCE_DOMAIN_LABELS:
        if host == domain or host.endswith(f".{domain}"):
            return label

    if source_text:
        return source_text

    return host or "未知来源"


def build_source_distribution(rows):
    counter = Counter()

    for row in rows:
        label = normalize_source_site(row.get("source"), row.get("url"))
        counter[label] += 1

    total = sum(counter.values())
    return [
        {
            "label": label,
            "value": count,
            "percent": round(count / total * 100, 2) if total else 0,
        }
        for label, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


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


@app.route('/api/dashboard/overview')
def api_dashboard_overview():
    start_date, end_date, trend_dates = build_dashboard_dates(30)
    summary = []
    distribution = []
    source_rows = []
    latest_by_type = {}
    trend_maps = {source["key"]: {day: 0 for day in trend_dates} for source in CONTENT_SOURCES}

    db = get_db_connection()
    cursor = db.cursor()

    try:
        for source in CONTENT_SOURCES:
            cursor.execute(
                f"SELECT COUNT(*) AS total, MAX(DATE(publish_time)) AS latest_date FROM {source['table']}"
            )
            stats = cursor.fetchone() or {}
            total = int(stats.get("total") or 0)
            latest_date = format_date_value(stats.get("latest_date"))

            summary.append({
                "key": source["key"],
                "label": source["label"],
                "total": total,
                "latest_date": latest_date,
            })
            distribution.append({
                "key": source["key"],
                "label": source["label"],
                "value": total,
            })

            try:
                cursor.execute(f"SELECT source, url FROM {source['table']}")
            except pymysql.err.OperationalError as error:
                if not error.args or error.args[0] != 1054:
                    raise
                cursor.execute(f"SELECT url FROM {source['table']}")

            source_rows.extend(cursor.fetchall())

            cursor.execute(
                f"""
                SELECT DATE(publish_time) AS publish_date, COUNT(*) AS total
                FROM {source['table']}
                WHERE publish_time IS NOT NULL
                  AND DATE(publish_time) BETWEEN %s AND %s
                GROUP BY DATE(publish_time)
                ORDER BY publish_date
                """,
                (start_date, end_date)
            )
            for row in cursor.fetchall():
                publish_date = format_date_value(row.get("publish_date"))
                if publish_date in trend_maps[source["key"]]:
                    trend_maps[source["key"]][publish_date] = int(row.get("total") or 0)

            cursor.execute(
                f"""
                SELECT title, publish_time, url
                FROM {source['table']}
                WHERE title IS NOT NULL AND title <> ''
                ORDER BY publish_time IS NULL, publish_time DESC
                LIMIT 5
                """
            )
            latest_by_type[source["key"]] = [
                {
                    "type": source["key"],
                    "type_label": source["label"],
                    "title": row.get("title"),
                    "publish_time": format_date_value(row.get("publish_time")),
                    "url": row.get("url"),
                }
                for row in cursor.fetchall()
            ]

        latest_sql_parts = []
        for source in CONTENT_SOURCES:
            latest_sql_parts.append(
                f"""
                SELECT
                    '{source['key']}' AS type,
                    '{source['label']}' AS type_label,
                    title,
                    publish_time,
                    url
                FROM {source['table']}
                WHERE title IS NOT NULL AND title <> ''
                """
            )

        cursor.execute(
            f"""
            SELECT type, type_label, title, publish_time, url
            FROM (
                {' UNION ALL '.join(latest_sql_parts)}
            ) AS latest_content
            ORDER BY publish_time IS NULL, publish_time DESC
            LIMIT 5
            """
        )
        latest = []
        for row in cursor.fetchall():
            latest.append({
                "type": row.get("type"),
                "type_label": row.get("type_label"),
                "title": row.get("title"),
                "publish_time": format_date_value(row.get("publish_time")),
                "url": row.get("url"),
            })

    finally:
        cursor.close()
        db.close()

    return jsonify({
        "summary": summary,
        "updated_at": max((item["latest_date"] for item in summary if item["latest_date"]), default=None),
        "trend": {
            "dates": trend_dates,
            "series": [
                {
                    "key": source["key"],
                    "label": source["label"],
                    "data": [trend_maps[source["key"]][day] for day in trend_dates],
                }
                for source in CONTENT_SOURCES
            ],
        },
        "distribution": distribution,
        "source_distribution": build_source_distribution(source_rows),
        "latest": latest,
        "latest_by_type": latest_by_type,
    })


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


def get_home_card_items(limit=15):
    home_cards = {source["key"]: [] for source in CONTENT_SOURCES}
    db = get_db_connection()
    cursor = db.cursor()

    try:
        for source in CONTENT_SOURCES:
            cursor.execute(
                f"""
                SELECT title, url
                FROM {source['table']}
                WHERE title IS NOT NULL AND title <> ''
                ORDER BY publish_time IS NULL, publish_time DESC
                LIMIT %s
                """,
                (limit,)
            )
            home_cards[source["key"]] = cursor.fetchall()
    finally:
        cursor.close()
        db.close()

    return home_cards


def get_request_page_args(default_size=10, max_size=50):
    page = max(int(request.args.get('page', 1)), 1)
    size = max(min(int(request.args.get('size', default_size)), max_size), 1)
    return page, size, (page - 1) * size


def get_search_keyword():
    return request.args.get('q', '').strip()[:100]


def build_content_search_clause(keyword):
    if not keyword:
        return "", []

    like_keyword = f"%{keyword}%"
    return " WHERE title LIKE %s", [like_keyword]


def jsonify_paginated_content(table):
    page, size, offset = get_request_page_args()
    keyword = get_search_keyword()
    where_sql, search_params = build_content_search_clause(keyword)

    db = get_db_connection()
    cursor = db.cursor()

    try:
        cursor.execute(f"SELECT COUNT(*) AS total FROM {table}{where_sql}", search_params)
        total = cursor.fetchone()['total']

        cursor.execute(
            f"""
            SELECT title, content, publish_time, url
            FROM {table}
            {where_sql}
            ORDER BY publish_time DESC
            LIMIT %s OFFSET %s
            """,
            search_params + [size, offset]
        )
        result = cursor.fetchall()
    finally:
        cursor.close()
        db.close()

    for item in result:
        item['summary'] = (item['content'][:100] + '...') if item['content'] else "（暂无正文内容）"

    return jsonify({
        "total": total,
        "data": result,
        "keyword": keyword,
    })


@app.route('/api/knowledges')
def api_knowledges():
    return jsonify_paginated_content("knowledges")


@app.route('/api/news')
def api_news():
    return jsonify_paginated_content("news")


@app.route('/api/notice')
def api_notice():
    return jsonify_paginated_content("notice")


@app.route('/api/policy')
def api_policy():
    return jsonify_paginated_content("policy")


def ensure_user_health_test_schema(cursor):
    cursor.execute("SHOW COLUMNS FROM user")
    existing_columns = {row["Field"]: row for row in cursor.fetchall()}

    if "chest_result" in existing_columns:
        cursor.execute("ALTER TABLE user MODIFY COLUMN chest_result VARCHAR(50) NULL DEFAULT NULL")
    if "diabetes_result" in existing_columns:
        cursor.execute("ALTER TABLE user MODIFY COLUMN diabetes_result TINYINT NULL DEFAULT NULL")
    if "heart_result" in existing_columns:
        cursor.execute("ALTER TABLE user MODIFY COLUMN heart_result TINYINT NULL DEFAULT NULL")

    columns = {
        "chest_confidence": "ALTER TABLE user ADD COLUMN chest_confidence DECIMAL(5,2) NULL",
        "diabetes_confidence": "ALTER TABLE user ADD COLUMN diabetes_confidence DECIMAL(5,2) NULL",
        "heart_confidence": "ALTER TABLE user ADD COLUMN heart_confidence DECIMAL(5,2) NULL",
    }

    for column, sql in columns.items():
        if column not in existing_columns:
            cursor.execute(sql)

    if "chest_tested" in existing_columns:
        cursor.execute(
            """
            UPDATE user
            SET chest_result = NULL, chest_confidence = NULL
            WHERE chest_tested = 0 AND (chest_result IS NULL OR chest_result = '无病')
            """
        )
        cursor.execute("ALTER TABLE user DROP COLUMN chest_tested")

    if "diabetes_tested" in existing_columns:
        cursor.execute(
            """
            UPDATE user
            SET diabetes_result = NULL, diabetes_confidence = NULL
            WHERE diabetes_tested = 0 AND (diabetes_result IS NULL OR diabetes_result = 0)
            """
        )
        cursor.execute("ALTER TABLE user DROP COLUMN diabetes_tested")

    if "heart_tested" in existing_columns:
        cursor.execute(
            """
            UPDATE user
            SET heart_result = NULL, heart_confidence = NULL
            WHERE heart_tested = 0 AND (heart_result IS NULL OR heart_result = 0)
            """
        )
        cursor.execute("ALTER TABLE user DROP COLUMN heart_tested")

    cursor.execute(
        """
        UPDATE user
        SET chest_result = NULL
        WHERE chest_result = '无病' AND chest_confidence IS NULL
        """
    )
    cursor.execute(
        """
        UPDATE user
        SET diabetes_result = NULL
        WHERE diabetes_result = 0 AND diabetes_confidence IS NULL
        """
    )
    cursor.execute(
        """
        UPDATE user
        SET heart_result = NULL
        WHERE heart_result = 0 AND heart_confidence IS NULL
        """
    )


# 注册页面
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        connection = get_db_connection()
        with connection.cursor() as cursor:
            ensure_user_health_test_schema(cursor)
            cursor.execute("SELECT id FROM user WHERE username=%s", (username,))
            if cursor.fetchone():
                flash("用户名已存在")
                return redirect(url_for('register'))
            cursor.execute(
                "INSERT INTO user (username, password) VALUES (%s, %s)",
                (username, password)
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

        if not row:
            return {}

        has_risk = (
                (row.get('chest_result') is not None and row.get('chest_result') != '无病')
                or row.get('diabetes_result') == 1
                or row.get('heart_result') == 1
        )
        user_status = {
            'chest_result': row.get('chest_result') or '无病',
            'diabetes_result': int(row.get('diabetes_result') or 0),
            'heart_result': int(row.get('heart_result') or 0),
        }
        return {'user_status': user_status, 'has_health_risk': has_risk}
    return {}


def get_latest_bmi_record(cursor, user_id):
    cursor.execute(
        """
        SELECT bmi, category, created_at
        FROM bmi_records
        WHERE user_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (user_id,),
    )
    return cursor.fetchone()


def get_latest_sleep_record(cursor, user_id):
    cursor.execute(
        """
        SELECT score, category, sleep_duration, created_at
        FROM sleep_quality_records
        WHERE user_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (user_id,),
    )
    return cursor.fetchone()


def get_today_water_state_for_radar(cursor, user_id):
    today = date.today()
    cursor.execute(
        """
        SELECT target_ml
        FROM water_daily_targets
        WHERE user_id = %s AND target_date = %s
        """,
        (user_id, today),
    )
    target = cursor.fetchone()
    if not target:
        return None

    cursor.execute(
        """
        SELECT COALESCE(SUM(amount_ml), 0) AS current_ml
        FROM water_intake_records
        WHERE user_id = %s AND target_date = %s
        """,
        (user_id, today),
    )
    current = cursor.fetchone() or {}
    return {
        "target_ml": int(target.get("target_ml") or 0),
        "current_ml": int(current.get("current_ml") or 0),
    }


def get_table_exists(cursor, table_name):
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    return bool(cursor.fetchone())


def build_health_radar_metric(label, value, detail):
    return {
        "label": label,
        "value": max(0, min(int(round(value)), 100)),
        "detail": detail,
    }


def build_disease_risk_metric(label, tested, healthy, confidence, no_test_detail, healthy_detail, abnormal_detail):
    if not tested:
        return build_health_radar_metric(label, 60, no_test_detail)

    if healthy:
        return build_health_radar_metric(label, 100, healthy_detail)

    if confidence is None:
        return build_health_radar_metric(label, 35, f"{abnormal_detail}，置信度未记录")

    confidence_value = max(0, min(float(confidence), 100))
    return build_health_radar_metric(
        label,
        100 - confidence_value,
        f"{abnormal_detail}，置信度 {confidence_value:.1f}%"
    )


@app.route('/api/health-risk-radar')
def api_health_risk_radar():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"authenticated": False, "metrics": [], "message": "登录后可查看健康风险雷达图。"}), 401

    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            ensure_user_health_test_schema(cursor)
            cursor.execute(
                """
                SELECT chest_result, diabetes_result, heart_result,
                       chest_confidence, diabetes_confidence, heart_confidence
                FROM user
                WHERE id = %s
                """,
                (user_id,),
            )
            user_row = cursor.fetchone() or {}

            bmi_record = get_latest_bmi_record(cursor, user_id) if get_table_exists(cursor, "bmi_records") else None
            sleep_record = get_latest_sleep_record(cursor, user_id) if get_table_exists(cursor, "sleep_quality_records") else None
            water_state = None
            if get_table_exists(cursor, "water_daily_targets") and get_table_exists(cursor, "water_intake_records"):
                water_state = get_today_water_state_for_radar(cursor, user_id)
    finally:
        db.close()

    if bmi_record:
        bmi_value = float(bmi_record.get("bmi") or 0)
        bmi_category = bmi_record.get("category") or "未知"
        if 18.5 <= bmi_value < 25:
            weight_score = 100
        elif bmi_value < 18.5:
            weight_score = 68
        elif bmi_value < 30:
            weight_score = 58
        else:
            weight_score = 38
        weight_detail = f"最近 BMI {bmi_value:.1f}，{bmi_category}"
    else:
        weight_score = 60
        weight_detail = "暂无 BMI 记录"

    if sleep_record:
        sleep_score_raw = int(sleep_record.get("score") or 0)
        sleep_score = sleep_score_raw / 18 * 100
        sleep_detail = f"最近睡眠评分 {sleep_score_raw}/18，{sleep_record.get('category') or '未知'}"
    else:
        sleep_score = 60
        sleep_detail = "暂无睡眠评估记录"

    if water_state and water_state["target_ml"]:
        progress = water_state["current_ml"] / water_state["target_ml"] * 100
        if 80 <= progress <= 120:
            water_score = 92
        elif 60 <= progress < 80:
            water_score = 72
        elif 120 < progress <= 150:
            water_score = 78
        else:
            water_score = 48
        water_detail = f"今日饮水完成 {int(round(progress))}%"
    else:
        water_score = 60
        water_detail = "暂无今日饮水目标"

    chest_result = user_row.get("chest_result")
    diabetes_result = user_row.get("diabetes_result")
    heart_result = user_row.get("heart_result")
    chest_tested = chest_result is not None
    diabetes_tested = diabetes_result is not None
    heart_tested = heart_result is not None
    chest_healthy = chest_tested and chest_result == "无病"
    diabetes_healthy = diabetes_tested and int(diabetes_result) == 0
    heart_healthy = heart_tested and int(heart_result) == 0
    chest_confidence = user_row.get("chest_confidence")
    diabetes_confidence = user_row.get("diabetes_confidence")
    heart_confidence = user_row.get("heart_confidence")

    metrics = [
        build_health_radar_metric("体重管理", weight_score, weight_detail),
        build_health_radar_metric("睡眠质量", sleep_score, sleep_detail),
        build_health_radar_metric("饮水习惯", water_score, water_detail),
        build_disease_risk_metric(
            "胸部疾病风险",
            chest_tested,
            chest_healthy,
            chest_confidence,
            "胸部疾病暂无测试结果",
            "胸部疾病测试未发现风险",
            f"胸部检测异常：{chest_result or '未知'}",
        ),
        build_disease_risk_metric(
            "糖尿病风险",
            diabetes_tested,
            diabetes_healthy,
            diabetes_confidence,
            "糖尿病暂无测试结果",
            "糖尿病测试未发现风险",
            "糖尿病检测异常",
        ),
        build_disease_risk_metric(
            "心脏疾病风险",
            heart_tested,
            heart_healthy,
            heart_confidence,
            "心脏疾病暂无测试结果",
            "心脏疾病测试未发现风险",
            "心脏疾病诊断异常",
        ),
    ]

    return jsonify({
        "authenticated": True,
        "metrics": metrics,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "note": "评分越高代表当前健康状态越好，结果仅供健康管理参考。",
    })


@app.after_request
def inject_health_radar_assets(response):
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type or response.direct_passthrough:
        return response

    try:
        body = response.get_data(as_text=True)
    except RuntimeError:
        return response

    script_tag = '<script src="/static/js/health_radar.js"></script>'
    if "user-info" in body and script_tag not in body and "</body>" in body:
        body = body.replace("</body>", f"{script_tag}\n</body>")
        response.set_data(body)
        response.headers["Content-Length"] = str(len(response.get_data()))

    return response


@app.route('/')
def home():
    try:
        home_cards = get_home_card_items()
    except Exception:
        app.logger.exception("Failed to load home card items")
        home_cards = {source["key"]: [] for source in CONTENT_SOURCES}

    return render_template('home.html', active_page='home', home_cards=home_cards)


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


@app.route('/disease-map')
def disease_map():
    return render_template('disease-map.html', active_page='disease-map')


@app.route('/api/disease-map')
def api_disease_map():
    try:
        data = load_disease_map_from_db()
    except pymysql.MySQLError:
        app.logger.exception("Failed to load disease map data from database, falling back to static JSON")
        data = load_disease_map_file()
    return jsonify(data)


@app.route('/about')
def about():
    return render_template('about.html', active_page='about')


@app.route('/bmi')
def bmi():
    return render_template('bmi.html', active_page='bmi')


def ensure_bmi_records_schema(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bmi_records (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            height_cm DECIMAL(6,2) NOT NULL,
            weight_kg DECIMAL(6,2) NOT NULL,
            bmi DECIMAL(5,2) NOT NULL,
            category VARCHAR(30) NOT NULL,
            created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_bmi_records_user_time (user_id, created_at, id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def get_bmi_category(bmi_value):
    if bmi_value < 18.5:
        return "体重过轻"
    if bmi_value < 25:
        return "正常范围"
    if bmi_value < 30:
        return "超重"
    return "肥胖"


def format_bmi_record(row):
    created_at = row.get("created_at")
    if hasattr(created_at, "strftime"):
        created_label = created_at.strftime("%m-%d %H:%M")
        created_value = created_at.strftime("%Y-%m-%d %H:%M:%S")
    else:
        created_value = str(created_at or "")
        created_label = created_value[5:16] if len(created_value) >= 16 else created_value

    return {
        "id": row.get("id"),
        "height_cm": float(row.get("height_cm") or 0),
        "weight_kg": float(row.get("weight_kg") or 0),
        "bmi": float(row.get("bmi") or 0),
        "category": row.get("category") or "",
        "created_at": created_value,
        "label": created_label,
    }


def fetch_bmi_records(cursor, user_id, limit=10):
    cursor.execute(
        """
        SELECT id, height_cm, weight_kg, bmi, category, created_at
        FROM bmi_records
        WHERE user_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT %s
        """,
        (user_id, limit),
    )
    return [format_bmi_record(row) for row in reversed(cursor.fetchall())]


@app.route('/api/bmi-records', methods=['GET', 'POST'])
def api_bmi_records():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({
            "authenticated": False,
            "records": [],
            "message": "登录后可保存并查看最近10次BMI变化趋势。",
        }), 401 if request.method == 'POST' else 200

    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            ensure_bmi_records_schema(cursor)

            if request.method == 'POST':
                payload = request.get_json(silent=True) or {}
                try:
                    height_cm = float(payload.get("height_cm"))
                    weight_kg = float(payload.get("weight_kg"))
                except (TypeError, ValueError):
                    return jsonify({"error": "身高和体重必须为有效数字。"}), 400

                if not 50 <= height_cm <= 250 or not 10 <= weight_kg <= 300:
                    return jsonify({"error": "请输入合理范围内的身高和体重。"}), 400

                height_m = height_cm / 100
                bmi_value = round(weight_kg / (height_m * height_m), 2)
                category = get_bmi_category(bmi_value)
                cursor.execute(
                    """
                    INSERT INTO bmi_records (user_id, height_cm, weight_kg, bmi, category)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (user_id, height_cm, weight_kg, bmi_value, category),
                )
                db.commit()

            records = fetch_bmi_records(cursor, user_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return jsonify({
        "authenticated": True,
        "records": records,
        "normal_range": {"min": 18.5, "max": 24.9},
    })


@app.route('/water-intake')
def water_intake():
    return render_template('water-intake.html', active_page='water-intake')


WATER_ACTIVITY_MULTIPLIERS = {
    "sedentary": 1.0,
    "light": 1.1,
    "moderate": 1.2,
    "active": 1.3,
    "extra": 1.4,
}

WATER_CLIMATE_MULTIPLIERS = {
    "temperate": 1.0,
    "hot": 1.1,
    "dry": 0.95,
}


def ensure_water_intake_schema(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS water_daily_targets (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            target_date DATE NOT NULL,
            unit_system VARCHAR(20) NOT NULL,
            gender VARCHAR(20) NOT NULL,
            weight_value DECIMAL(7,2) NOT NULL,
            weight_kg DECIMAL(7,2) NOT NULL,
            activity_level VARCHAR(30) NOT NULL,
            climate_level VARCHAR(30) NOT NULL,
            pregnancy_status VARCHAR(10) NOT NULL,
            target_ml INT UNSIGNED NOT NULL,
            created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_water_daily_target_user_date (user_id, target_date),
            INDEX idx_water_daily_targets_user_date (user_id, target_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS water_intake_records (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            target_date DATE NOT NULL,
            amount_ml INT UNSIGNED NOT NULL,
            created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_water_intake_records_user_date (user_id, target_date, created_at, id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def calculate_water_target_ml(unit_system, gender, weight_value, activity_level, climate_level, pregnancy_status):
    if unit_system not in ("metric", "imperial"):
        raise ValueError("单位系统无效。")
    if gender not in ("male", "female"):
        raise ValueError("性别选项无效。")
    if activity_level not in WATER_ACTIVITY_MULTIPLIERS:
        raise ValueError("活动水平选项无效。")
    if climate_level not in WATER_CLIMATE_MULTIPLIERS:
        raise ValueError("气候选项无效。")
    if gender == "male":
        pregnancy_status = "no"
    elif pregnancy_status not in ("yes", "no"):
        raise ValueError("怀孕/哺乳选项无效。")

    if unit_system == "metric":
        if not 20 <= weight_value <= 250:
            raise ValueError("请输入合理范围内的体重。")
        weight_kg = weight_value
        weight_lb = weight_value * 2.20462
    else:
        if not 44 <= weight_value <= 550:
            raise ValueError("请输入合理范围内的体重。")
        weight_lb = weight_value
        weight_kg = weight_value * 0.45359237

    base_oz = weight_lb * (0.5 if gender == "male" else 0.45)
    total_oz = (
        base_oz
        * WATER_ACTIVITY_MULTIPLIERS[activity_level]
        * WATER_CLIMATE_MULTIPLIERS[climate_level]
        * (1.3 if pregnancy_status == "yes" else 1.0)
    )
    target_ml = int(round(total_oz * 29.5735))
    return target_ml, round(weight_kg, 2)


def format_water_target(row):
    if not row:
        return None

    target_date = row.get("target_date")
    if hasattr(target_date, "strftime"):
        target_date = target_date.strftime("%Y-%m-%d")

    return {
        "target_date": target_date,
        "unit_system": row.get("unit_system"),
        "gender": row.get("gender"),
        "weight_value": float(row.get("weight_value") or 0),
        "weight_kg": float(row.get("weight_kg") or 0),
        "activity_level": row.get("activity_level"),
        "climate_level": row.get("climate_level"),
        "pregnancy_status": row.get("pregnancy_status"),
        "target_ml": int(row.get("target_ml") or 0),
    }


def format_water_record(row):
    created_at = row.get("created_at")
    if hasattr(created_at, "strftime"):
        created_label = created_at.strftime("%H:%M")
        created_value = created_at.strftime("%Y-%m-%d %H:%M:%S")
    else:
        created_value = str(created_at or "")
        created_label = created_value[11:16] if len(created_value) >= 16 else created_value

    return {
        "id": row.get("id"),
        "amount_ml": int(row.get("amount_ml") or 0),
        "created_at": created_value,
        "label": created_label,
    }


def fetch_water_today_state(cursor, user_id):
    today = date.today()
    cursor.execute(
        """
        SELECT target_date, unit_system, gender, weight_value, weight_kg,
               activity_level, climate_level, pregnancy_status, target_ml
        FROM water_daily_targets
        WHERE user_id = %s AND target_date = %s
        """,
        (user_id, today),
    )
    target = format_water_target(cursor.fetchone())

    cursor.execute(
        """
        SELECT COALESCE(SUM(amount_ml), 0) AS total_ml
        FROM water_intake_records
        WHERE user_id = %s AND target_date = %s
        """,
        (user_id, today),
    )
    current_ml = int((cursor.fetchone() or {}).get("total_ml") or 0)

    cursor.execute(
        """
        SELECT id, amount_ml, created_at
        FROM water_intake_records
        WHERE user_id = %s AND target_date = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 10
        """,
        (user_id, today),
    )
    records = [format_water_record(row) for row in cursor.fetchall()]

    target_ml = int(target["target_ml"]) if target else 0
    return {
        "authenticated": True,
        "target": target,
        "target_ml": target_ml,
        "current_ml": current_ml,
        "remaining_ml": max(target_ml - current_ml, 0) if target_ml else 0,
        "progress_percent": round(current_ml / target_ml * 100) if target_ml else 0,
        "records": records,
        "target_date": today.strftime("%Y-%m-%d"),
    }


@app.route('/api/water-intake/today')
def api_water_intake_today():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({
            "authenticated": False,
            "target": None,
            "target_ml": 0,
            "current_ml": 0,
            "remaining_ml": 0,
            "progress_percent": 0,
            "records": [],
            "message": "登录后可保存今日饮水目标和饮水记录。",
        })

    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            ensure_water_intake_schema(cursor)
            state = fetch_water_today_state(cursor, user_id)
    finally:
        db.close()

    return jsonify(state)


@app.route('/api/water-intake/target', methods=['POST'])
def api_water_intake_target():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"error": "登录后可保存今日饮水目标。"}), 401

    payload = request.get_json(silent=True) or {}
    try:
        unit_system = str(payload.get("unit_system") or "")
        gender = str(payload.get("gender") or "")
        weight_value = float(payload.get("weight_value"))
        activity_level = str(payload.get("activity_level") or "")
        climate_level = str(payload.get("climate_level") or "")
        pregnancy_status = str(payload.get("pregnancy_status") or "")
        if gender == "male":
            pregnancy_status = "no"
        target_ml, weight_kg = calculate_water_target_ml(
            unit_system,
            gender,
            weight_value,
            activity_level,
            climate_level,
            pregnancy_status,
        )
    except (TypeError, ValueError) as error:
        return jsonify({"error": str(error) or "饮水目标参数无效。"}), 400

    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            ensure_water_intake_schema(cursor)
            cursor.execute(
                """
                INSERT INTO water_daily_targets
                    (user_id, target_date, unit_system, gender, weight_value, weight_kg,
                     activity_level, climate_level, pregnancy_status, target_ml)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    unit_system = VALUES(unit_system),
                    gender = VALUES(gender),
                    weight_value = VALUES(weight_value),
                    weight_kg = VALUES(weight_kg),
                    activity_level = VALUES(activity_level),
                    climate_level = VALUES(climate_level),
                    pregnancy_status = VALUES(pregnancy_status),
                    target_ml = VALUES(target_ml)
                """,
                (
                    user_id,
                    date.today(),
                    unit_system,
                    gender,
                    weight_value,
                    weight_kg,
                    activity_level,
                    climate_level,
                    pregnancy_status,
                    target_ml,
                ),
            )
            db.commit()
            state = fetch_water_today_state(cursor, user_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return jsonify(state)


@app.route('/api/water-intake/record', methods=['POST'])
def api_water_intake_record():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"error": "登录后可记录饮水量。"}), 401

    payload = request.get_json(silent=True) or {}
    try:
        amount_ml = int(payload.get("amount_ml"))
    except (TypeError, ValueError):
        return jsonify({"error": "饮水量必须为有效数字。"}), 400

    if amount_ml <= 0 or amount_ml > 3000:
        return jsonify({"error": "单次饮水量应在 1-3000 ml 之间。"}), 400

    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            ensure_water_intake_schema(cursor)
            cursor.execute(
                """
                SELECT id FROM water_daily_targets
                WHERE user_id = %s AND target_date = %s
                """,
                (user_id, date.today()),
            )
            if not cursor.fetchone():
                return jsonify({"error": "请先计算并保存今日推荐饮水量。"}), 400

            cursor.execute(
                """
                INSERT INTO water_intake_records (user_id, target_date, amount_ml)
                VALUES (%s, %s, %s)
                """,
                (user_id, date.today(), amount_ml),
            )
            db.commit()
            state = fetch_water_today_state(cursor, user_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return jsonify(state)


@app.route('/sleep-quality')
def sleep_quality():
    return render_template('sleep-quality.html', active_page='sleep-quality')


def ensure_sleep_records_schema(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sleep_quality_records (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            sleep_duration DECIMAL(4,2) NOT NULL,
            sleep_latency SMALLINT UNSIGNED NOT NULL,
            awakenings SMALLINT UNSIGNED NOT NULL,
            morning_feeling TINYINT UNSIGNED NOT NULL,
            daytime_function TINYINT UNSIGNED NOT NULL,
            satisfaction TINYINT UNSIGNED NOT NULL,
            score TINYINT UNSIGNED NOT NULL,
            category VARCHAR(20) NOT NULL,
            created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_sleep_quality_records_user_time (user_id, created_at, id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def calculate_sleep_score(sleep_duration, sleep_latency, awakenings, morning_feeling, daytime_function, satisfaction):
    if not 0 <= sleep_duration <= 24:
        raise ValueError("睡眠时长应在 0-24 小时之间。")
    if not 0 <= sleep_latency <= 180:
        raise ValueError("入睡时间应在 0-180 分钟之间。")
    if not 0 <= awakenings <= 20:
        raise ValueError("夜间醒来次数应在 0-20 次之间。")
    for value in (morning_feeling, daytime_function, satisfaction):
        if value not in (1, 2, 3):
            raise ValueError("主观评价参数无效。")

    duration_score = 3 if sleep_duration >= 7 else 2 if sleep_duration >= 6 else 1 if sleep_duration >= 5 else 0
    latency_score = 3 if sleep_latency <= 15 else 2 if sleep_latency <= 30 else 1 if sleep_latency <= 45 else 0
    awakenings_score = 3 if awakenings == 0 else 2 if awakenings == 1 else 1 if awakenings == 2 else 0

    return (
        duration_score
        + latency_score
        + awakenings_score
        + morning_feeling
        + daytime_function
        + satisfaction
    )


def get_sleep_category(score):
    if score >= 14:
        return "优秀"
    if score >= 10:
        return "一般"
    return "较差"


def format_sleep_record(row):
    created_at = row.get("created_at")
    if hasattr(created_at, "strftime"):
        created_label = created_at.strftime("%m-%d %H:%M")
        created_value = created_at.strftime("%Y-%m-%d %H:%M:%S")
    else:
        created_value = str(created_at or "")
        created_label = created_value[5:16] if len(created_value) >= 16 else created_value

    return {
        "id": row.get("id"),
        "sleep_duration": float(row.get("sleep_duration") or 0),
        "sleep_latency": int(row.get("sleep_latency") or 0),
        "awakenings": int(row.get("awakenings") or 0),
        "morning_feeling": int(row.get("morning_feeling") or 0),
        "daytime_function": int(row.get("daytime_function") or 0),
        "satisfaction": int(row.get("satisfaction") or 0),
        "score": int(row.get("score") or 0),
        "category": row.get("category") or "",
        "created_at": created_value,
        "label": created_label,
    }


def fetch_sleep_records(cursor, user_id, limit=10):
    cursor.execute(
        """
        SELECT id, sleep_duration, sleep_latency, awakenings, morning_feeling,
               daytime_function, satisfaction, score, category, created_at
        FROM sleep_quality_records
        WHERE user_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT %s
        """,
        (user_id, limit),
    )
    return [format_sleep_record(row) for row in reversed(cursor.fetchall())]


@app.route('/api/sleep-records', methods=['GET', 'POST'])
def api_sleep_records():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({
            "authenticated": False,
            "records": [],
            "message": "登录后可保存并查看最近10次睡眠质量变化趋势。",
        }), 401 if request.method == 'POST' else 200

    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            ensure_sleep_records_schema(cursor)

            if request.method == 'POST':
                payload = request.get_json(silent=True) or {}
                try:
                    sleep_duration = float(payload.get("sleep_duration"))
                    sleep_latency = int(payload.get("sleep_latency"))
                    awakenings = int(payload.get("awakenings"))
                    morning_feeling = int(payload.get("morning_feeling"))
                    daytime_function = int(payload.get("daytime_function"))
                    satisfaction = int(payload.get("satisfaction"))
                except (TypeError, ValueError):
                    return jsonify({"error": "睡眠评估参数必须为有效数字。"}), 400

                score = calculate_sleep_score(
                    sleep_duration,
                    sleep_latency,
                    awakenings,
                    morning_feeling,
                    daytime_function,
                    satisfaction,
                )
                category = get_sleep_category(score)
                cursor.execute(
                    """
                    INSERT INTO sleep_quality_records
                        (user_id, sleep_duration, sleep_latency, awakenings, morning_feeling,
                         daytime_function, satisfaction, score, category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        sleep_duration,
                        sleep_latency,
                        awakenings,
                        morning_feeling,
                        daytime_function,
                        satisfaction,
                        score,
                        category,
                    ),
                )
                db.commit()

            records = fetch_sleep_records(cursor, user_id)
    except ValueError as error:
        db.rollback()
        return jsonify({"error": str(error)}), 400
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return jsonify({
        "authenticated": True,
        "records": records,
        "score_range": {"min": 0, "max": 18},
        "quality_ranges": [
            {"label": "较差", "min": 0, "max": 9},
            {"label": "一般", "min": 10, "max": 13},
            {"label": "优秀", "min": 14, "max": 18},
        ],
    })


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
                confidence_value = prob[pred_idx].item() * 100
                confidence = f"{confidence_value:.2f}"

            result = {
                'label': pred_label,
                'confidence': confidence,
                'filename': filename
            }

            if 'user_id' in session:
                connection = get_db_connection()
                with connection.cursor() as cursor:
                    ensure_user_health_test_schema(cursor)
                    cursor.execute(
                        """
                        UPDATE user
                        SET chest_result=%s, chest_confidence=%s
                        WHERE id=%s
                        """,
                        (pred_label, confidence_value, session['user_id'])
                    )
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
                confidence_value = prob * 100
                label = "患糖尿病" if prob >= 0.5 else "未患糖尿病"

            result = {
                "label": label,
                "confidence": f"{confidence_value:.2f}"
            }

            if 'user_id' in session:
                connection = get_db_connection()
                with connection.cursor() as cursor:
                    ensure_user_health_test_schema(cursor)
                    cursor.execute(
                        """
                        UPDATE user
                        SET diabetes_result=%s, diabetes_confidence=%s
                        WHERE id=%s
                        """,
                        (1 if prob >= 0.5 else 0, confidence_value, session['user_id'])
                    )
                    connection.commit()
                connection.close()

        except Exception as e:
            result = {
                "label": "输入错误",
                "confidence": "0.00"
            }

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
            confidence_value = prob * 100
            label = "有心脏病风险" if prob >= 0.5 else "风险较低"

            result = {
                "label": label,
                "confidence": f"{confidence_value:.2f}"
            }

            if 'user_id' in session:
                connection = get_db_connection()
                with connection.cursor() as cursor:
                    ensure_user_health_test_schema(cursor)
                    cursor.execute(
                        """
                        UPDATE user
                        SET heart_result=%s, heart_confidence=%s
                        WHERE id=%s
                        """,
                        (1 if prob >= 0.5 else 0, confidence_value, session['user_id'])
                    )
                    connection.commit()
                connection.close()

        except Exception as e:
            result = {
                "label": "输入错误或模型故障",
                "confidence": "0.00"
            }

    return render_template("heart-diagnosis.html", active_page="heart-diagnosis", result=result)


if __name__ == '__main__':
    app.run(host=APP_HOST, port=APP_PORT, debug=APP_DEBUG)
