import schedule
import time
import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re

# 数据库配置
DB_CONFIG = {
    'host': '119.3.163.155',
    'user': 'root',
    'password': 'nineone4536251',
    'database': 'health',
    'charset': 'utf8mb4'
}


# 数据库连接函数
def get_db_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)


# 爬虫函数：抓取中国新闻网健康新闻标题 + 正文
def crawl_chinanews():
    url = "https://www.chinanews.com/life/"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = 'utf-8'
    except Exception as e:
        print(f"[错误] 请求失败: {e}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    articles = []
    for li in soup.find_all('li'):
        a_tag = li.find('a')
        if not a_tag:
            continue
        title = a_tag.get_text().strip()
        link = a_tag['href'].strip() if a_tag.has_attr('href') else ''
        if not title or len(title) < 5 or any(x in title for x in ['联系我们', '广告服务', '专题', '侨网', '图']):
            continue

        full_link = urljoin("https://www.chinanews.com/", link)

        # 获取正文和发布时间
        content = ''
        pub_time_clean = None
        try:
            detail_res = requests.get(full_link, headers=headers, timeout=10)
            detail_res.encoding = 'utf-8'
            detail_soup = BeautifulSoup(detail_res.text, 'html.parser')

            # 正文
            content_tag = detail_soup.find('div', class_='left_zw')
            content = content_tag.get_text(strip=True) if content_tag else ''

            # 发布时间
            time_tag = detail_soup.find('div', class_='left-t')
            if time_tag:
                time_text = time_tag.get_text(strip=True)
                match = re.search(r'\d{4}-\d{2}-\d{2}( \d{2}:\d{2})?', time_text)
                if match:
                    time_str = match.group()
                    try:
                        if len(time_str) == 10:
                            pub_time_clean = datetime.strptime(time_str, "%Y-%m-%d")
                        else:
                            pub_time_clean = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
                    except Exception as e:
                        print(f"[时间解析失败] {time_str}")
        except Exception as e:
            print(f"[错误] 抓取正文失败: {full_link}，原因: {e}")

        if content and title:
            articles.append({
                'title': title,
                'url': full_link,
                'publish_time': pub_time_clean,
                'content': content,
                'source': '中国新闻网'
            })

    print(f"[完成] 成功抓取新闻 {len(articles)} 条")
    return articles


# 存储函数
def save_to_db(articles):
    db = get_db_connection()
    cursor = db.cursor()
    count = 0
    for item in articles:
        # 去重判断
        cursor.execute("SELECT COUNT(*) FROM news WHERE title = %s", (item['title'],))
        if cursor.fetchone()['COUNT(*)'] > 0:
            continue
        cursor.execute("""
            INSERT INTO knowledges (title, url, publish_time, content, source)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            item['title'],
            item['url'],
            item['publish_time'],
            item['content'],
            item['source']
        ))
        count += 1
    db.commit()
    cursor.close()
    db.close()
    print(f"[存储] 本次新增入库新闻 {count} 条")


# 定时任务函数
def job():
    print(f"[任务开始] {datetime.now()}")
    articles = crawl_chinanews()
    save_to_db(articles)
    print(f"[任务结束] {datetime.now()}")


# 每小时执行一次
schedule.every(1).hours.do(job)

print("新闻定时抓取已启动，每小时运行一次.")

job()
while True:
    schedule.run_pending()
    time.sleep(1)
