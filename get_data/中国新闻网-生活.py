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
    'password': 'nine',
    'database': 'health',
    'charset': 'utf8mb4',
}


# 数据库连接函数
def get_db_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)


# 爬虫函数：抓取中国新闻网健康知识标题 + 正文
def crawl_chinanews_health():
    url = "https://www.chinanews.com/life/gd.shtml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = 'utf-8'
        res.raise_for_status()
    except Exception as e:
        print(f"[错误] 请求失败: {e}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    articles = []

    # 查找知识列表容器
    news_container = soup.find('div', class_='content_list') or soup.find('ul', class_='content_list')

    if not news_container:
        print("[警告] 未找到知识列表容器")
        return articles

    # 查找所有知识项
    for item in news_container.find_all('li'):
        a_tag = item.find('a')
        if not a_tag:
            continue

        title = a_tag.get_text().strip()
        link = a_tag.get('href', '').strip()

        # 过滤无效标题
        if (not title or len(title) < 5 or
                any(x in title for x in ['联系我们', '广告服务', '专题', '侨网', '图', '...'])):
            continue

        # 构建完整URL
        full_link = urljoin("https://www.chinanews.com/", link)

        # 获取正文和发布时间
        content = ''
        pub_time_clean = None
        try:
            detail_res = requests.get(full_link, headers=headers, timeout=15)
            detail_res.encoding = 'utf-8'
            detail_soup = BeautifulSoup(detail_res.text, 'html.parser')

            # 获取正文
            content_tag = detail_soup.find('div', class_='left_zw') or detail_soup.find('div', class_='content')

            if content_tag:
                # 清理不需要的元素
                for elem in content_tag.find_all(['script', 'style', 'iframe', 'ins', 'a']):
                    elem.decompose()
                content = content_tag.get_text(strip=True, separator='\n')

            # 尝试多种方式获取发布时间
            pub_time_clean = extract_publish_time(detail_soup, full_link)

        except Exception as e:
            print(f"[错误] 抓取正文失败: {full_link}，原因: {e}")
            continue

        if content and title:
            articles.append({
                'title': title,
                'url': full_link,
                'publish_time': pub_time_clean,
                'content': content,
                'source': '中国新闻网健康频道'
            })

    print(f"[完成] 成功抓取健康知识 {len(articles)} 条")
    return articles


def extract_publish_time(soup, url):
    """从BeautifulSoup对象中提取发布时间"""
    # 1. 优先从meta标签获取发布时间 (最准确)
    meta_selectors = [
        {'name': 'pubdate'},
        {'property': 'article:published_time'},
        {'name': 'publishdate'},
        {'itemprop': 'datePublished'},
        {'name': 'publish_time'},
        {'name': 'og:published_time'},
        {'property': 'og:published_time'}
    ]

    for selector in meta_selectors:
        meta_tag = soup.find('meta', attrs=selector)
        if meta_tag and meta_tag.get('content'):
            time_str = meta_tag['content'].strip()
            try:
                # 处理带时区的格式: "2024-06-12T15:23:00+08:00"
                if 'T' in time_str:
                    # 移除时区部分
                    if '+' in time_str:
                        time_str = time_str.split('+')[0]
                    elif 'Z' in time_str:
                        time_str = time_str.split('Z')[0]
                    return datetime.fromisoformat(time_str)
                # 处理简单日期时间格式
                elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', time_str):
                    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                # 处理简单日期格式
                elif re.match(r'\d{4}-\d{2}-\d{2}', time_str):
                    return datetime.strptime(time_str, "%Y-%m-%d")
            except Exception as e:
                print(f"[meta时间解析失败] {url} - {time_str}: {e}")

    # 2. 尝试从可见元素获取发布时间 (第二优先级)
    time_selectors = [
        {'class': 'left-t'},
        {'class': 'time-source'},
        {'class': 'source-time'},
        {'class': 'pubtime'},
        {'class': 'time'},
        {'class': 'date'},
        {'id': 'pubtime'},
        {'class': 'article-time'},
        {'class': 'publish-time'},
        {'class': 'timestamp'},
    ]

    for selector in time_selectors:
        time_tag = soup.find('div', selector) or soup.find('span', selector) or soup.find('time', selector)
        if time_tag:
            time_text = time_tag.get_text(strip=True)
            # 改进时间提取正则 - 支持更多格式
            patterns = [
                r'发布时间[:：]?\s*(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?[ \t]*\d{1,2}:\d{1,2}',
                r'(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?[ \t]*\d{1,2}:\d{1,2}',
                r'(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?',
                r'(\d{1,2})[-/月](\d{1,2})[日][ \t]*\d{1,2}:\d{1,2}',
                r'发布于\s*(\d{4}-\d{2}-\d{2})',
                r'时间[:：]\s*(\d{4}-\d{2}-\d{2})'
            ]

            for pattern in patterns:
                match = re.search(pattern, time_text)
                if match:
                    try:
                        # 提取日期组件
                        date_parts = [p for p in match.groups() if p is not None]

                        # 根据匹配组件数量处理
                        if len(date_parts) == 3:  # 年月日
                            year, month, day = date_parts
                        elif len(date_parts) == 2:  # 月日
                            month, day = date_parts
                            year = datetime.now().year
                        else:
                            continue

                        # 处理可能的两位数年份
                        if len(year) == 2:
                            year = f"20{year}" if int(year) < 50 else f"19{year}"

                        return datetime(int(year), int(month), int(day))
                    except Exception as e:
                        print(f"[时间解析失败] {url} - {time_text}: {e}")

    # 3. 从URL中提取日期 (最低优先级)
    match = re.search(r'/(\d{4})/(\d{2})-(\d{2})/', url)
    if match:
        year, month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except Exception:
            pass

    # 4. 从URL中提取另一种格式的日期
    match = re.search(r'jk/(\d{4})/(\d{2})-(\d{2})/', url)
    if match:
        year, month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except Exception:
            pass

    print(f"[警告] 无法提取发布时间: {url}")
    return None


def save_to_db(articles):
    if not articles:
        print("[存储] 无新文章需要存储")
        return

    db = get_db_connection()
    cursor = db.cursor()
    count = 0
    skipped = 0

    try:
        # 提取所有URL用于批量检查
        urls = [item['url'] for item in articles]

        # 批量查询已存在的URL
        if urls:
            placeholders = ', '.join(['%s'] * len(urls))
            query = f"SELECT url FROM knowledges WHERE url IN ({placeholders})"
            cursor.execute(query, urls)
            existing_urls = {row['url'] for row in cursor.fetchall()}
        else:
            existing_urls = set()

        for item in articles:
            # 主去重：URL检查
            if item['url'] in existing_urls:
                skipped += 1
                continue

            # 备选去重：标题检查（防止URL变化）
            cursor.execute("SELECT COUNT(*) FROM knowledges WHERE title = %s", (item['title'],))
            if cursor.fetchone()['COUNT(*)'] > 0:
                skipped += 1
                continue

            # 插入新记录
            try:
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
                # 添加到已存在集合，防止同一批文章重复插入
                existing_urls.add(item['url'])
            except pymysql.Error as e:
                print(f"[错误] 插入失败: {item['url']}，原因: {e}")
                skipped += 1

        if count > 0:
            db.commit()
            print(f"[存储] 成功新增 {count} 条记录，跳过 {skipped} 条重复记录")
        else:
            print(f"[存储] 无新增记录，所有 {len(articles)} 条均为重复")
    except Exception as e:
        print(f"[严重错误] 存储过程失败: {e}")
    finally:
        cursor.close()
        db.close()


# 定时任务函数
def job():
    print(f"[任务开始] {datetime.now()}")
    articles = crawl_chinanews_health()
    if articles:
        save_to_db(articles)
    print(f"[任务结束] {datetime.now()}")


# 每小时执行一次
schedule.every(1).hours.do(job)

print("健康知识定时抓取已启动，每小时运行一次.")

# 首次执行
job()

# 保持定时任务运行
while True:
    schedule.run_pending()
    time.sleep(60)  # 每分钟检查一次，减少CPU占用