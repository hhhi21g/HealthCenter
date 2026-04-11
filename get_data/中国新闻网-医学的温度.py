import schedule
import time
import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import json

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


# 爬虫函数：抓取医学的温度频道的文章
def crawl_temperatureofmedicine():
    base_url = "https://channel.chinanews.com.cn/cns/cl/life-temperatureofmedicine.shtml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }
    articles = []

    # 抓取前5页内容
    for page in range(0, 7):
        url = f"{base_url}?pager={page}"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = 'utf-8'
            res.raise_for_status()
        except Exception as e:
            print(f"[错误] 请求失败: {e}")
            continue

        # 使用正则表达式提取JavaScript中的docArr数据
        doc_arr_match = re.search(r'var docArr\s*=\s*(\[.*?\]);', res.text, re.DOTALL)
        if not doc_arr_match:
            print(f"[警告] 第{page + 1}页未找到文章数据")
            continue

        try:
            doc_list = json.loads(doc_arr_match.group(1))
        except json.JSONDecodeError as e:
            print(f"[错误] 解析文章数据失败: {e}")
            continue

        for item in doc_list:
            try:
                title = item.get('title', '').strip()
                link = item.get('url', '').strip()

                # 过滤无效标题
                if (not title or len(title) < 5 or
                        any(x in title for x in ['联系我们', '广告服务', '专题', '侨网', '图', '...'])):
                    continue

                # 构建完整URL
                full_link = urljoin(base_url, link)

                # 提取发布时间
                pub_time_str = item.get('pubtime', '')
                pub_time_clean = None
                if pub_time_str:
                    try:
                        pub_time_clean = datetime.strptime(pub_time_str, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass

                # 获取正文内容
                content = item.get('content', '')

                # 如果详情页有更好的发布时间，使用详情页的时间
                if not pub_time_clean:
                    try:
                        detail_res = requests.get(full_link, headers=headers, timeout=15)
                        detail_res.encoding = 'utf-8'
                        detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
                        pub_time_clean = extract_publish_time(detail_soup, full_link)
                    except Exception as e:
                        print(f"[错误] 抓取详情页失败: {full_link}，原因: {e}")

                if content and title:
                    articles.append({
                        'title': title,
                        'url': full_link,
                        'publish_time': pub_time_clean,
                        'content': content,
                        'source': '医学的温度'
                    })
            except Exception as e:
                print(f"[错误] 处理文章时出错: {e}")
                continue

        print(f"[完成] 第{page + 1}页成功抓取 {len(doc_list)} 篇文章")

    print(f"[总计] 成功抓取医学的温度文章 {len(articles)} 条")
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
        {'class': 'article-time'},  # 新增常见选择器
        {'class': 'publish-time'},  # 新增常见选择器
        {'class': 'timestamp'},  # 新增常见选择器
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
    # 示例URL: https://www.chinanews.com.cn/jk/2024/06-12/10227689.shtml
    match = re.search(r'/(\d{4})/(\d{2})-(\d{2})/', url)
    if match:
        year, month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except Exception:
            pass

    # 4. 从URL中提取另一种格式的日期
    # 示例URL: https://www.chinanews.com/jk/2025/06-25/10437969.shtml
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
            query = f"SELECT url FROM news WHERE url IN ({placeholders})"
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
            cursor.execute("SELECT COUNT(*) FROM news WHERE title = %s", (item['title'],))
            if cursor.fetchone()['COUNT(*)'] > 0:
                skipped += 1
                continue

            # 插入新记录
            try:
                cursor.execute("""
                    INSERT INTO news (title, url, publish_time, content, source)
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
    articles = crawl_temperatureofmedicine()
    if articles:
        save_to_db(articles)
    print(f"[任务结束] {datetime.now()}")


# 每小时执行一次
schedule.every(1).hours.do(job)

print("医学的温度频道定时抓取已启动，每小时运行一次.")

# 首次执行
job()

# 保持定时任务运行
while True:
    schedule.run_pending()
    time.sleep(60)  # 每分钟检查一次，减少CPU占用