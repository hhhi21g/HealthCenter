import schedule
import time
import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import chardet

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


# 爬虫函数：抓取人民网健康新闻标题 + 正文
def crawl_people_health():
    base_url = "http://health.people.com.cn/GB/408565/index{}.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }

    all_articles = []

    # 爬取前3页内容
    for page in range(1, 3):
        url = base_url.format(page)
        try:
            print(f"[开始] 抓取第 {page} 页: {url}")
            res = requests.get(url, headers=headers, timeout=15)

            # 自动检测编码
            encoding = chardet.detect(res.content)['encoding'] or 'gb2312'
            res.encoding = encoding

            # 验证内容是否有效
            if '健康' not in res.text:
                print(f"[警告] 页面内容验证失败，尝试其他编码")
                res.encoding = 'gbk'
                if '健康' not in res.text:
                    res.encoding = 'utf-8'

            res.raise_for_status()
        except Exception as e:
            print(f"[错误] 请求人民网失败: {e}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        articles = []

        # 查找文章列表容器 - 尝试多种选择器
        list_container = None
        container_selectors = [
            ('div', 'p2j_list'),  # 主要容器
            ('ul', 'list_14'),  # 列表容器
            ('div', 'list'),  # 通用列表容器
            ('div', 'content_list'),  # 内容列表
            ('div', 'news-list')  # 新闻列表
        ]

        for tag, cls in container_selectors:
            list_container = soup.find(tag, class_=cls)
            if list_container:
                print(f"[成功] 找到列表容器: {tag}.{cls}")
                break

        if not list_container:
            print(f"[警告] 未找到人民网文章列表容器，尝试直接查找列表项")
            # 尝试查找所有可能的列表项
            list_items = soup.find_all(['li', 'div'], class_=re.compile(r'list|item|news'))
        else:
            # 在容器内查找列表项（不限制标签类型）
            list_items = list_container.find_all(['li', 'div'], recursive=True)

        print(f"[信息] 找到 {len(list_items)} 个列表项")

        if len(list_items) == 0:
            # 保存页面用于调试
            with open(f'people_page_{page}_debug.html', 'w', encoding='utf-8') as f:
                f.write(res.text)
            print(f"[调试] 已保存页面内容到 people_page_{page}_debug.html")

        # 查找所有文章项
        for idx, item in enumerate(list_items):
            # 提取标题和链接 - 更灵活的方式
            a_tag = item.find('a')
            if not a_tag:
                # 可能是div直接包含链接
                if item.name == 'a' and item.get('href'):
                    a_tag = item
                else:
                    continue

            title = a_tag.get_text().strip()
            link = a_tag.get('href', '').strip()

            # 过滤无效标题 - 放宽条件
            if not title:
                continue

            if any(x in title for x in ['图片', '滚动', '专题', '视频', '更多', '...']):
                print(f"[跳过] 标题包含过滤词: {title}")
                continue

            print(f"[处理] [{idx + 1}/{len(list_items)}] 标题: {title}")

            # 构建完整URL
            full_link = urljoin(url, link)

            # 获取正文和发布时间
            content = ''
            pub_time_clean = None
            try:
                print(f"[获取] 详情页: {full_link}")
                detail_res = requests.get(full_link, headers=headers, timeout=20)

                # 自动检测详情页编码
                detail_encoding = chardet.detect(detail_res.content)['encoding'] or 'gb2312'
                detail_res.encoding = detail_encoding

                detail_soup = BeautifulSoup(detail_res.text, 'html.parser')

                # 获取正文 - 尝试多种选择器
                content_div = None
                content_selectors = [
                    ('div', 'box_con'),
                    ('div', 'rwb_zw'),
                    ('div', 'artDet'),
                    ('div', 'content'),
                    ('div', 'article'),
                    ('div', 'TRS_Editor'),
                    ('div', 'article-content')
                ]

                for tag, cls in content_selectors:
                    content_div = detail_soup.find(tag, class_=cls)
                    if content_div:
                        print(f"[正文] 使用选择器: {tag}.{cls}")
                        break

                if content_div:
                    # 清理不需要的元素
                    for elem in content_div.find_all(['script', 'style', 'iframe', 'ins', 'a', 'div']):
                        elem.decompose()
                    content = content_div.get_text(strip=True, separator='\n')
                    print(f"[成功] 获取正文，长度: {len(content)}")
                else:
                    # 尝试查找包含"正文"的元素
                    text_elements = detail_soup.find_all(string=re.compile('正文'))
                    if text_elements:
                        parent = text_elements[0].find_parent()
                        if parent:
                            content = parent.get_text(strip=True, separator='\n')
                            print(f"[备用] 获取正文，长度: {len(content)}")
                    else:
                        print("[警告] 未找到正文内容")

                # 获取发布时间
                pub_time_clean = extract_publish_time(detail_soup, full_link)

                # 如果提取失败，尝试从URL中提取日期
                if not pub_time_clean:
                    # 示例URL: http://health.people.com.cn/n1/2025/0710/c408572-40567270.html
                    match = re.search(r'/n1/(\d{4})/(\d{4})/', full_link)
                    if match:
                        year, month_day = match.groups()
                        month = month_day[:2]
                        day = month_day[2:]
                        try:
                            pub_time_clean = datetime(int(year), int(month), int(day))
                            print(f"[时间] 从URL提取: {pub_time_clean}")
                        except:
                            pass

            except Exception as e:
                print(f"[错误] 抓取详情页失败: {full_link}，原因: {e}")
                continue

            if content and title:
                articles.append({
                    'title': title,
                    'url': full_link,
                    'publish_time': pub_time_clean,
                    'content': content,
                    'source': '人民网健康频道'
                })

        print(f"[完成] 第 {page} 页成功抓取 {len(articles)} 条")
        all_articles.extend(articles)

    print(f"[总计] 成功抓取人民网健康新闻 {len(all_articles)} 条")
    return all_articles


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
        {'property': 'og:published_time'},
        {'name': 'weibo: article:create_at'},  # 人民网特有的meta标签
        {'name': 'publishdate'},
        {'name': 'PubDate'},
        {'name': 'publish_time'}
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
                    dt = datetime.fromisoformat(time_str)
                    print(f"[时间] 从meta标签提取: {dt}")
                    return dt
                # 处理简单日期时间格式
                elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', time_str):
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    print(f"[时间] 从meta标签提取: {dt}")
                    return dt
                # 处理简单日期格式
                elif re.match(r'\d{4}-\d{2}-\d{2}', time_str):
                    dt = datetime.strptime(time_str, "%Y-%m-%d")
                    print(f"[时间] 从meta标签提取: {dt}")
                    return dt
            except Exception as e:
                print(f"[meta时间解析失败] {url} - {time_str}: {e}")

    # 2. 尝试从可见元素获取发布时间 (第二优先级)
    # 人民网特定的时间选择器
    time_selectors = [
        {'class': 'channel'},  # 人民网的发布时间通常在这个class内
        {'class': 'fl'},  # 可能包含日期的class
        {'class': 'box01'},  # 可能包含日期的class
        {'id': 'p_publishtime'},  # 特定的ID
        {'class': 'time'},  # 常见的时间class
        {'class': 'date'},  # 常见的日期class
        {'class': 'pubtime'},  # 发布时间class
        {'class': 'source'},  # 来源可能包含时间
        {'class': 'publish-date'},  # 发布日期
        {'class': 'article-time'},  # 文章时间
    ]

    for selector in time_selectors:
        time_tag = soup.find('div', selector) or soup.find('span', selector)
        if time_tag:
            time_text = time_tag.get_text(strip=True)
            # 改进时间提取正则 - 针对人民网格式
            patterns = [
                r'(\d{4})年(\d{1,2})月(\d{1,2})日\s*\d{1,2}:\d{1,2}',
                r'(\d{4})年(\d{1,2})月(\d{1,2})日',
                r'(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?\s*\d{1,2}:\d{1,2}',
                r'(\d{4})-(\d{1,2})-(\d{1,2})\s*\d{1,2}:\d{1,2}',
                r'发布时间[:：]?\s*(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?',
                r'时间[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'(\d{4}-\d{2}-\d{2})\s*\d{1,2}:\d{1,2}',
                r'发布日期[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'发表时间[:：]\s*(\d{4}-\d{2}-\d{2})'
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

                        dt = datetime(int(year), int(month), int(day))
                        print(f"[时间] 从文本提取: {dt}")
                        return dt
                    except Exception as e:
                        print(f"[时间解析失败] {url} - {time_text}: {e}")

    # 3. 从URL中提取日期 (最低优先级)
    # 尝试多种URL模式
    patterns = [
        r'/n1/(\d{4})/(\d{4})/',  # /n1/2025/0710/
        r'/(\d{4})(\d{2})(\d{2})/',  # /20240624/
        r'_(\d{4})(\d{2})(\d{2})\.',  # _20240624.
        r'/(\d{4})-(\d{2})-(\d{2})/'  # /2024-06-24/
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            try:
                if len(match.groups()) == 2:
                    year, month_day = match.groups()
                    month = month_day[:2]
                    day = month_day[2:4]
                elif len(match.groups()) == 3:
                    year, month, day = match.groups()
                else:
                    continue

                dt = datetime(int(year), int(month), int(day))
                print(f"[时间] 从URL提取: {dt}")
                return dt
            except Exception as e:
                print(f"[URL时间解析失败] {url}: {e}")

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
                print(f"[跳过] URL重复: {item['title']}")
                continue

            # 备选去重：标题检查（防止URL变化）
            cursor.execute("SELECT COUNT(*) FROM news WHERE title = %s", (item['title'],))
            if cursor.fetchone()['COUNT(*)'] > 0:
                skipped += 1
                print(f"[跳过] 标题重复: {item['title']}")
                continue

            # 插入新记录
            try:
                cursor.execute("""
                    INSERT INTO news (title, url, publish_time, content, source)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    item['title'],
                    item['url'],
                    item['publish_time'] or datetime.now(),  # 如果没有时间，使用当前时间
                    item['content'],
                    item['source']
                ))
                count += 1
                # 添加到已存在集合，防止同一批文章重复插入
                existing_urls.add(item['url'])
                print(f"[存储] 新增: {item['title']}")
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
        db.rollback()
    finally:
        cursor.close()
        db.close()


# 定时任务函数
def job():
    print(f"[任务开始] {datetime.now()}")
    articles = crawl_people_health()
    if articles:
        save_to_db(articles)
    print(f"[任务结束] {datetime.now()}\n")


# 每小时执行一次
schedule.every(1).hours.do(job)

print("人民网健康新闻定时抓取已启动，每小时运行一次.")

# 首次执行
job()

# 保持定时任务运行
while True:
    schedule.run_pending()
    time.sleep(60)  # 每分钟检查一次，减少CPU占用