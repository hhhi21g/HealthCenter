import schedule
import time
import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import chardet
import random
import os

# 数据库配置
DB_CONFIG = {
    'host': '119.3.163.155',
    'user': 'root',
    'password': 'nine',
    'database': 'health',
    'charset': 'utf8mb4',
}

# 创建调试目录
os.makedirs('debug_pages', exist_ok=True)


# 数据库连接函数
def get_db_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)


# 爬虫函数：抓取人民网健康政策标题 + 正文
def crawl_people_health():
    base_url = "http://health.people.com.cn/GB/408564/index{}.html"

    # 更真实的浏览器头信息
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": "http://health.people.com.cn/",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1"
    }

    all_articles = []

    # 爬取内容
    for page in range(1, 2):
        url = base_url.format(page)
        try:
            print(f"[开始] 抓取第 {page} 页: {url}")

            # 添加随机延迟，避免被屏蔽
            time.sleep(random.uniform(2.0, 4.0))

            res = requests.get(url, headers=headers, timeout=25)
            print(f"[响应] 状态码: {res.status_code}, 内容长度: {len(res.content)} bytes")

            # 自动检测编码
            encoding = chardet.detect(res.content)['encoding'] or 'gb2312'
            res.encoding = encoding
            print(f"[编码] 使用: {res.encoding}")

            # 验证内容是否有效
            if '健康' not in res.text and '政策' not in res.text:
                print(f"[警告] 页面内容验证失败，尝试其他编码")
                for enc in ['gbk', 'utf-8', 'gb18030', 'iso-8859-1']:
                    res.encoding = enc
                    if '健康' in res.text or '政策' in res.text:
                        print(f"[编码] 回退到: {enc}")
                        break

            # 检查是否被重定向
            if res.url != url:
                print(f"[重定向] 从 {url} 到 {res.url}")
                url = res.url  # 更新URL为实际访问的URL

            res.raise_for_status()
        except Exception as e:
            print(f"[错误] 请求人民网失败: {e}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        articles = []

        # 保存原始页面用于调试
        debug_filename = f"debug_pages/people_page_{page}.html"
        with open(debug_filename, 'w', encoding='utf-8') as f:
            f.write(res.text)
        print(f"[调试] 页面已保存到: {debug_filename}")

        # 打印页面标题，确认内容
        page_title = soup.title.string if soup.title else "无标题"
        print(f"[页面标题] {page_title}")

        # 尝试查找所有可能的文章链接
        possible_links = []

        # 方法1: 查找所有包含"n1"的链接（人民网常见文章URL格式）
        links_method1 = soup.find_all('a', href=re.compile(r'n1/\d{4}/\d{4}/c\d+-\d+\.html'))
        print(f"[链接方法1] 找到 {len(links_method1)} 个链接")
        possible_links.extend(links_method1)

        # 方法2: 查找所有包含"policy"关键字的链接
        links_method2 = soup.find_all('a', href=re.compile(r'policy'))
        print(f"[链接方法2] 找到 {len(links_method2)} 个链接")
        possible_links.extend(links_method2)

        # 方法3: 查找所有包含"健康"或"政策"文本的链接
        links_method3 = []
        for a_tag in soup.find_all('a'):
            if '健康' in a_tag.get_text() or '政策' in a_tag.get_text():
                links_method3.append(a_tag)
        print(f"[链接方法3] 找到 {len(links_method3)} 个链接")
        possible_links.extend(links_method3)

        # 去重
        unique_links = {a['href']: a for a in possible_links if a.has_attr('href')}
        print(f"[链接总计] 去重后 {len(unique_links)} 个唯一链接")

        if not unique_links:
            print(f"[警告] 未找到任何可能的文章链接")
            continue

        # 处理每个可能的文章链接
        for idx, (link, a_tag) in enumerate(unique_links.items()):
            title = a_tag.get_text().strip()

            # 过滤无效标题
            if not title or len(title) < 5:
                continue

            if any(x in title for x in
                   ['图片', '滚动', '专题', '视频', '更多', '...', '首页', '上一页', '下一页', '尾页']):
                continue

            print(f"[处理] [{idx + 1}/{len(unique_links)}] 标题: {title}")
            print(f"[链接] {link}")

            # 构建完整URL
            full_link = urljoin(url, link)

            # 获取正文和发布时间
            content = ''
            pub_time_clean = None
            try:
                print(f"[获取] 详情页: {full_link}")

                # 添加随机延迟
                time.sleep(random.uniform(1.0, 3.0))

                detail_res = requests.get(full_link, headers=headers, timeout=30)
                print(f"[详情响应] 状态码: {detail_res.status_code}, 长度: {len(detail_res.content)} bytes")

                # 自动检测详情页编码
                detail_encoding = chardet.detect(detail_res.content)['encoding'] or 'gb2312'
                detail_res.encoding = detail_encoding
                print(f"[详情编码] 使用: {detail_res.encoding}")

                detail_soup = BeautifulSoup(detail_res.text, 'html.parser')

                # 获取标题（再次确认）
                detail_title = detail_soup.title.string if detail_soup.title else ""
                print(f"[详情标题] {detail_title}")

                # 获取正文 - 尝试多种选择器
                content_div = None
                content_selectors = [
                    ('div', 'box_con'),
                    ('div', 'rwb_zw'),
                    ('div', 'artDet'),
                    ('div', 'content'),
                    ('div', 'article'),
                    ('div', 'TRS_Editor'),
                    ('div', 'article-content'),
                    ('div', 'artical'),
                    ('div', 'content-main'),
                    ('div', 'article-body')
                ]

                for tag, cls in content_selectors:
                    content_div = detail_soup.find(tag, class_=cls)
                    if content_div:
                        print(f"[正文] 使用选择器: {tag}.{cls}")
                        break

                # 备用方法：通过ID查找
                if not content_div:
                    content_div = detail_soup.find('div', id='rwb_zw')
                    if content_div:
                        print(f"[正文] 使用ID: rwb_zw")

                # 备用方法：查找包含"正文"的元素
                if not content_div:
                    text_elements = detail_soup.find_all(string=re.compile('正文'))
                    if text_elements:
                        for elem in text_elements:
                            parent = elem.find_parent('div')
                            if parent:
                                content_div = parent
                                print(f"[正文] 使用备用方法找到正文容器")
                                break

                if content_div:
                    # 清理不需要的元素
                    for elem in content_div.find_all(['script', 'style', 'iframe', 'ins', 'a', 'div']):
                        elem.decompose()
                    content = content_div.get_text(strip=True, separator='\n')
                    print(f"[成功] 获取正文，长度: {len(content)}")
                else:
                    print("[警告] 未找到正文内容")
                    # 保存详情页用于调试
                    detail_debug_filename = f"debug_pages/detail_{page}_{idx}.html"
                    with open(detail_debug_filename, 'w', encoding='utf-8') as f:
                        f.write(detail_res.text)
                    print(f"[调试] 详情页已保存到: {detail_debug_filename}")

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

    print(f"[总计] 成功抓取人民网健康政策 {len(all_articles)} 条")
    return all_articles


# extract_publish_time 和 save_to_db 函数保持不变
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
            query = f"SELECT url FROM policy WHERE url IN ({placeholders})"
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
            cursor.execute("SELECT COUNT(*) FROM policy WHERE title = %s", (item['title'],))
            if cursor.fetchone()['COUNT(*)'] > 0:
                skipped += 1
                print(f"[跳过] 标题重复: {item['title']}")
                continue

            # 插入新记录
            try:
                cursor.execute("""
                    INSERT INTO policy (title, url, publish_time, content, source)
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

print("人民网健康政策定时抓取已启动，每小时运行一次.")

# 首次执行
job()

# 保持定时任务运行
while True:
    schedule.run_pending()
    time.sleep(60)  # 每分钟检查一次，减少CPU占用