import requests
from bs4 import BeautifulSoup
import re
import pymysql
import time
import random
import os
from datetime import datetime
from urllib.parse import urljoin
import json
from fake_useragent import UserAgent

# 数据库配置
DB_CONFIG = {
    'host': '119.3.163.155',
    'user': 'root',
    'password': 'nine',
    'database': 'health',
    'charset': 'utf8mb4',
}

# 创建调试目录
os.makedirs('../policy/jksb_debug', exist_ok=True)


# 数据库连接函数
def get_db_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)


# 创建UserAgent对象
ua = UserAgent()


# 爬虫函数：抓取健康时报网新闻
def crawl_jksb_news(start_page=6, end_page=20):
    base_url = "http://www.jksb.com.cn/newslist/posid/8/"
    all_articles = []

    # 创建会话对象
    session = requests.Session()

    # 设置初始Cookie
    session.cookies.set("Hm_lvt_70e8428524e456fb71b7b90b03833bc3", str(int(time.time())))
    session.cookies.set("Hm_lpvt_70e8428524e456fb71b7b90b03833bc3", str(int(time.time())))

    # 处理每一页
    for page in range(start_page, end_page + 1):
        page_url = f"{base_url}{page}"
        print(f"\n[处理] 开始抓取第 {page} 页: {page_url}")

        headers = {
            "User-Agent": ua.random,
            "Referer": "http://www.jksb.com.cn/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "X-Requested-With": "XMLHttpRequest",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }

        try:
            # 添加随机延迟
            delay = random.uniform(2.5, 5.0)
            print(f"[延迟] 等待 {delay:.2f} 秒...")
            time.sleep(delay)

            # 请求列表页
            res = session.get(page_url, headers=headers, timeout=30)
            print(f"[响应] 状态码: {res.status_code}, 长度: {len(res.content)} bytes")

            # 检查内容有效性
            if res.status_code != 200 or '健康时报网' not in res.text or '新闻列表' not in res.text:
                print(f"[警告] 第 {page} 页内容验证失败")
                # 保存页面用于调试
                debug_filename = f"../policy/jksb_debug/page_{page}.html"
                with open(debug_filename, 'w', encoding='utf-8') as f:
                    f.write(res.text)
                print(f"[调试] 页面已保存到: {debug_filename}")
                continue

            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, "html.parser")

            # 查找列表容器
            list_container = soup.find('ul', id='list_url')
            if not list_container:
                print(f"[警告] 第 {page} 页未找到新闻列表")
                continue

            # 查找所有列表项
            list_items = list_container.find_all('li')
            print(f"[信息] 第 {page} 页找到 {len(list_items)} 条新闻")

            # 处理每个列表项
            for idx, item in enumerate(list_items):
                # 提取标题和链接
                a_tag = item.find('h1').find('a') if item.find('h1') else None
                if not a_tag or not a_tag.get('href'):
                    continue

                title = a_tag.get_text().strip()
                link = a_tag.get('href').strip()

                # 打印原始标题用于调试
                print(f"[处理] [{page}-{idx + 1}/{len(list_items)}] 标题: {title}")

                # 过滤无效标题
                if not title or len(title) < 5:
                    continue
                if any(x in title for x in ['图片', '滚动', '专题', '视频', '更多', '...']):
                    continue

                # 构建完整URL
                full_link = urljoin(page_url, link)

                # 提取发布时间
                time_div = item.find('div', class_='info')
                pub_time = None
                pub_time_clean = None

                if time_div:
                    time_match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', time_div.get_text())
                    if time_match:
                        pub_time = time_match.group(0)
                        try:
                            # 处理日期格式：2025-07-09 09:15
                            pub_time_clean = datetime.strptime(pub_time, "%Y-%m-%d %H:%M")
                            print(f"[时间] 从列表页提取: {pub_time_clean}")
                        except Exception as e:
                            print(f"[时间解析错误] {pub_time}: {e}")
                            pub_time_clean = datetime.now()

                # 获取正文内容
                content = ''
                try:
                    print(f"[获取] 详情页: {full_link}")

                    # 添加随机延迟
                    detail_delay = random.uniform(1.5, 3.5)
                    print(f"[延迟] 详情页等待 {detail_delay:.2f} 秒...")
                    time.sleep(detail_delay)

                    # 使用新的头信息请求详情页
                    detail_headers = headers.copy()
                    detail_headers["Referer"] = page_url
                    detail_headers["User-Agent"] = ua.random  # 更换UA

                    detail_res = session.get(full_link, headers=detail_headers, timeout=30)
                    print(f"[详情响应] 状态码: {detail_res.status_code}, 长度: {len(detail_res.content)} bytes")

                    # 检查内容有效性
                    if detail_res.status_code != 200:
                        print(f"[错误] 详情页状态码异常: {detail_res.status_code}")
                        continue

                    # 检查是否是有效的HTML页面
                    if not detail_res.text.strip().startswith('<!DOCTYPE'):
                        print(f"[警告] 详情页返回非HTML内容")
                        continue

                    detail_res.encoding = 'utf-8'
                    detail_soup = BeautifulSoup(detail_res.text, 'html.parser')

                    # 检查页面是否存在
                    if detail_soup.find('title') and '404' in detail_soup.find('title').text:
                        print(f"[警告] 页面不存在: {full_link}")
                        continue

                    # 获取正文 - 尝试多种可能的容器
                    content_div = detail_soup.find('div', class_='content')
                    if not content_div:
                        content_div = detail_soup.find('div', id='content')
                    if not content_div:
                        content_div = detail_soup.find('div', class_='article')
                    if not content_div:
                        content_div = detail_soup.find('div', class_='main')
                    if not content_div:
                        content_div = detail_soup.find('div', class_='show_content')

                    if content_div:
                        # 清理不需要的元素
                        for elem in content_div.find_all(['script', 'style', 'iframe', 'ins', 'a', 'form']):
                            elem.decompose()

                        # 保留段落结构
                        content = "\n".join([p.get_text(strip=True) for p in content_div.find_all(['p', 'div']) if
                                             p.get_text(strip=True)])
                        print(f"[成功] 获取正文，长度: {len(content)}")
                    else:
                        print(f"[警告] 未找到正文内容: {title}")
                        # 尝试提取整个正文区域
                        content = detail_soup.get_text()
                        print(f"[备选] 使用整个页面内容，长度: {len(content)}")

                except Exception as e:
                    print(f"[错误] 抓取详情页失败: {full_link}，原因: {e}")
                    continue

                if content and title:
                    all_articles.append({
                        'title': title,
                        'url': full_link,
                        'publish_time': pub_time_clean,
                        'content': content,
                        'source': '健康时报网'
                    })

        except Exception as e:
            print(f"[错误] 请求第 {page} 页失败: {e}")
            continue

    print(f"[完成] 成功抓取 {len(all_articles)} 条新闻")
    return all_articles


# 存储函数：将抓取结果保存到数据库
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


# 创建表结构
def create_table():
    db = get_db_connection()
    cursor = db.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                url VARCHAR(255) NOT NULL UNIQUE,
                publish_time DATETIME,
                content TEXT,
                source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.commit()
        print("[数据库] 表结构已创建或已存在")
    except Exception as e:
        print(f"[错误] 创建表失败: {e}")
    finally:
        cursor.close()
        db.close()


# 主函数
def main():
    print("=" * 50)
    print(f"健康时报网新闻抓取任务启动 - {datetime.now()}")
    print("=" * 50)

    # 创建表
    create_table()

    # 抓取新闻（从第1页到第5页）
    articles = crawl_jksb_news(start_page=76, end_page=100)

    # 存储到数据库
    if articles:
        save_to_db(articles)

    print("=" * 50)
    print(f"任务完成! 共处理 {len(articles)} 条新闻")
    print("=" * 50)


if __name__ == "__main__":
    main()