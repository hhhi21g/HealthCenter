import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import pymysql
import time
import random
import os
from datetime import datetime

# 数据库配置
DB_CONFIG = {
    'host': '119.3.163.155',
    'user': 'root',
    'password': 'nine',
    'database': 'health',
    'charset': 'utf8mb4',
}

# 创建调试目录
os.makedirs('nhc_debug', exist_ok=True)


# 数据库连接函数
def get_db_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)


# 爬虫函数：抓取国家卫健委通知公告
def crawl_nhc_announcements():
    base_url = "https://zwfw.nhc.gov.cn/kzx/tzgg/tzggqb/index.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Referer": "https://zwfw.nhc.gov.cn/",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cookie": "your_session_cookie"  # 从浏览器获取最新cookie
    }

    try:
        print(f"[开始] 抓取国家卫健委通知公告: {base_url}")

        # 添加随机延迟
        time.sleep(random.uniform(1.0, 2.5))

        res = requests.get(base_url, headers=headers, timeout=20)
        print(f"[响应] 状态码: {res.status_code}, 内容长度: {len(res.content)} bytes")

        # 强制使用 UTF-8 编码
        res.encoding = 'utf-8'
        print(f"[编码] 强制使用: UTF-8")

        # 检查内容有效性 - 使用更可靠的验证
        if '通知公告' not in res.text or '国家卫生健康委员会' not in res.text:
            print(f"[警告] 页面内容验证失败")
            # 保存页面用于调试
            debug_filename = f"nhc_debug/index_page.html"
            with open(debug_filename, 'w', encoding='utf-8') as f:
                f.write(res.text)
            print(f"[调试] 页面已保存到: {debug_filename}")
        else:
            print(f"[验证] 页面内容验证通过")

        res.raise_for_status()
    except Exception as e:
        print(f"[错误] 请求国家卫健委失败: {e}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    articles = []

    # 查找列表容器 - 使用更精确的选择器
    list_container = soup.select_one('ul.arts')
    if not list_container:
        print("[警告] 未找到通知公告列表")
        return articles

    # 查找所有列表项
    list_items = list_container.find_all('li')
    print(f"[信息] 找到 {len(list_items)} 条通知公告")

    # 处理每个列表项
    for idx, item in enumerate(list_items):
        # 提取标题和链接
        a_tag = item.find('a')
        if not a_tag or not a_tag.get('href'):
            continue

        title = a_tag.get_text().strip()
        link = a_tag.get('href').strip()

        # 打印原始标题用于调试
        print(f"[原始标题] {title}")

        # 过滤无效标题
        if not title or len(title) < 5:
            continue
        if any(x in title for x in ['图片', '滚动', '专题', '视频', '更多', '...']):
            continue

        print(f"[处理] [{idx + 1}/{len(list_items)}] 标题: {title}")

        # 构建完整URL
        full_link = urljoin(base_url, link)

        # 提取发布时间
        time_span = item.find('span', class_='time')
        pub_time = time_span.get_text().strip() if time_span else None
        pub_time_clean = None

        if pub_time:
            try:
                pub_time_clean = datetime.strptime(pub_time, "%Y-%m-%d")
                print(f"[时间] 从列表页提取: {pub_time_clean}")
            except Exception as e:
                print(f"[时间解析错误] {pub_time}: {e}")

        # 获取正文内容
        content = ''
        try:
            print(f"[获取] 详情页: {full_link}")

            # 添加随机延迟
            time.sleep(random.uniform(0.5, 1.5))

            detail_res = requests.get(full_link, headers=headers, timeout=25)
            print(f"[详情响应] 状态码: {detail_res.status_code}, 长度: {len(detail_res.content)} bytes")

            # 强制使用 UTF-8 编码
            detail_res.encoding = 'utf-8'
            print(f"[详情编码] 强制使用: UTF-8")

            detail_soup = BeautifulSoup(detail_res.text, 'html.parser')

            # 获取正文 - 国家卫健委使用特定的class
            content_div = detail_soup.find('div', class_='TRS_Editor')
            if not content_div:
                # 尝试其他可能的容器
                content_div = detail_soup.find('div', class_='content')
            if not content_div:
                content_div = detail_soup.find('div', class_='article')

            if content_div:
                # 清理不需要的元素
                for elem in content_div.find_all(['script', 'style', 'iframe', 'ins', 'a']):
                    elem.decompose()

                # 保留段落结构
                content = "\n".join([p.get_text(strip=True) for p in content_div.find_all(['p', 'div'])])
                print(f"[成功] 获取正文，长度: {len(content)}")
            else:
                print("[警告] 未找到正文内容")

                # 保存详情页用于调试
                detail_debug_filename = f"nhc_debug/detail_{idx}.html"
                with open(detail_debug_filename, 'w', encoding='utf-8') as f:
                    f.write(detail_res.text)
                print(f"[调试] 详情页已保存到: {detail_debug_filename}")

        except Exception as e:
            print(f"[错误] 抓取详情页失败: {full_link}，原因: {e}")
            continue

        if content and title:
            articles.append({
                'title': title,
                'url': full_link,
                'publish_time': pub_time_clean,
                'content': content,
                'source': '国家卫健委政务服务平台'
            })

    print(f"[完成] 成功抓取通知公告 {len(articles)} 条")
    return articles


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
            query = f"SELECT url FROM notice WHERE url IN ({placeholders})"
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
            cursor.execute("SELECT COUNT(*) FROM notice WHERE title = %s", (item['title'],))
            if cursor.fetchone()['COUNT(*)'] > 0:
                skipped += 1
                print(f"[跳过] 标题重复: {item['title']}")
                continue

            # 插入新记录
            try:
                cursor.execute("""
                    INSERT INTO notice (title, url, publish_time, content, source)
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


def extract_page_info(soup):
    page_info = {
        "current_page": 0,
        "total_pages": 0,
        "next_page_url": None
    }

    # 修改后的分页参数提取逻辑
    page_script = soup.find('div', class_='page').find('script')
    if page_script:
        script_text = page_script.string
        # 增强正则表达式容错性
        current_page_match = re.search(r'currentPage\s*=\s*(\d+)', script_text)
        count_page_match = re.search(r'countPage\s*=\s*(\d+)', script_text)

        if current_page_match:
            page_info["current_page"] = int(current_page_match.group(1))
        if count_page_match:
            page_info["total_pages"] = int(count_page_match.group(1))

    # 改进下一页链接定位
    next_page_btn = soup.find('a', text=re.compile(r'下一页'))
    if next_page_btn and 'href' in next_page_btn.attrs:
        page_info["next_page_url"] = next_page_btn['href']
        # 处理相对路径
        if not page_info["next_page_url"].startswith('http'):
            page_info["next_page_url"] = urljoin(base_url, page_info["next_page_url"])

    return page_info

# 主函数
def main():
    print("=" * 50)
    print(f"国家卫健委通知公告抓取任务启动 - {datetime.now()}")
    print("=" * 50)

    # 抓取通知公告
    articles = crawl_nhc_announcements()

    # 存储到数据库
    if articles:
        save_to_db(articles)

    print("=" * 50)
    print(f"任务完成! 共处理 {len(articles)} 条通知公告")
    print("=" * 50)


if __name__ == "__main__":
    main()