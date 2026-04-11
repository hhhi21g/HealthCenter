import schedule
import time
import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import logging
import os
import random

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("chinacdc_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("chinacdc_crawler")

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


# 爬虫函数：抓取中国CDC中心要闻
def crawl_chinacdc_news(max_pages=5):
    base_url = "https://www.chinacdc.cn/zxyw/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.chinacdc.cn/",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0"
    }

    try:
        logger.info(f"开始抓取中国CDC中心要闻，最多爬取 {max_pages} 页")
        # 创建会话
        session = requests.Session()
        all_articles = []

        # 初始化当前页URL
        current_page_url = base_url
        page = 1
        processed_pages = 0

        while current_page_url and processed_pages < max_pages:
            processed_pages += 1
            try:
                # 添加页间延迟
                if page > 1:
                    time.sleep(random.uniform(2.0, 4.0))

                logger.info(f"开始爬取第 {page} 页: {current_page_url}")

                # 获取列表页
                res = session.get(current_page_url, headers=headers, timeout=15)
                res.encoding = 'utf-8'
                res.raise_for_status()
                logger.info(f"请求成功: 状态码 {res.status_code}, 大小 {len(res.content)} 字节")
            except Exception as e:
                logger.error(f"获取第 {page} 页失败: {e}")
                break

            # 创建调试目录
            debug_dir = 'chinacdc_debug'
            os.makedirs(debug_dir, exist_ok=True)

            # 保存页面用于调试
            debug_file = os.path.join(debug_dir, f'list_page_{page}.html')
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(res.text)
            logger.info(f"列表页已保存到: {debug_file}")

            soup = BeautifulSoup(res.text, "html.parser")
            articles = []

            # 查找文章列表容器
            list_container = soup.find('ul', class_='xw_list')

            if not list_container:
                logger.warning("未找到文章列表容器，尝试备用选择器")
                # 尝试其他可能的选择器
                list_container = soup.find('div', class_='erjiRightBox') or soup.find('div', class_='erjiRight')

                if not list_container:
                    logger.error(f"第 {page} 页无法找到文章列表容器，跳过")
                    # 尝试查找下一页链接
                    next_page_link = get_next_page_link(soup, page)
                    if next_page_link:
                        current_page_url = next_page_link
                        page += 1
                        continue
                    else:
                        logger.info("找不到下一页链接，停止爬取")
                        break
                else:
                    logger.info("使用备用选择器找到容器")

            # 查找所有文章项
            list_items = list_container.find_all('li')
            logger.info(f"第 {page} 页找到 {len(list_items)} 个列表项")

            if not list_items:
                logger.info(f"第 {page} 页没有文章，尝试查找下一页")
                # 尝试查找下一页链接
                next_page_link = get_next_page_link(soup, page)
                if next_page_link:
                    current_page_url = next_page_link
                    page += 1
                    continue
                else:
                    logger.info("找不到下一页链接，停止爬取")
                    break

            for idx, item in enumerate(list_items):
                try:
                    # 提取标题和链接
                    a_tag = item.select_one('dd a')  # 使用更精确的选择器
                    if not a_tag:
                        logger.debug(f"列表项 {idx} 未找到链接标签")
                        continue

                    # 提取标题（排除时间部分）
                    title_text = a_tag.get_text().strip()
                    # 使用正则表达式移除时间部分
                    title = re.sub(r'\s*\d{4}-\d{2}-\d{2}$', '', title_text).strip()

                    # 如果正则没有移除时间，尝试分割
                    if len(title) == len(title_text):
                        title = title_text.split('<span>')[0].strip()

                    link = a_tag.get('href', '').strip()

                    # 过滤无效标题
                    if (not title or len(title) < 5 or
                            any(x in title for x in ['图片', '滚动', '专题', '视频', '更多', '...', '通知公告'])):
                        logger.debug(f"跳过无效标题: {title}")
                        continue

                    logger.info(f"处理文章 [{idx + 1}/{len(list_items)}]: {title}")

                    # 构建完整URL
                    full_link = urljoin(base_url, link)

                    # 获取发布时间 - 直接从列表项中提取
                    time_span = item.find('span')
                    pub_time = time_span.get_text().strip() if time_span else None
                    pub_time_clean = None

                    if pub_time:
                        try:
                            # 尝试多种日期格式
                            formats = [
                                "%Y-%m-%d",
                                "%Y/%m/%d",
                                "%Y年%m月%d日"
                            ]

                            for fmt in formats:
                                try:
                                    pub_time_clean = datetime.strptime(pub_time, fmt)
                                    logger.info(f"解析时间成功: {pub_time_clean}")
                                    break
                                except:
                                    continue
                        except Exception as e:
                            logger.warning(f"时间解析失败: {e}")

                    # 添加到当前页文章列表
                    articles.append({
                        'title': title,
                        'url': full_link,
                        'pub_time': pub_time_clean,
                        'page': page
                    })

                except Exception as e:
                    logger.error(f"处理列表项 {idx + 1} 时出错: {e}")
                    continue

            logger.info(f"第 {page} 页找到 {len(articles)} 篇文章")
            all_articles.extend(articles)

            # 查找下一页链接
            next_page_link = get_next_page_link(soup, page)
            if next_page_link:
                current_page_url = next_page_link
                page += 1
            else:
                logger.info(f"第 {page} 页后无更多页面，停止爬取")
                break

        logger.info(f"总共找到 {len(all_articles)} 篇文章，开始处理详情页")

        # 处理所有文章详情
        results = []
        for idx, article in enumerate(all_articles):
            try:
                # 添加请求延迟
                time.sleep(random.uniform(1.0, 3.0))

                logger.info(f"获取详情页 [{idx + 1}/{len(all_articles)}]: {article['title']}")
                detail_res = session.get(article['url'], headers=headers, timeout=20)
                detail_res.encoding = 'utf-8'
                detail_res.raise_for_status()

                # 保存详情页用于调试
                detail_file = os.path.join(debug_dir, f'detail_{article["page"]}_{idx}.html')
                with open(detail_file, 'w', encoding='utf-8') as f:
                    f.write(detail_res.text)
                logger.info(f"详情页已保存: {detail_file}")

                detail_soup = BeautifulSoup(detail_res.text, 'html.parser')

                # 获取正文 - 尝试多种选择器
                content_selectors = [
                    'div.TRS_Editor',
                    'div.content',
                    'div.article-content',
                    'div.main-text',
                    'div.detail',
                    'div.article-body',
                    'div.content-text',
                    'article'
                ]

                content_div = None
                for selector in content_selectors:
                    content_div = detail_soup.select_one(selector)
                    if content_div:
                        logger.info(f"使用选择器 '{selector}' 找到正文容器")
                        break

                if not content_div:
                    logger.warning("未找到正文容器，尝试备用选择器")
                    content_div = detail_soup.find('div', class_='main') or detail_soup.find('div', class_='content')

                content = ''
                if content_div:
                    # 清理不需要的元素
                    for elem in content_div.find_all(
                            ['script', 'style', 'iframe', 'ins', 'a', 'div.ad', 'div.related']):
                        elem.decompose()

                    # 提取文本内容
                    content = content_div.get_text(strip=True, separator='\n')
                    content = re.sub(r'\n{3,}', '\n\n', content)  # 清理多余空行
                    logger.info(f"获取正文成功，长度: {len(content)} 字符")
                else:
                    logger.warning("未找到正文内容")
                    # 尝试回退到整个正文区域
                    content = detail_soup.get_text(separator='\n', strip=True)
                    content = re.sub(r'\n{3,}', '\n\n', content)

                # 如果列表页没有时间，尝试从详情页提取
                pub_time_clean = article['pub_time']
                if not pub_time_clean:
                    pub_time_clean = extract_publish_time(detail_soup, article['url'])

                # 确保有标题和内容
                if not article['title']:
                    logger.warning("文章无标题，跳过")
                    continue

                if not content or len(content) < 100:
                    logger.warning("文章内容过短，可能无效")

                # 添加到结果列表
                results.append({
                    'title': article['title'],
                    'url': article['url'],
                    'publish_time': pub_time_clean or datetime.now(),
                    'content': content,
                    'source': '中国疾病预防控制中心'
                })

            except Exception as e:
                logger.error(f"处理详情页失败: {article['url']}，原因: {e}")
                continue

        logger.info(f"成功解析 {len(results)} 篇文章")
        return results

    except Exception as e:
        logger.error(f"爬取过程中出错: {e}")
        return []


def get_next_page_link(soup, current_page):
    """获取下一页链接"""
    try:
        # 查找分页容器
        pagination = soup.find('div', class_='fya')
        if not pagination:
            logger.warning("未找到分页容器")
            return None

        # 查找当前页的下一页链接
        # 通常下一页链接的文本是"下一页"或包含"下一页"
        next_page_link = None

        # 尝试查找文本为"下一页"的链接
        next_page_tag = pagination.find('a', string='下一页')
        if next_page_tag and next_page_tag.get('href'):
            next_page_link = urljoin("https://www.chinacdc.cn/zxyw/", next_page_tag['href'])
            logger.info(f"找到下一页链接: {next_page_link}")
            return next_page_link

        # 如果找不到"下一页"，尝试通过页码计算
        # 当前页是1时，下一页是index_1.html
        # 当前页是n时，下一页是index_{n}.html
        if current_page == 1:
            next_page_url = "index_1.html"
        else:
            next_page_url = f"index_{current_page}.html"

        # 检查这个链接是否存在于分页中
        if pagination.find('a', href=next_page_url):
            next_page_link = urljoin("https://www.chinacdc.cn/zxyw/", next_page_url)
            logger.info(f"通过页码计算找到下一页: {next_page_link}")
            return next_page_link

        logger.warning("未找到有效的下一页链接")
        return None

    except Exception as e:
        logger.error(f"获取下一页链接失败: {e}")
        return None

def extract_publish_time(soup, url):
    """从BeautifulSoup对象中提取发布时间"""
    # 1. 优先从meta标签获取发布时间
    meta_selectors = [
        {'name': 'pubdate'},
        {'property': 'article:published_time'},
        {'name': 'publishdate'},
        {'itemprop': 'datePublished'},
        {'name': 'publish_time'},
        {'name': 'og:published_time'},
        {'property': 'og:published_time'},
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
                    return datetime.fromisoformat(time_str)
                # 处理简单日期时间格式
                elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', time_str):
                    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                # 处理简单日期格式
                elif re.match(r'\d{4}-\d{2}-\d{2}', time_str):
                    return datetime.strptime(time_str, "%Y-%m-%d")
            except Exception:
                continue

    # 2. 尝试从可见元素获取发布时间
    time_selectors = [
        {'class': 'source'},  # 中国CDC的发布时间通常在这个class内
        {'class': 'time'},  # 可能包含日期的class
        {'class': 'date'},  # 常见的日期class
        {'id': 'pubtime'},  # 特定的ID
        {'class': 'info'},  # 信息类
        {'class': 'publish-date'},  # 发布日期的class
    ]

    for selector in time_selectors:
        time_tag = soup.find('div', selector) or soup.find('span', selector)
        if time_tag:
            time_text = time_tag.get_text(strip=True)
            # 改进时间提取正则
            patterns = [
                r'(\d{4})年(\d{1,2})月(\d{1,2})日',
                r'(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?',
                r'发布时间[:：]?\s*(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?',
                r'时间[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'(\d{4}-\d{2}-\d{2})\s*\d{1,2}:\d{1,2}',
                r'日期[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'发布日期[:：]\s*(\d{4}-\d{2}-\d{2})'
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
                        elif len(date_parts) == 1:  # 完整日期字符串
                            return datetime.strptime(date_parts[0], "%Y-%m-%d")
                        else:
                            continue

                        # 处理可能的两位数年份
                        if len(year) == 2:
                            year = f"20{year}" if int(year) < 50 else f"19{year}"

                        return datetime(int(year), int(month), int(day))
                    except Exception:
                        continue

    # 3. 从URL中提取日期 (最低优先级)
    # 示例URL: https://www.chinacdc.cn/zxyw/202406/t20240624_601860.html
    match = re.search(r'/(\d{6})/t(\d{8})_', url)
    if match:
        year_month, full_date = match.groups()
        year = year_month[:4]
        month = year_month[4:6]
        day = full_date[6:8]
        try:
            return datetime(int(year), int(month), int(day))
        except Exception:
            pass

    logger.warning(f"无法提取发布时间: {url}")
    return None


def save_to_db(articles):
    if not articles:
        logger.info("无新文章需要存储")
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
                logger.info(f"跳过 URL重复: {item['title']}")
                continue

            # 备选去重：标题检查（防止URL变化）
            cursor.execute("SELECT COUNT(*) FROM news WHERE title = %s", (item['title'],))
            if cursor.fetchone()['COUNT(*)'] > 0:
                skipped += 1
                logger.info(f"跳过 标题重复: {item['title']}")
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
                logger.info(f"存储新增: {item['title']}")
            except pymysql.Error as e:
                logger.error(f"插入失败: {item['url']}，原因: {e}")
                skipped += 1

        if count > 0:
            db.commit()
            logger.info(f"存储成功: 新增 {count} 条记录，跳过 {skipped} 条重复记录")
        else:
            logger.info(f"无新增记录，所有 {len(articles)} 条均为重复")
    except Exception as e:
        logger.error(f"存储过程失败: {e}")
        db.rollback()
    finally:
        cursor.close()
        db.close()


# 定时任务函数
def job():
    logger.info(f"{'=' * 60}")
    logger.info(f"任务开始: {datetime.now()}")
    articles = crawl_chinacdc_news()
    if articles:
        save_to_db(articles)
    logger.info(f"任务结束: {datetime.now()}")
    logger.info(f"{'=' * 60}\n")


if __name__ == "__main__":
    # 创建调试目录
    os.makedirs('chinacdc_debug', exist_ok=True)

    logger.info("中国CDC中心要闻定时抓取已启动，每小时运行一次.")

    # 首次执行
    job()

    # 每小时执行一次
    schedule.every(1).hours.do(job)

    # 保持定时任务运行
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("程序被用户中断")
            break
        except Exception as e:
            logger.error(f"主循环异常: {str(e)}")
            time.sleep(300)  # 出错后等待5分钟