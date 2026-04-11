import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import time
import random
import os
import logging
import pymysql
import schedule
import chardet
import traceback

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("jkzx_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("jkzx_crawler")

# 数据库配置
DB_CONFIG = {
    'host': '119.3.163.155',
    'user': 'root',
    'password': 'nine',
    'database': 'health',
    'charset': 'utf8mb4',
}
# 创建调试目录
os.makedirs('jkzx_debug', exist_ok=True)


# 数据库连接函数
def get_db_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)


def fetch_page(url, session=None):
    """获取页面内容"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "http://jkzx.org.cn/",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    }

    headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1"
    })

    # 使用会话或创建新会话
    if session is None:
        session = requests.Session()

    try:
        logger.info(f"请求页面: {url}")

        # 添加随机延迟避免请求过快
        time.sleep(random.uniform(1.5, 3.5))

        # 第一次请求
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # 检查内容长度
        if len(response.content) < 1000:
            logger.warning(f"页面内容过短（{len(response.content)}字节），可能是反爬机制")
            # 尝试使用备用User-Agent
            backup_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15"
            ]
            for agent in backup_agents:
                headers["User-Agent"] = agent
                logger.info(f"尝试使用备用User-Agent: {agent[:50]}...")
                time.sleep(random.uniform(2.0, 4.0))
                response = session.get(url, headers=headers, timeout=30)
                if len(response.content) > 1000:
                    logger.info(f"备用User-Agent获取成功，大小: {len(response.content)}字节")
                    break
                else:
                    logger.warning(f"备用User-Agent仍然获取短内容")

        # 检测编码
        if 'charset' in response.headers.get('content-type', '').lower():
            charset = re.search(r'charset=([\w-]+)', response.headers['content-type'], re.I)
            if charset:
                response.encoding = charset.group(1)
        else:
            # 从HTML中检测编码
            charset_match = re.search(r'<meta.*?charset=["\']?([\w-]+)["\']?', response.text[:1000], re.I)
            if charset_match:
                response.encoding = charset_match.group(1)
            else:
                # 尝试自动检测编码
                detected = chardet.detect(response.content)
                if detected['encoding']:
                    response.encoding = detected['encoding']

        logger.info(
            f"请求成功: 状态码 {response.status_code}, 大小 {len(response.content)} 字节, 编码 {response.encoding}")
        return response.text, session
    except Exception as e:
        logger.error(f"请求页面失败: {str(e)}")
        return None, session


def parse_jkzx_list_page(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    articles = []

    # 尝试多种可能的列表容器选择器
    list_containers = [
        ('div.son_list', None),
        ('div.list-container', None),
        ('ul.news-list', None),
        ('div.article-list', None)
    ]

    list_container = None
    for selector, condition in list_containers:
        if not list_container:
            list_container = soup.select_one(selector)
            if list_container:
                logger.info(f"找到列表容器: {selector}")
                break

    if not list_container:
        logger.error("未找到政策发布列表容器")
        # 尝试全局搜索列表项作为备用方案
        logger.info("尝试全局搜索列表项...")
        list_items = soup.find_all('div', class_=re.compile(r'index_\d+_left_text_out|news-item|list-item'))
        logger.info(f"全局搜索找到 {len(list_items)} 个列表项")
    else:
        # 更通用的列表项选择器
        list_items = list_container.find_all(['div', 'li'], class_=re.compile(
            r'index_\d+_left_text_out|news-item|list-item|article-item'
        ))
        logger.info(f"容器内找到 {len(list_items)} 个列表项")

    for idx, item in enumerate(list_items):
        try:
            title_tag = item.find('h4', class_='index_2_left_text_h4') or item.find('a')
            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            a_tag = item.find('a')
            if not a_tag or not a_tag.get('href'):
                continue

            link = a_tag['href']
            full_link = urljoin(base_url, link)

            # 放宽过滤条件
            invalid_keywords = ['图片', '滚动', '视频', '>>', '<<']
            if len(title) < 5 or any(x in title for x in invalid_keywords):
                logger.debug(f"跳过标题: {title}")
                continue

            articles.append({
                'title': title,
                'url': full_link,
                'publish_time': ""
            })

        except Exception as e:
            logger.error(f"解析列表项 {idx + 1} 失败: {str(e)}")

    return articles


def parse_jkzx_article_content(html, url):
    """解析文章详情内容 - 修复标题提取问题"""
    soup = BeautifulSoup(html, 'html.parser')
    result = {
        'title': '',
        'content': '',
        'publish_time': '',
        'source': '健康资讯网'
    }

    try:
        # 增强标题提取 - 针对健康资讯网的特定结构
        title = ""

        # 尝试多种可能的标题位置
        title_selectors = [
            ('div.title h1', None),  # 主标题位置
            ('div.title', None),  # 没有h1标签时的备选
            ('h1.title', None),  # 其他可能的标题位置
            ('h1', lambda t: "健康在线网" not in t and len(t) > 10),  # 过滤网站标题
            ('h2.title', None),
            ('div.article-title', None),
            ('div.article-header h1', None),
            ('div.main h1', None)
        ]

        for selector, condition in title_selectors:
            if not title:  # 如果还没有找到标题
                title_tag = soup.select_one(selector)
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # 应用条件检查（如果有）
                    if condition is None or condition(title_text):
                        title = title_text
                        logger.info(f"使用选择器 '{selector}' 找到标题: {title}")
                        break

        # 如果仍未找到标题，尝试从列表页传递的标题
        if not title:
            # 尝试从URL提取标题（最后手段）
            match = re.search(r'show-(\d+)\.html', url)
            if match:
                title = f"文章_{match.group(1)}"
                logger.warning(f"使用URL中的文章ID作为标题: {title}")
            else:
                title = "未命名文章"
                logger.warning("无法确定标题，使用'未命名文章'")

        result['title'] = title

        # 增强时间提取 - 优化时间提取逻辑
        time_selectors = [
            ('div.time', None),
            ('span.date', None),
            ('div.date', None),
            ('span.pubdate', None),
            ('div.publish-time', None),
            ('div.info', None),
            ('div.article-info', None),
            ('div.source', None)
        ]

        time_str = ""
        for selector, _ in time_selectors:
            if not time_str:
                time_tag = soup.select_one(selector)
                if time_tag:
                    time_str = time_tag.get_text(strip=True)
                    logger.info(f"使用选择器 '{selector}' 找到时间: {time_str}")

        # 从HTML文本中搜索发布时间
        if not time_str:
            time_patterns = [
                r'发布时间[:：]\s*(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?\s*\d{1,2}:\d{1,2}(?::\d{1,2})?)',
                r'发布日期[:：]\s*(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?)',
                r'(\d{4}-\d{1,2}-\d{1,2}\s*\d{1,2}:\d{1,2}(?::\d{1,2})?)',
                r'(\d{4}年\d{1,2}月\d{1,2}日)'
            ]

            for pattern in time_patterns:
                match = re.search(pattern, html)
                if match:
                    time_str = match.group(1)
                    logger.info(f"使用正则表达式找到时间: {time_str}")
                    break

        if time_str:
            # 清理时间字符串
            time_str = re.sub(r'[\s　]+', ' ', time_str)
            time_str = re.sub(r'[^\d\-:/年月日时分秒]', '', time_str)
            result['publish_time'] = time_str

        # 增强内容提取 - 优化内容提取逻辑
        content_div = None

        # 尝试多种可能的内容选择器
        content_selectors = [
            'div.content',
            'div.article-content',
            'div.detail',
            'div#content',
            'div.TRS_Editor',
            'div.article-body',
            'div.main-text',
            'div.content-text',
            'div.article-detail',
            'div.content-main',
            'article',
            'div.zw'
            'div.article',  # 新增
            'div.text'  # 新增
        ]

        for selector in content_selectors:
            if not content_div:
                content_div = soup.select_one(selector)
                if content_div:
                    logger.info(f"使用选择器 '{selector}' 找到内容容器")

        # 如果未找到特定容器，尝试查找包含正文的div
        if not content_div:
            content_div = soup.select_one('div.main') or soup.body
            if content_div:
                logger.info("使用默认内容容器")

        if content_div:
            # 深度清理不需要的元素
            for elem in content_div.find_all(['script', 'style', 'iframe', 'ins', 'a', 'button', 'form',
                                              'table', 'footer', 'header', 'nav', 'aside', 'img', 'figure',
                                              'blockquote', 'ul', 'ol', 'li', 'div.related-articles']):
                elem.decompose()

            # 提取所有文本元素
            text_elements = content_div.find_all(['p', 'div', 'section'])

            # 收集有效段落
            content_paragraphs = []
            for element in text_elements:
                text = element.get_text(separator=' ', strip=True)
                if text and len(text) > 3 and not text.startswith('扫一扫在手机打开当前页'):
                    # 移除多余空格和特殊字符
                    text = re.sub(r'\s+', ' ', text)
                    content_paragraphs.append(text)

            if content_paragraphs:
                result['content'] = "\n\n".join(content_paragraphs)
            else:
                # 回退到直接提取文本
                result['content'] = content_div.get_text(separator='\n', strip=True)
        else:
            # 最终回退：整个页面文本
            result['content'] = soup.get_text(separator='\n', strip=True)

        # 清理内容中的多余空行
        if result['content']:
            result['content'] = re.sub(r'\n{3,}', '\n\n', result['content'])

    except Exception as e:
        logger.error(f"解析文章内容失败: {str(e)}")

    # 记录内容长度用于调试
    content_length = len(result['content']) if result['content'] else 0
    logger.info(f"提取内容长度: {content_length} 字符")

    return result

def clean_jkzx_publish_time(time_str):
    if not time_str:
        return datetime.now()

    # 尝试多种日期格式
    date_formats = [
        "%Y-%m-%d %H:%M:%S",  # 2023-10-15 14:30:45
        "%Y-%m-%d %H:%M",  # 2023-10-15 14:30
        "%Y-%m-%d",  # 2023-10-15
        "%Y/%m/%d %H:%M:%S",  # 2023/10/15 14:30:45
        "%Y/%m/%d %H:%M",  # 2023/10/15 14:30
        "%Y/%m/%d",  # 2023/10/15
        "%Y年%m月%d日 %H:%M",  # 2023年10月15日 14:30
        "%Y年%m月%d日"  # 2023年10月15日
    ]

    # 清理时间字符串
    time_str = re.sub(r'[\s　]+', ' ', time_str).strip()
    time_str = re.sub(r'[^\d\-:/年月日时分秒]', '', time_str)

    # 尝试直接解析
    for fmt in date_formats:
        try:
            return datetime.strptime(time_str, fmt)
        except:
            continue

    # 使用正则表达式提取日期
    date_match = re.search(r'(\d{4})[\/年\-\.](\d{1,2})[\/月\-\.](\d{1,2})', time_str)
    if date_match:
        try:
            year, month, day = map(int, date_match.groups())
            return datetime(year, month, day)
        except:
            pass

    # 尝试提取时间部分
    time_match = re.search(r'(\d{1,2}:\d{1,2}(?::\d{1,2})?', time_str)
    if time_match:
        time_part = time_match.group(0)
        # 尝试与今天的日期组合
        today = datetime.now().date()
        try:
            return datetime.strptime(f"{today} {time_part}", "%Y-%m-%d %H:%M")
        except:
            try:
                return datetime.strptime(f"{today} {time_part}", "%Y-%m-%d %H:%M:%S")
            except:
                pass

    logger.warning(f"无法解析时间: {time_str}，使用当前时间")
    return datetime.now()


def crawl_jkzx_articles(max_pages=125):
    base_url_pattern = "http://jkzx.org.cn/list-zhengcefabu-{}.html"
    logger.info(f"开始爬取健康资讯网政策发布栏目，最多爬取 {max_pages} 页")

    # 创建会话
    session = requests.Session()
    all_articles = []

    # 爬取多个页面（从第1页开始）
    for page in range(100, max_pages + 1):
        try:
            # 构建当前页URL
            current_page_url = base_url_pattern.format(page)
            logger.info(f"开始爬取第 {page}/{max_pages} 页: {current_page_url}")

            # 添加页间延迟
            if page > 1:
                time.sleep(random.uniform(3.0, 6.0))

            # 获取列表页
            list_html, session = fetch_page(current_page_url, session)
            if not list_html:
                logger.error(f"获取第 {page} 页失败，跳过")
                continue

            # 保存列表页用于调试
            debug_filename = f"jkzx_debug/list_page_{page}_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
            with open(debug_filename, 'w', encoding='utf-8') as f:
                f.write(list_html)
            logger.info(f"列表页已保存到: {debug_filename}")

            # 解析列表页
            articles = parse_jkzx_list_page(list_html, current_page_url)
            logger.info(f"第 {page} 页找到 {len(articles)} 篇文章")

            # 添加到总文章列表
            all_articles.extend(articles)

            # 如果没有文章了，提前结束
            if not articles:
                logger.info(f"第 {page} 页没有文章，停止爬取")
                break

        except Exception as e:
            logger.error(f"爬取第 {page} 页时出错: {str(e)}")
            continue

    if not all_articles:
        logger.error("未找到任何文章，终止爬取")
        return []

    logger.info(f"共找到 {len(all_articles)} 篇待处理文章")
    results = []

    # 处理所有文章详情
    for idx, article in enumerate(all_articles):
        try:
            # 随机延迟避免请求过快
            time.sleep(random.uniform(2.0, 4.0))

            logger.info(f"处理文章 [{idx + 1}/{len(all_articles)}]: {article['title']}")

            # 获取文章详情页
            detail_html, session = fetch_page(article['url'], session)
            if not detail_html:
                logger.warning(f"获取文章详情失败: {article['url']}")
                continue

            # 保存详情页用于调试
            detail_debug_filename = f"jkzx_debug/detail_{idx}_{datetime.now().strftime('%H%M%S')}.html"
            with open(detail_debug_filename, 'w', encoding='utf-8') as f:
                f.write(detail_html)
            logger.info(f"详情页已保存: {detail_debug_filename}")

            # 解析文章内容
            article_detail = parse_jkzx_article_content(detail_html, article['url'])

            # 如果没有从详情页获取到标题，使用列表页的标题
            if (article_detail['title'].startswith("文章_") or
                    "健康在线网" in article_detail['title'] or
                    len(article_detail['title']) < 5):
                logger.warning(f"详情页标题无效，使用列表页标题: {article['title']}")
                article_detail['title'] = article['title']

            # 标准化发布时间
            pub_time = clean_jkzx_publish_time(article_detail['publish_time'])

            # 构建结果
            result = {
                'title': article_detail['title'],
                'url': article['url'],
                'publish_time': pub_time,
                'content': article_detail['content'],
                'source': article_detail['source'] or "健康资讯网"
            }

            # 记录最终标题用于调试
            logger.info(f"最终标题: {result['title']}")

            results.append(result)
            logger.info(f"文章处理完成: {result['title']}")

        except Exception as e:
            logger.error(f"处理文章 [{idx + 1}/{len(all_articles)}] 时出错: {str(e)}")
            continue  # 继续处理下一篇文章

    logger.info(f"成功解析 {len(results)} 篇文章")
    return results


def save_to_db(articles):
    """保存文章到数据库 - 优化重复检测"""
    if not articles:
        logger.info("无新文章需要存储")
        return

    db = None
    cursor = None
    try:
        db = get_db_connection()
        cursor = db.cursor()
        count = 0
        skipped = 0

        # 提取所有URL用于批量检查
        urls = [item['url'] for item in articles]

        # 批量查询已存在的URL
        existing_urls = set()
        if urls:
            placeholders = ', '.join(['%s'] * len(urls))
            query = f"SELECT url FROM policy WHERE url IN ({placeholders})"
            cursor.execute(query, urls)
            existing_urls = {row['url'] for row in cursor.fetchall()}

        for item in articles:
            # 主去重：URL检查
            if item['url'] in existing_urls:
                skipped += 1
                logger.info(f"跳过 URL重复: {item['title']} ({item['url']})")
                continue

            # 备选去重：标题+内容组合检查
            title = item['title']
            content_start = item['content'][:100] if item['content'] else ""

            cursor.execute("""
                SELECT COUNT(*) FROM policy 
                WHERE title = %s 
                AND LEFT(content, 100) = %s
            """, (title, content_start))

            if cursor.fetchone()['COUNT(*)'] > 0:
                skipped += 1
                logger.info(f"跳过 标题+内容重复: {title}")
                continue

            # 插入新记录
            try:
                cursor.execute("""
                    INSERT INTO policy (title, url, publish_time, content, source)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    item['title'],
                    item['url'],
                    item['publish_time'],
                    item['content'],
                    item['source']
                ))
                count += 1
                existing_urls.add(item['url'])
                logger.info(f"存储新增: {item['title']}")
            except pymysql.Error as e:
                logger.error(f"插入失败: {item['url']}，原因: {e}")
                skipped += 1

        if count > 0:
            db.commit()
            logger.info(f"存储成功: 新增 {count} 条，跳过 {skipped} 条重复记录")
        else:
            logger.info(f"无新增记录: 所有 {len(articles)} 条均为重复")
    except Exception as e:
        logger.error(f"存储过程失败: {e}")
        if db:
            db.rollback()
    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()


# 定时任务函数
def job():
    try:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"任务开始: {datetime.now()}")
        articles = crawl_jkzx_articles()
        if articles:
            save_to_db(articles)
        logger.info(f"任务结束: {datetime.now()}")
        logger.info(f"{'=' * 60}\n")
    except Exception as e:
        logger.error(f"任务执行失败: {str(e)}")
        logger.error(traceback.format_exc())


# 主程序
if __name__ == "__main__":
    logger.info("健康资讯网政策发布栏目爬虫已启动，每小时运行一次.")

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