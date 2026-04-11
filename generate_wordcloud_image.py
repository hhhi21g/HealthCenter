import pymysql
import jieba
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# 1. 数据库配置
db_config = {
    'host': '212.129.223.4',
    'user': 'root',
    'password': 'nineone4536251',
    'database': 'health',
    'charset': 'utf8mb4'
}

# 2. 提取标题字段
def fetch_titles():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT title FROM policy")  # 可改为其他表名
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]

# 3. 分词统计
def extract_keywords(titles):
    words = []
    for title in titles:
        words += jieba.lcut(title)
    stopwords = set("的了是和在与就也为不对等及 关于 如何 做好 这些 这样".split())
    words = [w for w in words if len(w) > 1 and w not in stopwords]
    return Counter(words)

# 4. 生成词云图
def generate_wordcloud(counter, output_path='wordcloud_policy.png'):
    wc = WordCloud(
        font_path='simhei.ttf',  # 中文字体路径，确保有 simhei.ttf
        background_color='white',
        width=800,
        height=600,
        max_words=200
    )
    wc.generate_from_frequencies(counter)
    wc.to_file(output_path)

    # 可选：显示图片
    plt.imshow(wc, interpolation='bilinear')
    plt.axis("off")
    plt.show()

# 5. 主流程
if __name__ == '__main__':
    titles = fetch_titles()
    keyword_freq = extract_keywords(titles)
    generate_wordcloud(keyword_freq)
