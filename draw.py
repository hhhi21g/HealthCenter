import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
matplotlib.use('TkAgg')  # 强制使用 Tk 后端（可视化支持最稳定）


# 替换为你自己的混淆矩阵值
TN, FP, FN, TP = 90, 9, 23, 32

# 构造混淆矩阵（2×2）
cm = np.array([[TN, FP],
               [FN, TP]])

labels = ['Disease', 'No Disease']

plt.figure(figsize=(6, 5))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
            xticklabels=labels, yticklabels=labels)

plt.xlabel('Predicted')
plt.ylabel('True')
plt.title('Confusion Matrix (diabetes)')
plt.tight_layout()
plt.show()
