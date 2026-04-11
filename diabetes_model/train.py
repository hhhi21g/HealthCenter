import joblib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader, TensorDataset

# 1. 读取数据
df = pd.read_csv('dataset\\diabetes.csv')
print("数据预览：")
print(df.head())
print("\n标签分布：")
print(df['Outcome'].value_counts())

# 2. 数据预处理
X = df.drop('Outcome', axis=1).values
y = df['Outcome'].values

scaler = StandardScaler()
X = scaler.fit_transform(X)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 转为 Tensor
X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
y_train_tensor = torch.tensor(y_train, dtype=torch.float32).unsqueeze(1)
X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
y_test_tensor = torch.tensor(y_test, dtype=torch.float32).unsqueeze(1)

train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
test_dataset = TensorDataset(X_test_tensor, y_test_tensor)

train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=32)


# 3. 构建模型
class DiabetesNet(nn.Module):
    def __init__(self):
        super(DiabetesNet, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(8, 64),
            nn.ReLU(),
            nn.Dropout(0.3),

            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(0.2),

            nn.Linear(32, 16),
            nn.ReLU(),

            nn.Linear(16, 1),

            nn.Sigmoid()
        )

    def forward(self, x):
        return self.net(x)

model = DiabetesNet()

criterion = nn.BCELoss()

optimizer = torch.optim.AdamW(model.parameters(), lr=0.0001)

# 4. 训练
epochs = 200
train_losses = []

for epoch in range(epochs):
    model.train()
    epoch_loss = 0.0
    for X_batch, y_batch in train_loader:
        optimizer.zero_grad()
        outputs = model(X_batch)
        loss = criterion(outputs, y_batch)
        loss.backward()
        optimizer.step()
        epoch_loss += loss.item()

    avg_loss = epoch_loss / len(train_loader)
    train_losses.append(avg_loss)
    print(f"Epoch {epoch + 1}/{epochs}, Loss: {avg_loss:.4f}")

torch.save(model.state_dict(), "diabetes_model.pth")
print("✅ 模型参数已保存为 diabetes_model.pth")

model = DiabetesNet()
model.load_state_dict(torch.load("diabetes_model.pth"))
import joblib

# 保存标准化器
joblib.dump(scaler, "diabetes_scaler.pkl")
print("✅ 标准化器已保存为 models/diabetes_scaler.pkl")



model.eval()  # 设置为推理模式

# 5. 测试评估
model.eval()
with torch.no_grad():
    y_pred = model(X_test_tensor)
    y_pred_labels = (y_pred > 0.5).int().numpy()
    y_true = y_test_tensor.int().numpy()

print("\n📋 Classification Report:")
print(classification_report(y_true, y_pred_labels, digits=4))

# 6. 混淆矩阵可视化
cm = confusion_matrix(y_true, y_pred_labels)
plt.figure(figsize=(5, 4))
plt.imshow(cm, cmap='Blues')
plt.title("Confusion Matrix")
plt.xticks([0, 1], ['No Diabetes', 'Diabetes'])
plt.yticks([0, 1], ['No Diabetes', 'Diabetes'])
plt.xlabel("Predicted")
plt.ylabel("Actual")
for i in range(2):
    for j in range(2):
        plt.text(j, i, cm[i, j], ha='center', va='center', color='black')
plt.colorbar()
plt.tight_layout()
plt.savefig("confusion_matrix.png")
plt.close()
