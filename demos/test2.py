import matplotlib.font_manager as fm
import matplotlib.pyplot as plt

# 方法1：指定系统中已安装的字体名称
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]  # 支持中文

# 方法2：加载自定义字体文件（.ttf 或 .otf）
font_path = "../SimHei.ttf"  # 字体文件路径
my_font = fm.FontProperties(fname=font_path)

# 使用自定义字体
plt.title("标题", fontproperties=my_font)
plt.xlabel("X轴", fontproperties=my_font)
plt.show()