import matplotlib.font_manager as fm

# 列出所有可识别的字体名称
font_paths = fm.findSystemFonts()
font_names = [f for f in font_paths]

# 搜索与"黑体"相关的字体（SimHei通常对应"黑体"）
for name in font_names:
    if "hei" in name.lower() or "黑体" in name:
        print(name)