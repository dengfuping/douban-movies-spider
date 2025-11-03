# 豆瓣电影数据爬虫

一个功能完善的豆瓣电影数据爬虫工具，支持爬取多种类型的电影数据，并自动获取详细信息（海报、导演、演员、简介等）。

**GitHub**: https://github.com/dengfuping/douban-movies-spider  
**Kaggle Dataset**: https://www.kaggle.com/datasets/dengfuping/douban-movies-dataset

## 功能特性

- ✅ **多种数据源支持**

  - 豆瓣电影 Top250
  - 豆瓣高分电影（480+ 条记录）
  - 华语电影、欧美电影、日本电影、香港电影（各 500 条记录）
  - 支持一次性爬取所有类型并自动去重

- ✅ **详细信息获取**

  - 默认自动获取电影详情（无需手动确认）
  - 包含海报、导演、演员、编剧、类型、制片国家、语言、上映日期、片长、简介、标签、IMDb 等完整信息

- ✅ **自动去重**

  - 使用电影链接或标题作为唯一标识
  - 支持跨类型去重（选择全部爬取时）

- ✅ **实时保存**

  - 支持逐行写入文件（JSONL 格式），数据不丢失
  - 同时支持 JSON 和 CSV 格式输出

- ✅ **数据标准化**
  - 自动排序字段
  - 限制演员数量（最多 5 个）
  - 限制导演和编剧（只保留第一个）

## 安装说明

### 环境要求

- Python 3.6 或更高版本

### 安装依赖

```bash
pip install -r requirements.txt
```

依赖包：

- `requests>=2.31.0` - HTTP 请求库
- `beautifulsoup4>=4.12.0` - HTML 解析库

## 使用方法

### 基本使用

直接运行脚本，按提示选择爬取类型：

```bash
python3 douban_movies_spider.py
```

### 爬取选项

运行后会显示以下选项：

```
请选择爬取类型:
1. 豆瓣电影 Top250
2. 豆瓣高分电影（480 条记录）
3. 全部华语电影（500 条记录）
4. 全部欧美电影（500 条记录）
5. 全部日本电影（500 条记录）
6. 全部香港电影（500 条记录）
7. 爬取上述所有电影，并去重
```

### 输出文件

爬取完成后，会在项目目录下生成以下格式的文件：

- **JSON 格式** (`*.json`): 完整的 JSON 数组，格式化输出
- **JSONL 格式** (`*.jsonl`): 每行一个 JSON 对象，追加模式，推荐用于大数据集
- **CSV 格式** (`*.csv`): 表格格式，便于 Excel 等工具打开

文件保存位置：

- 所有数据文件统一保存在 `data/` 目录

## 数据字段说明

电影数据包含以下字段（按顺序）：

| 字段名          | 说明               | 类型   |
| --------------- | ------------------ | ------ |
| `movie_id`      | 电影 ID（豆瓣）    | 整数   |
| `title`         | 电影标题           | 字符串 |
| `rating`        | 评分               | 浮点数 |
| `total_ratings` | 评价人数           | 整数   |
| `directors`     | 导演（仅第一个）   | 字符串 |
| `actors`        | 主演（最多 5 个）  | 字符串 |
| `screenwriters` | 编剧（仅第一个）   | 字符串 |
| `release_date`  | 上映日期           | 字符串 |
| `genres`        | 类型               | 字符串 |
| `countries`     | 制片国家/地区      | 字符串 |
| `languages`     | 语言               | 字符串 |
| `runtime`       | 片长               | 字符串 |
| `summary`       | 简介               | 字符串 |
| `tags`          | 标签（最多 20 个） | 字符串 |
| `imdb`          | IMDb 链接          | 字符串 |
| `link`          | 豆瓣链接           | 字符串 |
| `poster`        | 海报链接           | 字符串 |

## 项目结构

```
douban_movies_spider/
├── douban_movies_spider.py       # 主程序
├── kaggle_upload.py            # Kaggle 上传工具
├── requirements.txt             # 依赖包列表
├── README.md                    # 项目说明文档
└── data/                        # 数据文件目录
    ├── dataset-metadata.json    # Kaggle 数据集元数据
    ├── DATASET.md               # 数据集说明文档
    ├── douban_all_movies.*      # 所有电影（去重后）
    ├── douban_top250_movies.*   # Top250 数据
    ├── douban_high_rating.*     # 高分电影数据
    ├── douban_chinese_movies.*  # 华语电影数据
    ├── douban_western_movies.*  # 欧美电影数据
    ├── douban_japanese_movies.* # 日本电影数据
    └── douban_hongkong_movies.* # 香港电影数据
```

## 注意事项

1. **爬取速度**

   - 默认会获取详细信息，需要访问每个电影的详情页
   - 为避免被反爬，每次请求之间有 1-3 秒的随机延迟
   - 爬取大量数据可能需要较长时间，请耐心等待

2. **网络要求**

   - 需要能够访问豆瓣网站（movie.douban.com）
   - 建议在网络稳定时运行

3. **数据去重**

   - 使用选项 7（爬取所有电影）时，会自动使用 movie_id 进行去重
   - 确保每个电影只保存一次

4. **文件覆盖**

   - 每次运行会删除旧的 JSONL 和 CSV 文件（如果启用实时保存）
   - JSON 文件会在最后保存完整列表，会覆盖同名文件

5. **中断恢复**
   - JSONL 格式支持追加模式，可以部分恢复数据
   - 建议优先查看 JSONL 文件

## 上传到 Kaggle

项目包含 Kaggle 数据集上传工具，可以将数据上传到 Kaggle 平台。

### 一键上传（推荐）

**创建新数据集**（不带 `-m` 参数）：

```bash
python3 kaggle_upload.py
```

**更新现有数据集**（带 `-m` 参数）：

```bash
python3 kaggle_upload.py -m "更新数据集，新增 2024 年数据"
```

**注意**：

- 不带 `-m` 参数：创建新数据集
- 带 `-m` 参数：更新现有数据集（必须提供版本说明）

一行命令即可完成：

- ✅ 准备上传文件（CSV + 元数据）
- ✅ 上传到 Kaggle（创建或更新）
- ✅ 自动清理临时目录

### 脚本功能

脚本会自动：

- ✅ 创建 `kaggle-upload/` 目录
- ✅ 复制所有 CSV 文件
- ✅ 复制 `dataset-metadata.json` 和 `DATASET.md`
- ✅ 排除所有 JSON 和 JSONL 文件
- ✅ 检查 Kaggle CLI 是否安装（默认行为）
- ✅ 自动上传到 Kaggle（默认行为）
- ✅ 自动清理临时目录（默认行为）

### 注意事项

- **Kaggle CLI 要求**：需要先安装并配置 Kaggle CLI
  ```bash
  pip3 install kaggle
  # 配置 API token (参考: https://www.kaggle.com/docs/api)
  ```
- **`.kaggleignore` 不支持**：由于 Kaggle CLI 不支持 `.kaggleignore` 文件，脚本会自动创建独立的上传目录来确保只上传 CSV 文件。

更多详细信息请参考项目中的数据集文档：`data/DATASET.md`

## 代码特点

- **代码复用**: 使用统一的方法处理电影详情获取、标准化和保存
- **简洁高效**: 合并了重复代码逻辑，代码更易维护
- **日志优化**: 简洁明了的进度显示和错误提示

## 许可证

本项目仅供学习和研究使用，请遵守豆瓣的使用条款和 robots.txt。
