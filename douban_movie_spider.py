#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
豆瓣电影数据爬虫
支持爬取：
1. 豆瓣电影 Top250
2. 豆瓣高分电影（480 条记录）
3. 全部华语电影（500 条记录）
4. 全部欧美电影（500 条记录）
5. 全部日本电影（500 条记录）
6. 全部香港电影（500 条记录）
7. 爬取上述所有电影，并去重

特性：
- 自动去重（使用电影链接或标题作为唯一标识）
- 实时保存（逐行写入文件）
- 详细信息支持（海报、导演、演员、简介等）
"""

import sys

# 检查Python版本
if sys.version_info < (3, 6):
    print("错误: 此脚本需要Python 3.6或更高版本")
    print("当前Python版本: {}".format(sys.version))
    print("请使用 'python3' 命令运行，或升级Python版本")
    sys.exit(1)

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import random
import re
from typing import List, Dict
import os
from urllib.parse import urljoin

class DoubanMovieSpider:
    """豆瓣电影爬虫类"""
    
    # 定义字段的合理排序
    FIELD_ORDER = [
        # 基本信息
        'movie_id', 'title',
        # 评分信息
        'rating', 'total_ratings',
        # 人员信息
        'directors', 'actors', 'screenwriters',
        # 影片信息
        'release_date', 'genres', 'countries', 'languages', 'runtime',
        # 内容信息
        'summary', 'tags',
        # 其他信息
        'imdb', 'link', 'category', 'box_office',
        # 海报（放最后）
        'poster'
    ]
    
    def __init__(self):
        self.base_url = "https://movie.douban.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def normalize_movie_data(self, movie: Dict) -> Dict:
        """
        标准化电影数据：排序字段、限制人名字段
        
        Args:
            movie: 原始电影数据字典
            
        Returns:
            标准化后的电影数据字典
        """
        # 1. 删除 rank 字段
        if 'rank' in movie:
            del movie['rank']
        
        # 2. 删除 other_titles 字段
        if 'other_titles' in movie:
            del movie['other_titles']
        
        # 3. 限制演员字段：最多保留5个主演
        if 'actors' in movie and movie['actors']:
            actors_list = [a.strip() for a in movie['actors'].split(',') if a.strip()]
            if actors_list:
                # 最多保留5个
                movie['actors'] = ', '.join(actors_list[:5])
        
        # 4. 限制导演：只保留第一个（主导演）
        if 'directors' in movie and movie['directors']:
            directors_list = [d.strip() for d in movie['directors'].split(',') if d.strip()]
            if directors_list:
                movie['directors'] = directors_list[0]
        
        # 5. 限制编剧：只保留第一个（主编剧）
        if 'screenwriters' in movie and movie.get('screenwriters'):
            screenwriters_list = [s.strip() for s in movie['screenwriters'].split(',') if s.strip()]
            if screenwriters_list:
                movie['screenwriters'] = screenwriters_list[0]
        
        # 6. 删除 also_known_as 字段
        if 'also_known_as' in movie:
            del movie['also_known_as']
        
        # 7. 删除所有评分分布字段
        for key in ['rating_5star', 'rating_4star', 'rating_3star', 'rating_2star', 'rating_1star']:
            if key in movie:
                del movie[key]
        
        # 8. 删除 quote 字段
        if 'quote' in movie:
            del movie['quote']
        
        # 9. 删除 info 字段
        if 'info' in movie:
            del movie['info']
        
        # 9.2. 删除 category 字段
        if 'category' in movie:
            del movie['category']
        
        # 9.3. 删除 rating_detail 字段
        if 'rating_detail' in movie:
            del movie['rating_detail']
        
        # 9.5. 确保 rating 为数字类型（浮点数）
        if 'rating' in movie and movie.get('rating'):
            rating_str = str(movie['rating']).strip()
            if rating_str:
                try:
                    # 尝试转换为浮点数
                    movie['rating'] = float(rating_str)
                except (ValueError, TypeError):
                    # 如果转换失败，设为0.0
                    movie['rating'] = 0.0
            else:
                movie['rating'] = 0.0
        elif 'rating' not in movie:
            movie['rating'] = 0.0
        
        # 10. 转换 people 为 total_ratings，提取数字并保存为整数类型（JSON中不带引号）
        if 'people' in movie and movie.get('people'):
            people_text = movie['people']
            # 提取数字部分（例如："3225399人评价" -> 3225399）
            numbers = re.findall(r'\d+', people_text.replace(',', '').replace('，', ''))
            if numbers:
                # 取第一个数字（通常是评价人数），转换为整数
                try:
                    movie['total_ratings'] = int(numbers[0])
                except ValueError:
                    movie['total_ratings'] = 0
            else:
                movie['total_ratings'] = 0
            del movie['people']
        elif 'total_ratings' not in movie:
            movie['total_ratings'] = 0
        elif isinstance(movie.get('total_ratings'), str):
            # 如果total_ratings是字符串，尝试转换为整数
            try:
                # 提取数字部分
                numbers = re.findall(r'\d+', str(movie['total_ratings']).replace(',', '').replace('，', ''))
                if numbers:
                    movie['total_ratings'] = int(numbers[0])
                else:
                    movie['total_ratings'] = 0
            except (ValueError, TypeError):
                movie['total_ratings'] = 0
        
        # 10.5. 提取 movie_id（从 link 中提取，如果还没有设置），转换为整数
        if 'movie_id' not in movie or not movie.get('movie_id'):
            if 'link' in movie and movie.get('link'):
                link = movie['link']
                # 从链接中提取电影ID，例如：https://movie.douban.com/subject/1292052/
                match = re.search(r'/subject/(\d+)/', link)
                if match:
                    try:
                        movie['movie_id'] = int(match.group(1))
                    except (ValueError, TypeError):
                        movie['movie_id'] = 0
                else:
                    movie['movie_id'] = 0
            else:
                movie['movie_id'] = 0
        else:
            # 如果已有 movie_id，确保它是整数类型
            movie_id_value = movie.get('movie_id')
            if isinstance(movie_id_value, str) and movie_id_value.strip():
                try:
                    movie['movie_id'] = int(movie_id_value)
                except (ValueError, TypeError):
                    movie['movie_id'] = 0
            elif not isinstance(movie_id_value, int):
                movie['movie_id'] = 0
        
        # 11. 清理 countries 和 languages 字段（如果包含了过多内容，可能是解析错误）
        if 'countries' in movie and movie.get('countries'):
            countries_text = movie['countries']
            # 如果包含"导演"、"主演"等关键词，说明解析错误，清空
            if '导演' in countries_text or '主演' in countries_text or len(countries_text) > 200:
                movie['countries'] = ''
        
        if 'languages' in movie and movie.get('languages'):
            languages_text = movie['languages']
            # 如果包含"导演"、"主演"等关键词，说明解析错误，清空
            if '导演' in languages_text or '主演' in languages_text or len(languages_text) > 200:
                movie['languages'] = ''
        
        # 12. 处理上映时间：只保留最早地区的完整日期（替换release_year）
        if 'release_dates' in movie and movie.get('release_dates'):
            release_dates = movie['release_dates']
            # 提取最早的日期（可能有多个日期，用逗号分隔）
            dates = [d.strip() for d in release_dates.split(',') if d.strip()]
            if dates:
                # 取第一个日期（最早的）
                first_date = dates[0]
                # 提取完整日期（格式可能是：1994-09-10(多伦多电影节) 或 1994-09-23(加拿大)）
                # 尝试提取 YYYY-MM-DD 格式
                date_match = re.search(r'(\d{4}-\d{1,2}-\d{1,2})', first_date)
                if date_match:
                    movie['release_date'] = date_match.group(1)
                else:
                    # 如果没有完整日期，尝试提取 YYYY-MM
                    date_match = re.search(r'(\d{4}-\d{1,2})', first_date)
                    if date_match:
                        movie['release_date'] = date_match.group(1)
                    else:
                        # 如果只有年份，保存年份
                        year_match = re.search(r'(\d{4})', first_date)
                        if year_match:
                            movie['release_date'] = year_match.group(1)
            # 删除 release_dates 字段（只保留 release_date）
            del movie['release_dates']
        elif 'release_date' not in movie or not movie.get('release_date'):
            # 如果没有 release_dates，尝试从其他地方提取
            # 如果有旧的 release_year，转换为 release_date
            if 'release_year' in movie and movie.get('release_year'):
                movie['release_date'] = movie['release_year']
                del movie['release_year']
            elif 'info' in movie and movie.get('info'):
                year_match = re.search(r'(\d{4})', movie['info'])
                if year_match:
                    year_str = year_match.group(1)
                    year = int(year_str)
                    if 1900 <= year <= 2100:
                        movie['release_date'] = year_str  # 保存为字符串
        # 确保 release_date 是字符串格式
        if 'release_date' in movie and isinstance(movie.get('release_date'), int):
            movie['release_date'] = str(movie['release_date'])
        # 删除旧的 release_year 字段（如果存在）
        if 'release_year' in movie:
            del movie['release_year']
        
        # 按照定义的顺序排序字段
        ordered_movie = {}
        # 先添加有序字段
        for field in self.FIELD_ORDER:
            if field in movie:
                ordered_movie[field] = movie[field]
        # 再添加其他未定义的字段
        for key, value in movie.items():
            if key not in self.FIELD_ORDER:
                ordered_movie[key] = value
        
        return ordered_movie
        
    def print_movie_info(self, movie: Dict):
        """
        格式化打印电影信息
        
        Args:
            movie: 电影信息字典
        """
        print("\n" + "="*80)
        print(f"{movie.get('title', '未知')}")
        print("-"*80)
        
        # 评分信息
        rating = movie.get('rating', '')
        total_ratings = movie.get('total_ratings', '')
        if rating:
            print(f"评分: {rating}", end='')
            if total_ratings:
                print(f" (评价人数: {total_ratings})")
            else:
                print()
        
        # 导演
        if movie.get('directors'):
            print(f"导演: {movie['directors']}")
        
        # 演员
        if movie.get('actors'):
            print(f"主演: {movie['actors']}")
        
        # 编剧
        if movie.get('screenwriters'):
            print(f"编剧: {movie['screenwriters']}")
        
        # 年份和上映日期
        if movie.get('release_date'):
            print(f"上映日期: {movie['release_date']}")
        if movie.get('tags'):
            print(f"标签: {movie['tags']}")
        
        # 类型
        if movie.get('genres'):
            print(f"类型: {movie['genres']}")
        
        # 制片国家和语言
        if movie.get('countries'):
            print(f"制片国家/地区: {movie['countries']}")
        if movie.get('languages'):
            print(f"语言: {movie['languages']}")
        
        # 片长
        if movie.get('runtime'):
            print(f"片长: {movie['runtime']}")
        
        # 简介（完整显示）
        if movie.get('summary'):
            summary = movie['summary']
            print(f"简介: {summary}")
        
        # 海报链接
        if movie.get('poster'):
            print(f"海报: {movie['poster']}")
        
        # 详情链接
        if movie.get('link'):
            print(f"链接: {movie['link']}")
        
        # IMDb
        if movie.get('imdb'):
            print(f"IMDb: {movie['imdb']}")
        
        print("="*80 + "\n")
    
    def get_page(self, url: str, retry: int = 3) -> BeautifulSoup:
        """
        获取页面内容
        
        Args:
            url: 目标URL
            retry: 重试次数
            
        Returns:
            BeautifulSoup对象
        """
        for i in range(retry):
            try:
                # 随机延迟，避免被反爬
                time.sleep(random.uniform(1, 3))
                
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                response.encoding = 'utf-8'
                
                return BeautifulSoup(response.text, 'html.parser')
            except Exception as e:
                if i == retry - 1:
                    print(f"获取页面失败: {url} - {str(e)}")
                    raise
                time.sleep(2)
        return None
    
    def _process_movie_with_detail(self, movie: Dict, fetch_detail: bool = True, 
                                   save_immediately: bool = False,
                                   jsonl_filename: str = None,
                                   json_filename: str = None,
                                   csv_filename: str = None) -> Dict:
        """
        统一的电影处理方法：获取详情、标准化数据、保存
        
        Args:
            movie: 电影基本信息字典
            fetch_detail: 是否获取详细信息
            save_immediately: 是否立即保存
            jsonl_filename: JSONL文件名
            json_filename: JSON文件名
            csv_filename: CSV文件名
            
        Returns:
            处理后的电影数据字典
        """
        # 获取详细信息
        if fetch_detail and movie.get('link'):
            detail_info = self.parse_movie_detail(movie['link'])
            if detail_info:
                movie = self._merge_detail_info(movie, detail_info)
            time.sleep(random.uniform(1, 3))
        
        # 标准化数据
        movie = self.normalize_movie_data(movie)
        
        # 保存到文件
        if save_immediately:
            self.save_movie_line(
                movie,
                jsonl_filename=jsonl_filename,
                json_filename=json_filename,
                csv_filename=csv_filename
            )
        
        return movie
    
    def _merge_detail_info(self, movie: Dict, detail_info: Dict) -> Dict:
        """
        统一合并详情信息到电影数据中
        
        Args:
            movie: 电影数据字典
            detail_info: 从详情页解析的详细信息字典
            
        Returns:
            合并后的电影数据字典
        """
        if not detail_info:
            return movie
        
        # 合并各个字段（按统一顺序）
        if detail_info.get('poster'):
            movie['poster'] = detail_info['poster']
        if detail_info.get('directors'):
            movie['directors'] = detail_info['directors']
        if detail_info.get('actors'):
            movie['actors'] = detail_info['actors']
        if detail_info.get('screenwriters'):
            movie['screenwriters'] = detail_info['screenwriters']
        if detail_info.get('genres'):
            movie['genres'] = detail_info['genres']
        if detail_info.get('countries'):
            movie['countries'] = detail_info['countries']
        if detail_info.get('languages'):
            movie['languages'] = detail_info['languages']
        if detail_info.get('release_dates'):
            movie['release_dates'] = detail_info['release_dates']
        if detail_info.get('runtime'):
            movie['runtime'] = detail_info['runtime']
        if detail_info.get('summary'):
            movie['summary'] = detail_info['summary']
        if detail_info.get('tags'):
            movie['tags'] = detail_info['tags']
        if detail_info.get('imdb'):
            movie['imdb'] = detail_info['imdb']
        
        # 处理评分信息（从 rating_detail 中提取）
        if detail_info.get('rating_detail'):
            rating_detail = detail_info['rating_detail']
            if rating_detail.get('average'):
                avg_rating = rating_detail['average']
                if isinstance(avg_rating, str):
                    try:
                        movie['rating'] = float(avg_rating)
                    except (ValueError, TypeError):
                        movie['rating'] = movie.get('rating', 0.0)
                else:
                    movie['rating'] = float(avg_rating) if avg_rating else movie.get('rating', 0.0)
            if rating_detail.get('total_ratings'):
                movie['total_ratings'] = rating_detail['total_ratings']
        
        return movie
    
    def parse_movie_detail(self, movie_url: str) -> Dict:
        """
        从电影详情页解析详细信息
        
        Args:
            movie_url: 电影详情页URL
            
        Returns:
            包含详细信息的字典
        """
        detail_info = {}
        
        try:
            soup = self.get_page(movie_url)
            if not soup:
                return detail_info
            
            # 电影海报
            poster_elem = soup.find('img', title=lambda x: x) or soup.find('a', class_='nbgnbg')
            if poster_elem:
                if poster_elem.name == 'img':
                    detail_info['poster'] = poster_elem.get('src', '')
                else:
                    img = poster_elem.find('img')
                    if img:
                        detail_info['poster'] = img.get('src', '')
            else:
                # 尝试另一种方式
                poster_elem = soup.find('div', id='mainpic') or soup.find('div', class_='pic')
                if poster_elem:
                    img = poster_elem.find('img')
                    if img:
                        detail_info['poster'] = img.get('src', '')
            
            # 评分和评分分布
            rating_info = {}
            rating_num = soup.find('strong', class_='ll rating_num')
            if rating_num:
                rating_str = rating_num.get_text(strip=True)
                # 转换为浮点数
                try:
                    rating_info['average'] = float(rating_str)
                except (ValueError, TypeError):
                    rating_info['average'] = 0.0
            
            # 评分人数
            rating_people = soup.find('a', class_='rating_people')
            if rating_people:
                people_text = rating_people.get_text(strip=True)
                # 提取数字部分，保存为整数类型（JSON中不带引号）
                numbers = re.findall(r'\d+', people_text.replace(',', '').replace('，', ''))
                if numbers:
                    try:
                        rating_info['total_ratings'] = int(numbers[0])  # 保存为整数
                    except ValueError:
                        rating_info['total_ratings'] = 0
                else:
                    rating_info['total_ratings'] = 0
            
            # 不再保存评分分布信息
            
            detail_info['rating_detail'] = rating_info
            
            # 电影信息区域
            info_area = soup.find('div', id='info')
            if info_area:
                # 导演 - 方法1: 通过rel属性
                director_links = info_area.find_all('a', rel='v:directedBy')
                directors = [link.get_text(strip=True) for link in director_links]
                
                # 方法2: 如果没有找到，尝试通过文本匹配
                if not directors:
                    director_spans = info_area.find_all('span', string=lambda x: x and '导演' in str(x) if x else False)
                    for span in director_spans:
                        parent = span.parent
                        if parent:
                            director_links = parent.find_all('a')
                            if director_links:
                                directors = [link.get_text(strip=True) for link in director_links]
                                break
                
                detail_info['directors'] = ', '.join(directors) if directors else ''
                
                # 编剧
                screenwriter_spans = info_area.find_all('span', string=lambda x: x and '编剧' in str(x) if x else False)
                screenwriters = []
                if screenwriter_spans:
                    for span in screenwriter_spans:
                        parent = span.parent
                        if parent:
                            screenwriter_links = parent.find_all('a')
                            if screenwriter_links:
                                screenwriters = [link.get_text(strip=True) for link in screenwriter_links]
                                break
                detail_info['screenwriters'] = ', '.join(screenwriters) if screenwriters else ''
                
                # 主演 - 方法1: 通过rel属性
                actor_links = info_area.find_all('a', rel='v:starring')
                actors = [link.get_text(strip=True) for link in actor_links]
                
                # 方法2: 如果没有找到，尝试通过文本匹配
                if not actors:
                    actor_spans = info_area.find_all('span', string=lambda x: x and '主演' in str(x) if x else False)
                    for span in actor_spans:
                        parent = span.parent
                        if parent:
                            actor_links = parent.find_all('a')
                            if actor_links:
                                actors = [link.get_text(strip=True) for link in actor_links]
                                break
                
                detail_info['actors'] = ', '.join(actors) if actors else ''
                
                # 类型
                genre_links = info_area.find_all('span', property='v:genre')
                genres = [link.get_text(strip=True) for link in genre_links]
                detail_info['genres'] = ', '.join(genres)
                
                # 制片国家/地区 - 改进解析方法，只提取国家名称
                country_spans = info_area.find_all('span', string=lambda x: x and '制片国家' in str(x) if x else False)
                if country_spans:
                    for span in country_spans:
                        # 查找包含"制片国家/地区"的span，然后获取其父元素后面的内容
                        parent = span.parent
                        if parent:
                            # 方法1: 查找下一个兄弟节点
                            next_node = parent.next_sibling
                            if next_node:
                                if hasattr(next_node, 'strip'):
                                    country_text = next_node.strip()
                                else:
                                    country_text = next_node.get_text(strip=True) if hasattr(next_node, 'get_text') else str(next_node).strip()
                                if country_text and len(country_text) < 200:  # 确保不是整个info文本
                                    detail_info['countries'] = country_text
                                    break
                            
                            # 方法2: 从父元素完整文本中提取（更可靠）
                            full_text = parent.get_text(separator=' ', strip=True)
                            if '制片国家/地区:' in full_text:
                                # 提取"制片国家/地区:"后面的内容，直到遇到下一个字段
                                parts = full_text.split('制片国家/地区:')
                                if len(parts) > 1:
                                    country_text = parts[1].split('语言:')[0].split('上映日期:')[0].strip()
                                    if country_text and len(country_text) < 200:
                                        detail_info['countries'] = country_text
                                        break
                
                # 语言 - 改进解析方法，只提取语言名称
                language_spans = info_area.find_all('span', string=lambda x: x and '语言' in str(x) if x else False)
                if language_spans:
                    for span in language_spans:
                        parent = span.parent
                        if parent:
                            # 方法1: 查找下一个兄弟节点
                            next_node = parent.next_sibling
                            if next_node:
                                if hasattr(next_node, 'strip'):
                                    languages_text = next_node.strip()
                                else:
                                    languages_text = next_node.get_text(strip=True) if hasattr(next_node, 'get_text') else str(next_node).strip()
                                if languages_text and len(languages_text) < 200:  # 确保不是整个info文本
                                    detail_info['languages'] = languages_text
                                    break
                            
                            # 方法2: 从父元素完整文本中提取（更可靠）
                            full_text = parent.get_text(separator=' ', strip=True)
                            if '语言:' in full_text:
                                # 提取"语言:"后面的内容，直到遇到下一个字段
                                parts = full_text.split('语言:')
                                if len(parts) > 1:
                                    languages_text = parts[1].split('上映日期:')[0].split('片长:')[0].split('又名:')[0].strip()
                                    if languages_text and len(languages_text) < 200:
                                        detail_info['languages'] = languages_text
                                        break
                
                # 上映日期（保存原始数据，后续会在normalize_movie_data中处理）
                release_dates = info_area.find_all('span', property='v:initialReleaseDate')
                dates = [date.get_text(strip=True) for date in release_dates]
                if dates:
                    detail_info['release_dates'] = ', '.join(dates)
                
                # 片长
                runtime = info_area.find('span', property='v:runtime')
                if runtime:
                    detail_info['runtime'] = runtime.get_text(strip=True)
                
                # 不再保存 also_known_as 字段
                
                # IMDb
                imdb_elem = info_area.find('a', href=lambda x: x and 'imdb.com' in x if x else False)
                if imdb_elem:
                    detail_info['imdb'] = imdb_elem.get('href', '')
            
            # 简介/剧情 - 获取完整简介（确保不截断）
            # 根据DOM结构：class="short" 为截断版本，class="all hidden" 为完整版本
            summary_text = ''
            
            # 方法1: 优先查找 id="link-report-intra" 或 id="link-report" 的div
            summary_div = (soup.find('div', id='link-report-intra') or 
                          soup.find('div', class_='indent', id='link-report') or
                          soup.find('div', class_='indent'))
            
            if summary_div:
                # 优先查找完整版本：class="all hidden" 或 class="all" 的span（完整简介）
                # 避免使用 class="short" 的span（截断版本）
                
                # 方法1：直接查找 class="all" 的span（可能是 "all hidden" 或 "all"）
                summary_all_span = summary_div.find('span', class_=lambda x: x and 'all' in x if x else False)
                if summary_all_span:
                    # 提取完整简介文本
                    summary_text = summary_all_span.get_text(separator=' ', strip=True)
                    # 移除 "展开全部"、"©豆瓣" 等按钮和链接文本
                    summary_text = re.sub(r'(展开全部|\(展开全部\)|收起|©豆瓣)', '', summary_text)
                    # 清理多余的空白字符
                    summary_text = re.sub(r'\s+', ' ', summary_text).strip()
                
                # 方法2：如果没找到 "all" span，尝试查找 property="v:summary" 的span
                # 但要确保它在 "all" 中，而不是在 "short" 中
                if not summary_text or len(summary_text) < 100:
                    v_summary_spans = summary_div.find_all('span', property='v:summary')
                    for v_summary_span in v_summary_spans:
                        # 检查这个span的父元素
                        parent = v_summary_span.parent
                        if parent:
                            parent_classes = parent.get('class', [])
                            # 如果父元素有 "all" 类，这是完整版本，使用它
                            if 'all' in parent_classes:
                                summary_text = v_summary_span.get_text(separator=' ', strip=True)
                                summary_text = re.sub(r'\s+', ' ', summary_text).strip()
                                break
                            # 如果父元素有 "short" 类，这是截断版本，跳过
                            # 但需要查找同级的 "all" span
                            elif 'short' in parent_classes:
                                # 查找同级的 "all" span（完整版本）
                                for sibling in parent.find_next_siblings('span'):
                                    if 'all' in sibling.get('class', []):
                                        # 在 "all" span 中查找 property="v:summary" 或直接取文本
                                        v_summary_in_all = sibling.find('span', property='v:summary')
                                        if v_summary_in_all:
                                            summary_text = v_summary_in_all.get_text(separator=' ', strip=True)
                                        else:
                                            summary_text = sibling.get_text(separator=' ', strip=True)
                                        summary_text = re.sub(r'(展开全部|\(展开全部\)|收起|©豆瓣)', '', summary_text)
                                        summary_text = re.sub(r'\s+', ' ', summary_text).strip()
                                        break
                
                # 方法3：如果还是没有，尝试从所有span中选择最长的（排除short）
                if not summary_text or len(summary_text) < 100:
                    all_spans = summary_div.find_all('span')
                    candidates = []
                    for span in all_spans:
                        span_classes = span.get('class', [])
                        # 跳过 "short" 类（截断版本）
                        if 'short' in span_classes:
                            continue
                        span_text = span.get_text(separator=' ', strip=True)
                        # 排除按钮、链接等干扰文本
                        if ('展开全部' in span_text or '收起' in span_text or '©豆瓣' in span_text or 
                            len(span_text) < 50):
                            continue
                        # 清理文本
                        clean_text = re.sub(r'(展开全部|\(展开全部\)|收起|©豆瓣|\(展开\)|\(收起\))', '', span_text)
                        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                        if clean_text and len(clean_text) > 50:
                            candidates.append((len(clean_text), clean_text))
                    
                    # 选择最长的文本（通常是完整版本）
                    if candidates:
                        candidates.sort(reverse=True)
                        summary_text = candidates[0][1]
            
            # 方法2: 如果还没有，尝试property='v:summary'
            if not summary_text:
                summary_span = soup.find('span', property='v:summary')
                if summary_span:
                    summary_text = summary_span.get_text(separator=' ', strip=True)
            
            # 方法3: 尝试查找其他可能的简介位置
            if not summary_text or len(summary_text) < 100:
                # 尝试查找其他常见的简介容器
                for selector in ['div.intro', 'div.summary', 'div.movie-summary', 'div.content']:
                    intro_div = soup.select_one(selector)
                    if intro_div:
                        intro_text = intro_div.get_text(separator=' ', strip=True)
                        if intro_text and len(intro_text) > len(summary_text):
                            summary_text = intro_text
                            break
            
            # 方法4: 尝试从script标签中提取（某些动态加载的内容可能在script中）
            if not summary_text or len(summary_text) < 100:
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string and 'summary' in script.string.lower():
                        # 尝试提取JSON数据中的summary
                        json_match = re.search(r'"summary":\s*"([^"]+)"', script.string)
                        if json_match:
                            script_summary = json_match.group(1)
                            if len(script_summary) > len(summary_text):
                                summary_text = script_summary
                                break
            
            # 最终清理和设置
            if summary_text:
                # 移除多余的空白字符
                summary_text = re.sub(r'\s+', ' ', summary_text).strip()
                detail_info['summary'] = summary_text
            
            # 提取标签/关键词
            tags = []
            seen_tags = set()
            
            # 方法1: 查找标签区域（多种可能的class和id）
            tag_selectors = [
                'div.tags-body',
                'div#db-tags-section',
                'div.tags',
                'div.movie-tags',
                'section.tags-section',
                'div[id*="tag"]',
                'div[class*="tag"]'
            ]
            
            for selector in tag_selectors:
                try:
                    tags_section = soup.select_one(selector)
                    if tags_section:
                        # 查找所有可能的标签链接
                        tag_links = tags_section.find_all('a', class_=lambda x: x and ('tag' in str(x) or not x))
                        for link in tag_links:
                            tag_text = link.get_text(strip=True)
                            href = link.get('href', '')
                            # 确认是标签链接（包含/tag/路径）
                            if '/tag/' in href and tag_text and tag_text not in seen_tags and len(tag_text) < 50:
                                tags.append(tag_text)
                                seen_tags.add(tag_text)
                        if tags:
                            break
                except:
                    continue
            
            # 方法2: 如果没有找到标签区域，查找所有包含/tag/的链接
            if not tags:
                tag_elements = soup.find_all('a', href=lambda x: x and '/tag/' in str(x) if x else False)
                for tag_elem in tag_elements:
                    tag_text = tag_elem.get_text(strip=True)
                    href = tag_elem.get('href', '')
                    # 确保是有效的标签（过滤掉导航链接等）
                    if tag_text and tag_text not in seen_tags and len(tag_text) < 50 and len(tag_text) > 0:
                        # 进一步过滤：排除一些常见的非标签链接文本
                        if tag_text not in ['标签', '更多', '添加', '管理', '查看全部']:
                            tags.append(tag_text)
                            seen_tags.add(tag_text)
                            if len(tags) >= 30:  # 临时增加限制以便收集更多
                                break
            
            # 方法3: 尝试从script标签或JSON数据中提取标签
            if not tags:
                scripts = soup.find_all('script', type='application/ld+json')
                for script in scripts:
                    if script.string:
                        try:
                            data = json.loads(script.string)
                            if isinstance(data, dict):
                                # 查找keywords字段
                                keywords = data.get('keywords', '')
                                if keywords:
                                    if isinstance(keywords, str):
                                        keyword_list = [k.strip() for k in keywords.split(',') if k.strip()]
                                    elif isinstance(keywords, list):
                                        keyword_list = [str(k).strip() for k in keywords if k]
                                    else:
                                        keyword_list = []
                                    for kw in keyword_list:
                                        if kw and kw not in seen_tags and len(kw) < 50:
                                            tags.append(kw)
                                            seen_tags.add(kw)
                        except:
                            continue
            
            # 去重并限制数量
            if tags:
                # 再次去重（防止重复）
                unique_tags = []
                seen = set()
                for tag in tags:
                    if tag not in seen:
                        unique_tags.append(tag)
                        seen.add(tag)
                detail_info['tags'] = ', '.join(unique_tags[:20])  # 最多保留20个标签
            
        except Exception as e:
            print(f"解析电影详情页失败: {movie_url} - {str(e)}")
        
        return detail_info
    
    def parse_movie_item(self, item, fetch_detail: bool = False) -> Dict:
        """
        解析单个电影条目
        
        Args:
            item: BeautifulSoup电影条目对象
            fetch_detail: 是否爬取详情页获取完整信息
            
        Returns:
            电影信息字典
        """
        try:
            # 电影链接 - 尝试多种方式获取
            link = ''
            # 方式1: 通过class='nbg'的a标签
            link_elem = item.find('a', class_='nbg')
            if link_elem:
                link = link_elem.get('href', '')
            else:
                # 方式2: 通过div.pic下的a标签
                pic_elem = item.find('div', class_='pic')
                if pic_elem:
                    link_elem = pic_elem.find('a')
                    if link_elem:
                        link = link_elem.get('href', '')
                else:
                    # 方式3: 通过标题链接
                    title_elem = item.find('div', class_='hd')
                    if title_elem:
                        link_elem = title_elem.find('a')
                        if link_elem:
                            link = link_elem.get('href', '')
            
            # 如果是相对链接，转换为绝对链接
            if link and not link.startswith('http'):
                link = urljoin(self.base_url, link)
            
            # 电影海报（从列表页获取）
            img_elem = item.find('img')
            poster = ''
            if img_elem:
                poster = img_elem.get('src', '') or img_elem.get('data-src', '')
            
            # 电影标题
            title_elem = item.find('div', class_='hd')
            if title_elem:
                title_span = title_elem.find('span', class_='title')
                title = title_span.text.strip() if title_span else ''
                
            else:
                title = ''
            
            # 评分（转换为浮点数）
            rating_elem = item.find('span', class_='rating_num')
            if rating_elem:
                rating_str = rating_elem.text.strip()
                try:
                    rating = float(rating_str) if rating_str else 0.0
                except (ValueError, TypeError):
                    rating = 0.0
            else:
                rating = 0.0
            
            # 评价人数
            people_elem = item.find('div', class_='star')
            total_ratings = 0
            if people_elem:
                people_span = people_elem.find_all('span')
                if len(people_span) > 0:
                    people_text = people_span[-1].text.strip()
                    # 提取数字部分（例如："3225399人评价" -> 3225399），保存为整数
                    numbers = re.findall(r'\d+', people_text.replace(',', '').replace('，', ''))
                    if numbers:
                        try:
                            total_ratings = int(numbers[0])  # 保存为整数
                        except ValueError:
                            total_ratings = 0
                    else:
                        total_ratings = 0
            
            # 电影信息（导演、主演、年份、国家、类型）
            info_elem = item.find('div', class_='bd')
            info = ''
            directors = ''
            actors = ''
            release_date = ''
            
            if info_elem:
                info_p = info_elem.find('p', class_='')
                if info_p:
                    # 获取完整info文本
                    info = info_p.get_text(separator=' / ', strip=True)
                    
                    # 解析导演、主演等信息
                    # 格式通常是: 导演: 弗兰克·德拉邦特 / 主演: 蒂姆·罗宾斯 / 1994 / 美国 / 犯罪 剧情
                    parts = info.split('/')
                    for part in parts:
                        part = part.strip()
                        if '导演' in part:
                            # 提取导演名字（可能多个，用逗号或空格分隔）
                            director_text = part.replace('导演:', '').replace('导演', '').strip()
                            directors = director_text
                        elif '主演' in part:
                            # 提取主演名字
                            actor_text = part.replace('主演:', '').replace('主演', '').strip()
                            actors = actor_text
                        elif len(part) == 4 and part.isdigit():
                            # 可能是年份
                            if int(part) >= 1900 and int(part) <= 2100:
                                release_date = part
            
            movie_info = {
                'title': title,
                'link': link,
                'poster': poster,
                'rating': rating,
                'total_ratings': total_ratings,
                'directors': directors,
                'actors': actors,
                'release_date': release_date,
                'summary': ''  # 将在详情页获取
            }
            
            # 使用统一方法处理电影（获取详情、标准化）
            movie_info = self._process_movie_with_detail(movie_info, fetch_detail=fetch_detail)
            
            return movie_info
        except Exception as e:
            print(f"解析电影条目失败: {str(e)}")
            return None
    
    def crawl_top250(self, max_pages: int = 10, fetch_detail: bool = True, 
                     save_immediately: bool = True, json_filename: str = 'data/douban_top250.json',
                     jsonl_filename: str = 'data/douban_top250.jsonl',
                     csv_filename: str = 'data/douban_top250.csv',
                     existing_movies: set = None) -> List[Dict]:
        """
        爬取豆瓣电影Top250
        
        Args:
            max_pages: 最大爬取页数（每页25部电影，共10页）
            fetch_detail: 是否获取详细信息（会访问每个电影的详情页，速度较慢）
            save_immediately: 是否立即保存每个电影（实时写入文件）
            json_filename: JSON文件名
            jsonl_filename: JSONL文件名（逐行格式，追加模式）
            csv_filename: CSV文件名
            existing_movies: 已存在的电影集合（用于去重，存储电影的link或title）
            
        Returns:
            电影信息列表
        """
        movies = []
        base_url = "https://movie.douban.com/top250"
        
        # 初始化去重集合
        if existing_movies is None:
            existing_movies = set()
        
        # 如果启用实时保存，先删除旧文件（如果是新开始）
        if save_immediately:
            base_dir = os.path.dirname(__file__)
            # 删除JSONL和CSV文件（JSON需要保留用于最后保存完整列表）
            if jsonl_filename and os.path.exists(os.path.join(base_dir, jsonl_filename)):
                os.remove(os.path.join(base_dir, jsonl_filename))
            if csv_filename and os.path.exists(os.path.join(base_dir, csv_filename)):
                os.remove(os.path.join(base_dir, csv_filename))
        
        for page in range(max_pages):
            start = page * 25
            url = f"{base_url}?start={start}&filter="
            
            print(f"第 {page + 1} 页: {url}")
            
            try:
                soup = self.get_page(url)
                if not soup:
                    continue
                
                # 查找所有电影条目
                items = soup.find_all('div', class_='item')
                
                if not items:
                    break
                
                page_count = 0
                for item in items:
                    movie = self.parse_movie_item(item, fetch_detail=fetch_detail)
                    if movie:
                        # 去重检查：使用link作为唯一标识，如果没有link则使用title
                        movie_key = movie.get('link', '').strip() if movie.get('link') else movie.get('title', '').strip()
                        if movie_key and movie_key not in existing_movies:
                            existing_movies.add(movie_key)
                            
                            # 使用统一方法处理并保存
                            movie = self._process_movie_with_detail(
                                movie,
                                fetch_detail=False,  # 已在parse_movie_item中处理
                                save_immediately=save_immediately,
                                jsonl_filename=jsonl_filename,
                                json_filename=json_filename,
                                csv_filename=csv_filename
                            )
                            
                            movies.append(movie)
                            page_count += 1
                            print(f"  [{page_count}/{len(items)}] {movie.get('title', '未知')}")
                
                print(f"第 {page + 1} 页完成，获取 {page_count} 部电影\n")
                
            except Exception as e:
                print(f"第 {page + 1} 页出错: {str(e)}\n")
                continue
        
        return movies
    
    def parse_generic_movie_item(self, item) -> Dict:
        """
        通用电影条目解析方法（用于标签页等不同格式）
        
        Args:
            item: BeautifulSoup电影条目对象
            
        Returns:
            电影信息字典
        """
        try:
            movie = {}
            
            # 方法1: 查找a标签中的链接和标题
            link_elem = item.find('a')
            if link_elem and link_elem.get('href'):
                movie['link'] = link_elem.get('href', '')
                # 标题可能在a标签的文本、title属性或img的alt
                if link_elem.text.strip():
                    movie['title'] = link_elem.text.strip()
                elif link_elem.get('title'):
                    movie['title'] = link_elem.get('title')
            
            # 方法2: 查找img标签的alt作为标题
            img_elem = item.find('img')
            if img_elem and img_elem.get('alt'):
                if 'title' not in movie:
                    movie['title'] = img_elem.get('alt', '')
            
            # 方法3: 查找span.title
            title_elem = item.find('span', class_='title') or item.find('span', class_='pl2')
            if title_elem:
                title_link = title_elem.find('a')
                if title_link:
                    movie['title'] = title_link.get_text(strip=True)
                    movie['link'] = title_link.get('href', movie.get('link', ''))
            
            # 查找评分（转换为浮点数）
            rating_elem = (item.find('span', class_='rating_nums') or 
                          item.find('span', class_='rating_num') or
                          item.find('span', class_='rating'))
            if rating_elem:
                rating_str = rating_elem.get_text(strip=True)
                try:
                    movie['rating'] = float(rating_str) if rating_str else 0.0
                except (ValueError, TypeError):
                    movie['rating'] = 0.0
            else:
                movie['rating'] = 0.0
            
            # 查找评价人数
            people_elem = item.find('span', string=lambda text: text and '人评价' in text)
            if people_elem:
                people_text = people_elem.get_text(strip=True)
                # 提取数字部分，保存为整数类型（JSON中不带引号）
                numbers = re.findall(r'\d+', people_text.replace(',', '').replace('，', ''))
                if numbers:
                    try:
                        movie['total_ratings'] = int(numbers[0])  # 保存为整数
                    except ValueError:
                        movie['total_ratings'] = 0
                else:
                    movie['total_ratings'] = 0
            else:
                movie['total_ratings'] = 0
            
            # 不再保存 info 字段
            
            return movie if movie.get('title') else None
            
        except Exception as e:
            return None
    
    def crawl_classic_movies(self, max_pages: int = 20, existing_movies: set = None) -> List[Dict]:
        """
        爬取经典电影
        
        Args:
            max_pages: 最大爬取页数
            existing_movies: 已存在的电影集合（用于去重）
            
        Returns:
            电影信息列表
        """
        movies = []
        
        # 初始化去重集合
        if existing_movies is None:
            existing_movies = set()
        
        base_url = "https://movie.douban.com/explore"
        print("爬取经典电影...\n")
        
        try:
            print(f"正在爬取经典电影: {base_url}")
            soup = self.get_page(base_url)
            
            if not soup:
                print("⚠️  无法获取页面\n")
                return movies
            
            # 查找电影条目
            items = self._find_movie_items(soup, "页面")
            
            if not items:
                print("⚠️  未找到电影条目\n")
                return movies
            
            # 解析电影数据
            page_count = 0
            for item in items:
                # 先尝试Top250格式
                movie = self.parse_movie_item(item)
                if not movie:
                    # 再尝试通用格式
                    movie = self.parse_generic_movie_item(item)
                
                if movie:
                    # 去重检查：使用link作为唯一标识，如果没有link则使用title
                    movie_key = movie.get('link', '').strip() if movie.get('link') else movie.get('title', '').strip()
                    if movie_key and movie_key not in existing_movies:
                        existing_movies.add(movie_key)
                        movie['category'] = '经典电影'
                        movies.append(movie)
                        page_count += 1
                        print(f"  已获取: {movie.get('title', '未知')} - {movie.get('rating', '无评分')}")
                    else:
                        print(f"  ⚠️  跳过重复电影: {movie.get('title', '未知')}")
            
            print(f"获取了 {page_count} 部经典电影\n")
            
        except Exception as e:
            print(f"爬取失败: {str(e)}\n")
        
        return movies
    
    
    def _find_movie_items(self, soup, page_type: str = ""):
        """通用的查找电影条目的方法"""
        items = []
        selector_attempts = [
            lambda: soup.find_all('tr', class_='item'),
            lambda: soup.find_all('div', class_='item'),
            lambda: soup.find_all('li', class_='clearfix'),
            lambda: soup.select('div[class*="item"]'),
            lambda: soup.select('li[class*="item"]'),
            # 通过查找包含/subject/链接的元素来定位
            lambda: list(set([a.find_parent(['div', 'li', 'tr']) for a in soup.find_all('a', href=lambda h: h and '/subject/' in str(h) if h else False) if a.find_parent(['div', 'li', 'tr'])])),
        ]
        
        for i, selector_func in enumerate(selector_attempts):
            try:
                test_items = selector_func()
                test_items = [item for item in test_items if item is not None]
                if test_items and len(test_items) > 0:
                    items = test_items
                    if page_type:
                        print(f"    {page_type}：使用选择器 #{i+1} 找到 {len(items)} 个电影条目")
                    break
            except Exception as e:
                continue
        
        return items
    
    def _crawl_movies_from_api(self, category: str = None, category_name: str = '电影', 
                               max_pages: int = 500, fetch_detail: bool = True, 
                               save_immediately: bool = True, existing_movies: set = None, 
                               jsonl_filename: str = None, csv_filename: str = None, 
                               json_filename: str = None) -> List[Dict]:
        """
        通用方法：从API爬取电影列表
        使用API: https://m.douban.com/rexxar/api/v2/subject/recent_hot/movie
        
        Args:
            category: 分类参数（如'豆瓣高分'），None表示全部
            category_name: 类别显示名称
            max_pages: 最大爬取页数（每页20部电影）
            fetch_detail: 是否获取详细信息（详情页）
            save_immediately: 是否实时保存
            existing_movies: 已存在的电影集合（用于去重）
            jsonl_filename: JSONL文件名（实时保存）
            csv_filename: CSV文件名（实时保存）
            json_filename: JSON文件名（实时保存）
            
        Returns:
            电影信息列表
        """
        movies = []
        print(f"正在从API获取{category_name}...\n")
        
        # 初始化去重集合
        if existing_movies is None:
            existing_movies = set()
        
        # API端点
        from urllib.parse import quote
        api_base_url = "https://m.douban.com/rexxar/api/v2/subject/recent_hot/movie"
        type_param = quote('全部', safe='')
        limit = 20
        
        # 开始爬取
        try:
            for page in range(max_pages):
                start = page * limit
                if category:
                    category_param = quote(category, safe='')
                    api_url = f"{api_base_url}?start={start}&limit={limit}&category={category_param}&type={type_param}"
                else:
                    api_url = f"{api_base_url}?start={start}&limit={limit}&type={type_param}"
                
                print(f"第 {page + 1} 页")
                
                # 调用API
                try:
                    api_headers = {
                        'Referer': 'https://movie.douban.com/explore',
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    }
                    response = self.session.get(api_url, headers=api_headers, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    print(f"  获取API数据失败: {str(e)}")
                    if page == 0:
                        break
                    continue
                
                # 解析API返回的数据
                items = data.get('items', [])
                if not items:
                    if page == 0:
                        break
                    break
                
                page_count = 0
                for idx, item in enumerate(items):
                    try:
                        # 从API响应中提取基本信息
                        movie = {}
                        
                        # 构建电影链接（从uri或id）
                        movie_id = item.get('id', '')
                        uri = item.get('uri', '')
                        if uri and 'movie/' in uri:
                            movie_id = uri.split('movie/')[-1]
                        
                        if movie_id:
                            movie['link'] = f"{self.base_url}/subject/{movie_id}/"
                            try:
                                movie['movie_id'] = int(movie_id)
                            except (ValueError, TypeError):
                                continue
                        else:
                            continue
                        
                        movie['title'] = item.get('title', '未知')
                        
                        # 评分信息
                        rating_info = item.get('rating', {})
                        if rating_info:
                            movie['rating'] = float(rating_info.get('value', 0))
                            movie['total_ratings'] = int(rating_info.get('count', 0))
                        else:
                            movie['rating'] = 0.0
                            movie['total_ratings'] = 0
                        
                        # 海报
                        pic_info = item.get('pic', {})
                        if pic_info:
                            if isinstance(pic_info, dict):
                                movie['poster'] = pic_info.get('large', '') or pic_info.get('normal', '')
                            else:
                                movie['poster'] = pic_info
                        else:
                            movie['poster'] = ''
                        
                        # 去重
                        movie_key = movie.get('link', '').strip() if movie.get('link') else movie.get('title', '').strip()
                        if movie_key and movie_key not in existing_movies:
                            existing_movies.add(movie_key)
                            
                            # 使用统一方法处理电影
                            movie = self._process_movie_with_detail(
                                movie,
                                fetch_detail=fetch_detail,
                                save_immediately=save_immediately,
                                jsonl_filename=jsonl_filename,
                                json_filename=json_filename,
                                csv_filename=csv_filename
                            )
                            
                            movie['category'] = category_name
                            movies.append(movie)
                            page_count += 1
                            print(f"  [{page_count}/{len(items)}] {movie.get('title', '未知')}")
                            
                    except Exception as e:
                        print(f"  处理条目失败: {str(e)}")
                        continue
                
                print(f"第 {page + 1} 页完成，获取 {page_count} 部电影\n")
                
                if page_count == 0 and page > 0:
                    break
                
                time.sleep(random.uniform(1, 3))
                
        except Exception as e:
            print(f"从API爬取失败: {str(e)}")
        
        print(f"{category_name}爬取完成，共获取 {len(movies)} 部电影\n")
        return movies
    
    def crawl_high_rating_movies(self, max_pages: int = 500, fetch_detail: bool = True, 
                                  save_immediately: bool = True, existing_movies: set = None, 
                                  jsonl_filename: str = None, csv_filename: str = None, 
                                  json_filename: str = None) -> List[Dict]:
        """从API爬取高分电影"""
        return self._crawl_movies_from_api(
            category='豆瓣高分',
            category_name='高分电影',
            max_pages=max_pages,
            fetch_detail=fetch_detail,
            save_immediately=save_immediately,
            existing_movies=existing_movies,
            jsonl_filename=jsonl_filename,
            csv_filename=csv_filename,
            json_filename=json_filename
        )
    
    def _crawl_movies_by_region(self, region: str, category_name: str, max_pages: int = 500, fetch_detail: bool = True, save_immediately: bool = True, existing_movies: set = None, jsonl_filename: str = None, csv_filename: str = None, json_filename: str = None) -> List[Dict]:
        """
        通用方法：从API爬取指定地区的电影
        使用API: https://m.douban.com/rexxar/api/v2/movie/recommend
        
        Args:
            region: 地区名称（如"华语"、"欧美"、"日本"）
            category_name: 类别显示名称（如"华语电影"、"欧美电影"、"日本电影"）
            max_pages: 最大爬取页数（每页20部电影）
            fetch_detail: 是否获取详细信息（详情页）
            save_immediately: 是否实时保存
            existing_movies: 已存在的电影集合（用于去重）
            jsonl_filename: JSONL文件名（实时保存）
            csv_filename: CSV文件名（实时保存）
            json_filename: JSON文件名（实时保存）
            
        Returns:
            电影信息列表
        """
        movies = []
        print(f"正在从API获取全部{category_name}...\n")
        
        # 初始化去重集合
        if existing_movies is None:
            existing_movies = set()
        
        # API端点和参数
        from urllib.parse import quote
        import json
        
        api_base_url = "https://m.douban.com/rexxar/api/v2/movie/recommend"
        count = 20
        
        # selected_categories 是一个 JSON 字符串，需要 URL 编码
        selected_categories = {"地区": region}
        selected_categories_json = json.dumps(selected_categories, ensure_ascii=False)
        selected_categories_encoded = quote(selected_categories_json, safe='')
        
        # 开始爬取
        try:
            for page in range(max_pages):
                start = page * count
                
                # 构建API URL
                api_url = (
                    f"{api_base_url}?refresh=0&start={start}&count={count}"
                    f"&selected_categories={selected_categories_encoded}"
                    f"&uncollect=false&score_range=0,10&tags={quote(region, safe='')}"
                )
                
                print(f"第 {page + 1} 页")
                
                # 调用API
                try:
                    api_headers = {
                        'Referer': 'https://movie.douban.com/explore',
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Origin': 'https://movie.douban.com',
                    }
                    response = self.session.get(api_url, headers=api_headers, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    print(f"  获取API数据失败: {str(e)}")
                    if page == 0:
                        break
                    continue
                
                # 解析API返回的数据
                items = data.get('items', []) or data.get('subjects', []) or data.get('data', [])
                if not items:
                    if page == 0:
                        break
                    break
                
                page_count = 0
                for idx, item in enumerate(items):
                    try:
                        # 从API响应中提取基本信息
                        movie = {}
                        
                        # 构建电影链接（从uri或id）
                        movie_id = item.get('id', '')
                        uri = item.get('uri', '')
                        if uri and 'movie/' in uri:
                            movie_id = uri.split('movie/')[-1]
                        elif uri and '/subject/' in uri:
                            movie_id = uri.split('/subject/')[-1].rstrip('/')
                        
                        if movie_id:
                            movie['link'] = f"{self.base_url}/subject/{movie_id}/"
                            try:
                                movie['movie_id'] = int(movie_id)
                            except (ValueError, TypeError):
                                continue
                        else:
                            continue
                        
                        movie['title'] = item.get('title', '未知')
                        
                        # 评分信息
                        rating_info = item.get('rating', {})
                        if rating_info:
                            if isinstance(rating_info, dict):
                                movie['rating'] = float(rating_info.get('value', 0))
                                movie['total_ratings'] = int(rating_info.get('count', 0))
                            else:
                                movie['rating'] = float(rating_info)
                                movie['total_ratings'] = 0
                        else:
                            movie['rating'] = 0.0
                            movie['total_ratings'] = 0
                        
                        # 海报
                        pic_info = item.get('pic', {})
                        if pic_info:
                            if isinstance(pic_info, dict):
                                movie['poster'] = pic_info.get('large', '') or pic_info.get('normal', '') or pic_info.get('url', '')
                            else:
                                movie['poster'] = pic_info
                        else:
                            movie['poster'] = ''
                        
                        # 去重
                        movie_key = movie.get('link', '').strip() if movie.get('link') else movie.get('title', '').strip()
                        if movie_key and movie_key not in existing_movies:
                            existing_movies.add(movie_key)
                            
                            # 使用统一方法处理电影
                            movie = self._process_movie_with_detail(
                                movie,
                                fetch_detail=fetch_detail,
                                save_immediately=save_immediately,
                                jsonl_filename=jsonl_filename,
                                json_filename=json_filename,
                                csv_filename=csv_filename
                            )
                            
                            movie['category'] = category_name
                            movies.append(movie)
                            page_count += 1
                            print(f"  [{page_count}/{len(items)}] {movie.get('title', '未知')}")
                    
                    except Exception as e:
                        print(f"  处理条目失败: {str(e)}")
                        continue
                
                print(f"第 {page + 1} 页完成，获取 {page_count} 部电影，累计 {len(movies)} 部\n")
                
                if page_count == 0:
                    break
                
                time.sleep(random.uniform(1, 3))
                
        except Exception as e:
            print(f"从API爬取失败: {str(e)}")
            import traceback
            traceback.print_exc()
        
        print(f"\n{category_name}爬取完成，共获取 {len(movies)} 部电影\n")
        return movies
    
    def crawl_chinese_movies(self, max_pages: int = 500, fetch_detail: bool = True, save_immediately: bool = True, existing_movies: set = None, jsonl_filename: str = None, csv_filename: str = None, json_filename: str = None) -> List[Dict]:
        """
        从API爬取全部华语电影
        使用API: https://m.douban.com/rexxar/api/v2/movie/recommend
        """
        return self._crawl_movies_by_region(
            region='华语',
            category_name='华语电影',
            max_pages=max_pages,
            fetch_detail=fetch_detail,
            save_immediately=save_immediately,
            existing_movies=existing_movies,
            jsonl_filename=jsonl_filename,
            csv_filename=csv_filename,
            json_filename=json_filename
        )
    
    def crawl_western_movies(self, max_pages: int = 500, fetch_detail: bool = True, save_immediately: bool = True, existing_movies: set = None, jsonl_filename: str = None, csv_filename: str = None, json_filename: str = None) -> List[Dict]:
        """
        从API爬取全部欧美电影
        使用API: https://m.douban.com/rexxar/api/v2/movie/recommend
        """
        return self._crawl_movies_by_region(
            region='欧美',
            category_name='欧美电影',
            max_pages=max_pages,
            fetch_detail=fetch_detail,
            save_immediately=save_immediately,
            existing_movies=existing_movies,
            jsonl_filename=jsonl_filename,
            csv_filename=csv_filename,
            json_filename=json_filename
        )
    
    def crawl_japanese_movies(self, max_pages: int = 500, fetch_detail: bool = True, save_immediately: bool = True, existing_movies: set = None, jsonl_filename: str = None, csv_filename: str = None, json_filename: str = None) -> List[Dict]:
        """
        从API爬取全部日本电影
        使用API: https://m.douban.com/rexxar/api/v2/movie/recommend
        """
        return self._crawl_movies_by_region(
            region='日本',
            category_name='日本电影',
            max_pages=max_pages,
            fetch_detail=fetch_detail,
            save_immediately=save_immediately,
            existing_movies=existing_movies,
            jsonl_filename=jsonl_filename,
            csv_filename=csv_filename,
            json_filename=json_filename
        )
    
    def crawl_hongkong_movies(self, max_pages: int = 500, fetch_detail: bool = True, save_immediately: bool = True, existing_movies: set = None, jsonl_filename: str = None, csv_filename: str = None, json_filename: str = None) -> List[Dict]:
        """
        从API爬取全部香港电影
        使用API: https://m.douban.com/rexxar/api/v2/movie/recommend
        """
        return self._crawl_movies_by_region(
            region='中国香港',
            category_name='香港电影',
            max_pages=max_pages,
            fetch_detail=fetch_detail,
            save_immediately=save_immediately,
            existing_movies=existing_movies,
            jsonl_filename=jsonl_filename,
            csv_filename=csv_filename,
            json_filename=json_filename
        )
    
    
    def save_movie_line(self, movie: Dict, json_filename: str = None, csv_filename: str = None, jsonl_filename: str = None):
        """
        逐行保存单个电影数据到文件（实时写入）
        
        Args:
            movie: 单个电影数据字典
            json_filename: JSON文件名（追加模式，但会构建完整列表，性能较低）
            csv_filename: CSV文件名（追加模式）
            jsonl_filename: JSONL文件名（追加模式，推荐使用）
        """
        # 标准化数据（排序字段、限制人名字段）
        movie = self.normalize_movie_data(movie)
        
        base_dir = os.path.dirname(__file__)
        
        # 保存为JSONL格式（每行一个JSON对象，追加模式，推荐）
        if jsonl_filename:
            jsonl_filepath = os.path.join(base_dir, jsonl_filename)
            # 确保目录存在
            jsonl_dir = os.path.dirname(jsonl_filepath)
            if jsonl_dir and not os.path.exists(jsonl_dir):
                os.makedirs(jsonl_dir, exist_ok=True)
            with open(jsonl_filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(movie, ensure_ascii=False) + '\n')
        
        # 保存为JSON格式（追加到JSON数组，实时保存）
        if json_filename:
            json_filepath = os.path.join(base_dir, json_filename)
            # 确保目录存在
            json_dir = os.path.dirname(json_filepath)
            if json_dir and not os.path.exists(json_dir):
                os.makedirs(json_dir, exist_ok=True)
            
            file_exists = os.path.exists(json_filepath)
            
            if file_exists:
                # 读取现有JSON文件
                try:
                    with open(json_filepath, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        if isinstance(existing_data, list):
                            existing_data.append(movie)
                            with open(json_filepath, 'w', encoding='utf-8') as f:
                                json.dump(existing_data, f, ensure_ascii=False, indent=2, sort_keys=False)
                        else:
                            # 如果不是列表，转换为列表
                            with open(json_filepath, 'w', encoding='utf-8') as f:
                                json.dump([existing_data, movie], f, ensure_ascii=False, indent=2, sort_keys=False)
                except (json.JSONDecodeError, ValueError):
                    # 如果文件格式错误，重新创建
                    with open(json_filepath, 'w', encoding='utf-8') as f:
                        json.dump([movie], f, ensure_ascii=False, indent=2, sort_keys=False)
            else:
                # 新文件，创建JSON数组
                with open(json_filepath, 'w', encoding='utf-8') as f:
                    json.dump([movie], f, ensure_ascii=False, indent=2, sort_keys=False)
        
        # 保存为CSV格式（追加模式，按照定义的字段顺序）
        if csv_filename:
            csv_filepath = os.path.join(base_dir, csv_filename)
            # 确保目录存在
            csv_dir = os.path.dirname(csv_filepath)
            if csv_dir and not os.path.exists(csv_dir):
                os.makedirs(csv_dir, exist_ok=True)
            file_exists = os.path.exists(csv_filepath)
            
            # 读取已有文件的表头（如果存在）
            existing_fieldnames = []
            if file_exists:
                try:
                    with open(csv_filepath, 'r', encoding='utf-8-sig', newline='') as f:
                        reader = csv.reader(f)
                        existing_fieldnames = next(reader, [])
                except:
                    existing_fieldnames = []
            
            # 如果没有已有字段，使用定义的字段顺序
            if not existing_fieldnames:
                fieldnames = []
                for field in self.FIELD_ORDER:
                    if field in movie:
                        fieldnames.append(field)
                # 添加其他未定义的字段
                for key in movie.keys():
                    if key not in self.FIELD_ORDER:
                        fieldnames.append(key)
            else:
                # 使用已有字段顺序
                fieldnames = existing_fieldnames
            
            # 确保movie中包含所有字段（缺失的用空值填充）
            row = {field: movie.get(field, '') for field in fieldnames}
            
            with open(csv_filepath, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                # 如果是新文件，写入表头
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)
    
    def save_to_json(self, data: List[Dict], filename: str = 'douban_movies.json'):
        """
        保存数据为JSON格式（数组格式，格式化输出）
        
        Args:
            data: 电影数据列表
            filename: 保存的文件名
        """
        # 标准化所有数据
        normalized_data = [self.normalize_movie_data(movie) for movie in data]
        
        filepath = os.path.join(os.path.dirname(__file__), filename)
        # 确保目录存在
        file_dir = os.path.dirname(filepath)
        if file_dir and not os.path.exists(file_dir):
            os.makedirs(file_dir, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            # 保存为格式化的JSON数组，使用2个空格缩进，确保中文字符正确显示
            json.dump(normalized_data, f, ensure_ascii=False, indent=2, sort_keys=False)
        print(f"数据已保存为JSON（格式化数组）: {filepath}")
    
    def save_to_csv(self, data: List[Dict], filename: str = 'douban_movies.csv', append: bool = False):
        """
        保存数据为CSV格式
        
        Args:
            data: 电影数据列表
            filename: 保存的文件名
            append: 是否追加模式（True=追加，False=覆盖）
        """
        if not data:
            print("没有数据可保存")
            return
        
        # 标准化所有数据
        normalized_data = [self.normalize_movie_data(movie) for movie in data]
        
        filepath = os.path.join(os.path.dirname(__file__), filename)
        
        # 使用定义的字段顺序
        fieldnames = []
        for field in self.FIELD_ORDER:
            if any(field in item for item in normalized_data):
                fieldnames.append(field)
        # 添加其他未定义的字段
        all_fields = set()
        for item in normalized_data:
            all_fields.update(item.keys())
        for field in all_fields:
            if field not in fieldnames:
                fieldnames.append(field)
        
        # 如果是追加模式且文件已存在，需要读取已有的字段顺序
        file_exists = os.path.exists(filepath) and append
        if file_exists:
            try:
                with open(filepath, 'r', encoding='utf-8-sig', newline='') as f:
                    reader = csv.reader(f)
                    existing_fieldnames = next(reader, [])
                    # 合并字段名，保持顺序
                    for field in existing_fieldnames:
                        if field not in fieldnames:
                            fieldnames.insert(len(existing_fieldnames) if field in existing_fieldnames else len(fieldnames), field)
            except:
                pass
        
        # 打开文件（追加或覆盖模式）
        mode = 'a' if append else 'w'
        with open(filepath, mode, encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            # 只有在非追加模式或者文件不存在时才写入表头
            if not append or not file_exists:
                writer.writeheader()
            writer.writerows(normalized_data)
        
        mode_str = "追加到" if append else "保存为"
        print(f"数据已{mode_str}CSV: {filepath}")


def main():
    """主函数"""
    spider = DoubanMovieSpider()
    
    print("=" * 60)
    print("豆瓣电影爬虫")
    print("=" * 60)
    print()
    
    # 选择爬取类型
    print("请选择爬取类型:")
    print("1. 豆瓣电影 Top250")
    print("2. 豆瓣高分电影（480 条记录）")
    print("3. 全部华语电影（500 条记录）")
    print("4. 全部欧美电影（500 条记录）")
    print("5. 全部日本电影（500 条记录）")
    print("6. 全部香港电影（500 条记录）")
    print("7. 爬取上述所有电影，并去重")
    
    choice = input("请输入选择 (1/2/3/4/5/6/7，默认1): ").strip() or "1" 
    
    # 用于去重的集合，在所有爬取类型间共享
    existing_movies = set()
    
    if choice == "1" or choice == "7":
        print("\n开始爬取豆瓣电影Top250...")
        top250_movies = spider.crawl_top250(
            max_pages=10,
            save_immediately=True,
            existing_movies=existing_movies,
            jsonl_filename='data/douban_top250.jsonl',
            csv_filename='data/douban_top250.csv'
        )
        if top250_movies:
            # 同时保存完整的JSON文件（用于最终结果）
            spider.save_to_json(top250_movies, 'data/douban_top250.json')
            print(f"\n✅ Top250爬取完成，共获取 {len(top250_movies)} 部电影")
            print(f"   文件已保存：")
            print(f"   - data/douban_top250.json (完整JSON)")
            print(f"   - data/douban_top250.jsonl (逐行JSON，已实时保存)")
            print(f"   - data/douban_top250.csv (CSV格式，已实时保存)")
    
    if choice == "2" or choice == "7":
        print("\n开始爬取高分电影...")
        high_rating_movies = spider.crawl_high_rating_movies(
            max_pages=500,  # 爬取500页豆瓣高分电影
            save_immediately=True,
            existing_movies=existing_movies,
            jsonl_filename='data/douban_high_rating.jsonl',
            csv_filename='data/douban_high_rating.csv',
            json_filename='data/douban_high_rating.json'
        )
        if high_rating_movies:
            # 同时保存完整的JSON文件（用于最终结果）
            spider.save_to_json(high_rating_movies, 'data/douban_high_rating.json')
            print(f"\n✅ 高分电影爬取完成，共获取 {len(high_rating_movies)} 部电影")
            print(f"   文件已保存：")
            print(f"   - data/douban_high_rating.json (完整JSON)")
            print(f"   - data/douban_high_rating.jsonl (逐行JSON，已实时保存)")
            print(f"   - data/douban_high_rating.csv (CSV格式，已实时保存)")
    
    if choice == "3" or choice == "7":
        print("\n开始爬取全部华语电影...")
        chinese_movies = spider.crawl_chinese_movies(
            max_pages=500,  # 爬取500页华语电影
            save_immediately=True,
            existing_movies=existing_movies,
            jsonl_filename='data/douban_chinese_movies.jsonl',
            csv_filename='data/douban_chinese_movies.csv',
            json_filename='data/douban_chinese_movies.json'
        )
        if chinese_movies:
            # 同时保存完整的JSON文件（用于最终结果）
            spider.save_to_json(chinese_movies, 'data/douban_chinese_movies.json')
            print(f"\n✅ 华语电影爬取完成，共获取 {len(chinese_movies)} 部电影")
            print(f"   文件已保存：")
            print(f"   - data/douban_chinese_movies.json (完整JSON)")
            print(f"   - data/douban_chinese_movies.jsonl (逐行JSON，已实时保存)")
            print(f"   - data/douban_chinese_movies.csv (CSV格式，已实时保存)")
            print(f"\n注意：category 和 rating_detail 字段已自动过滤")
    
    if choice == "4" or choice == "7":
        print("\n开始爬取全部欧美电影...")
        western_movies = spider.crawl_western_movies(
            max_pages=500,  # 爬取500页欧美电影
            save_immediately=True,
            existing_movies=existing_movies,
            jsonl_filename='data/douban_western_movies.jsonl',
            csv_filename='data/douban_western_movies.csv',
            json_filename='data/douban_western_movies.json'
        )
        if western_movies:
            # 同时保存完整的JSON文件（用于最终结果）
            spider.save_to_json(western_movies, 'data/douban_western_movies.json')
            print(f"\n✅ 欧美电影爬取完成，共获取 {len(western_movies)} 部电影")
            print(f"   文件已保存：")
            print(f"   - data/douban_western_movies.json (完整JSON)")
            print(f"   - data/douban_western_movies.jsonl (逐行JSON，已实时保存)")
            print(f"   - data/douban_western_movies.csv (CSV格式，已实时保存)")
            print(f"\n注意：category 和 rating_detail 字段已自动过滤")
    
    if choice == "5" or choice == "7":
        print("\n开始爬取全部日本电影...")
        japanese_movies = spider.crawl_japanese_movies(
            max_pages=500,  # 爬取500页日本电影
            save_immediately=True,
            existing_movies=existing_movies,
            jsonl_filename='data/douban_japanese_movies.jsonl',
            csv_filename='data/douban_japanese_movies.csv',
            json_filename='data/douban_japanese_movies.json'
        )
        if japanese_movies:
            # 同时保存完整的JSON文件（用于最终结果）
            spider.save_to_json(japanese_movies, 'data/douban_japanese_movies.json')
            print(f"\n✅ 日本电影爬取完成，共获取 {len(japanese_movies)} 部电影")
            print(f"   文件已保存：")
            print(f"   - data/douban_japanese_movies.json (完整JSON)")
            print(f"   - data/douban_japanese_movies.jsonl (逐行JSON，已实时保存)")
            print(f"   - data/douban_japanese_movies.csv (CSV格式，已实时保存)")
            print(f"\n注意：category 和 rating_detail 字段已自动过滤")
    
    if choice == "6" or choice == "7":
        print("\n开始爬取全部香港电影...")
        hongkong_movies = spider.crawl_hongkong_movies(
            max_pages=500,  # 爬取500页香港电影
            save_immediately=True,
            existing_movies=existing_movies,
            jsonl_filename='data/douban_hongkong_movies.jsonl',
            csv_filename='data/douban_hongkong_movies.csv',
            json_filename='data/douban_hongkong_movies.json'
        )
        if hongkong_movies:
            # 同时保存完整的JSON文件（用于最终结果）
            spider.save_to_json(hongkong_movies, 'data/douban_hongkong_movies.json')
            print(f"\n✅ 香港电影爬取完成，共获取 {len(hongkong_movies)} 部电影")
            print(f"   文件已保存：")
            print(f"   - data/douban_hongkong_movies.json (完整JSON)")
            print(f"   - data/douban_hongkong_movies.jsonl (逐行JSON，已实时保存)")
            print(f"   - data/douban_hongkong_movies.csv (CSV格式，已实时保存)")
            print(f"\n注意：category 和 rating_detail 字段已自动过滤")
    
    print("\n爬取任务完成！")


if __name__ == "__main__":
    main()

