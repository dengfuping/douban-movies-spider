#!/usr/bin/env python3
"""
自动准备并上传 Kaggle 数据集
只复制 CSV 文件、dataset-metadata.json 和 DATASET.md 到 kaggle-upload 目录，然后上传并清理
"""

import os
import shutil
import sys
import argparse
import subprocess
import re
from pathlib import Path

def prepare_kaggle_upload():
    """准备 Kaggle 上传目录，只包含 CSV 文件和必要的元数据文件"""
    
    # 源目录和目标目录
    source_dir = Path("data")
    target_dir = Path("kaggle-upload")
    
    # 如果目标目录存在，先删除
    if target_dir.exists():
        print(f"删除现有目录: {target_dir}")
        shutil.rmtree(target_dir)
    
    # 创建目标目录
    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"创建上传目录: {target_dir}")
    
    # 要复制的文件列表
    files_to_copy = []
    
    # 1. 复制所有 CSV 文件
    csv_files = list(source_dir.glob("*.csv"))
    for csv_file in csv_files:
        files_to_copy.append(csv_file)
        print(f"  添加 CSV 文件: {csv_file.name}")
    
    # 2. 复制 dataset-metadata.json
    metadata_file = source_dir / "dataset-metadata.json"
    if metadata_file.exists():
        files_to_copy.append(metadata_file)
        print(f"  添加元数据文件: {metadata_file.name}")
    else:
        print(f"  警告: 未找到 {metadata_file}")
        return None
    
    # 3. 复制 DATASET.md
    dataset_md = source_dir / "DATASET.md"
    if dataset_md.exists():
        files_to_copy.append(dataset_md)
        print(f"  添加文档文件: {dataset_md.name}")
    else:
        print(f"  警告: 未找到 {dataset_md}")
    
    # 复制文件
    copied_count = 0
    for file_path in files_to_copy:
        try:
            shutil.copy2(file_path, target_dir)
            copied_count += 1
        except Exception as e:
            print(f"  错误: 复制 {file_path.name} 失败: {e}")
            return None
    
    print(f"\n✅ 准备完成！共复制 {copied_count} 个文件到 {target_dir}/")
    
    # 显示将要上传的文件列表
    print(f"\n将要上传的文件:")
    total_size = 0
    for file_path in sorted(target_dir.iterdir()):
        size = file_path.stat().st_size
        total_size += size
        size_mb = size / (1024 * 1024)
        print(f"  - {file_path.name} ({size_mb:.2f} MB)")
    
    total_size_mb = total_size / (1024 * 1024)
    print(f"\n总大小: {total_size_mb:.2f} MB")
    
    return target_dir

def check_kaggle_cli():
    """检查 Kaggle CLI 是否安装和配置"""
    try:
        result = subprocess.run(
            ["kaggle", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            print(f"✅ Kaggle CLI 已安装: {result.stdout.strip()}")
            return True
        else:
            print("❌ Kaggle CLI 未正确配置")
            return False
    except FileNotFoundError:
        print("❌ Kaggle CLI 未安装")
        print("   请运行: pip3 install kaggle")
        return False
    except subprocess.TimeoutExpired:
        print("❌ Kaggle CLI 检查超时")
        return False
    except Exception as e:
        print(f"❌ 检查 Kaggle CLI 时出错: {e}")
        return False

def upload_to_kaggle(target_dir, message=None):
    """上传数据集到 Kaggle，显示详细进度"""
    if not check_kaggle_cli():
        print("\n⚠️  无法上传: Kaggle CLI 未安装或未配置")
        print("   请先安装并配置 Kaggle CLI:")
        print("   1. pip3 install kaggle")
        print("   2. 配置 API token (参考: https://www.kaggle.com/docs/api)")
        return False
    
    # 显示上传文件详细信息
    print(f"\n{'='*60}")
    print(f"📦 准备上传的文件列表:")
    print(f"{'='*60}")
    
    file_list = []
    total_size = 0
    for file_path in sorted(target_dir.iterdir()):
        size = file_path.stat().st_size
        total_size += size
        size_mb = size / (1024 * 1024)
        file_list.append((file_path.name, size_mb))
        print(f"  📄 {file_path.name:50s} {size_mb:>8.2f} MB")
    
    total_size_mb = total_size / (1024 * 1024)
    print(f"{'='*60}")
    print(f"  总计: {len(file_list)} 个文件，总大小: {total_size_mb:.2f} MB")
    print(f"{'='*60}")
    
    print(f"\n🚀 开始上传到 Kaggle...")
    print(f"   上传目录: {target_dir}")
    
    # 根据是否有 -m 参数决定操作模式
    if message:
        print(f"   模式: 更新数据集")
        print(f"   版本说明: {message}")
    else:
        print(f"   模式: 创建新数据集")
    print(f"   状态: 正在上传...\n")
    
    try:
        # 切换到上传目录
        original_cwd = os.getcwd()
        os.chdir(target_dir)
        
        # 根据是否有 message 参数决定使用 create 还是 version
        if message:
            # 带 -m 参数：更新现有数据集
            upload_cmd = ["kaggle", "datasets", "version", "-m", message, "-p", "."]
        else:
            # 不带 -m 参数：创建新数据集
            upload_cmd = ["kaggle", "datasets", "create", "-p", "."]
        
        # 执行上传命令，实时输出进度
        process = subprocess.Popen(
            upload_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # 行缓冲，实时输出
            universal_newlines=True
        )
        
        # 实时输出进度信息
        output_lines = []
        print("上传进度:")
        print("-" * 60)
        
        # 实时读取并输出进度信息
        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            line = line.rstrip()
            if line:
                print(f"  {line}")
                output_lines.append(line)
                sys.stdout.flush()
        
        # 确保所有输出都被读取
        process.stdout.close()
        
        # 等待进程完成
        process.wait()
        
        output = "\n".join(output_lines)
        
        if process.returncode == 0:
            print("-" * 60)
            if message:
                print("\n✅ 数据集更新成功！")
            else:
                print("\n✅ 数据集创建成功！")
            
            # 解析输出，提取数据集 URL
            for line in output_lines:
                if "kaggle.com/datasets" in line and "http" in line:
                    print(f"   数据集地址: {line.strip()}")
                    break
            
            # 返回原始目录
            os.chdir(original_cwd)
            return True
        else:
            # 如果失败，显示错误信息
            print("-" * 60)
            if message:
                print("\n❌ 数据集更新失败")
                print("\n💡 提示: 如果数据集不存在，请先不带 -m 参数创建数据集")
            else:
                print("\n❌ 数据集创建失败")
                error_output = output.lower()
                if "already exists" in error_output or "already" in error_output:
                    print("\n💡 提示: 数据集已存在，请使用 -m 参数更新数据集:")
                    print("   python3 kaggle_upload.py -m \"版本说明\"")
            
            print("\n错误信息:")
            print(output)
            # 返回原始目录
            os.chdir(original_cwd)
            return False
            
    except KeyboardInterrupt:
        print("\n\n⚠️  上传被用户中断")
        os.chdir(original_cwd)
        return False
    except Exception as e:
        print(f"\n❌ 上传过程中出错: {e}")
        # 确保返回原始目录
        try:
            os.chdir(original_cwd)
        except:
            pass
        return False

def clean_upload_dir(target_dir):
    """清理上传临时目录"""
    if not target_dir.exists():
        print(f"⚠️  目录 {target_dir} 不存在，无需清理")
        return
    
    print(f"\n🧹 清理临时目录: {target_dir}")
    
    try:
        shutil.rmtree(target_dir)
        print(f"✅ 已删除临时目录: {target_dir}")
    except Exception as e:
        print(f"❌ 删除失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="自动准备并上传 Kaggle 数据集",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 创建新数据集（不带 -m 参数）
  python3 kaggle_upload.py

  # 更新现有数据集（带 -m 参数）
  python3 kaggle_upload.py -m "更新数据集，新增 2024 年数据"

  # 或者使用长参数
  python3 kaggle_upload.py --message "修复数据格式问题"

注意：
  - 不带 -m: 创建新数据集
  - 带 -m: 更新现有数据集（必须提供版本说明）
        """
    )
    parser.add_argument(
        "-m", "--message",
        type=str,
        help="版本说明（更新数据集时必需）",
        metavar="MESSAGE"
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("Kaggle 数据集上传工具")
    print("="*60)
    
    # 1. 准备上传文件
    target_dir = prepare_kaggle_upload()
    if not target_dir:
        print("\n❌ 准备上传文件失败，退出")
        sys.exit(1)
    
    # 2. 上传到 Kaggle
    success = upload_to_kaggle(target_dir, message=args.message)
    
    # 3. 清理临时目录
    if success:
        print(f"\n{'='*60}")
        clean_upload_dir(target_dir)
        print(f"\n✅ 所有操作完成！")
    else:
        print(f"\n{'='*60}")
        print(f"⚠️  上传失败，临时目录保留在 {target_dir}")
        print(f"   可以手动检查或清理该目录")
        sys.exit(1)

