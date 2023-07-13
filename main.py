import os
import shutil
import zipfile
from datetime import datetime
import time
import datetime
import subprocess
from multiprocessing.pool import ThreadPool
from tqdm import tqdm

def get_formatted_datetime():
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%a %m.%d.%Y_%H.%M.%S.%f")[:-3]
    return current_datetime, formatted_datetime

def check_and_transfer_archives(archives_path, transfer_path):
    if not os.path.exists(archives_path):
        return

    archives = os.listdir(archives_path)
    archives.sort()

    os.makedirs(transfer_path, exist_ok=True)

    for archive in archives:
        archive_path = os.path.join(archives_path, archive)
        transfer_name = f"{archive}.transfer"
        transfer_file = os.path.join(transfer_path, transfer_name)

        try:
            if archive.endswith('.zip'):
                shutil.move(archive_path, transfer_file)
                print(f"Перемещен архив: {archive} -> {transfer_name}")
            else:
                print(f"Пропущен файл: {archive} (не является архивом)")

        except Exception as e:
            print(f"Ошибка при перемещении архива: {archive}\n{str(e)}")


def count_folders(path):
    if not os.path.exists(path):
        return -1  # Папка не существует
    elif not os.path.isdir(path):
        return -2  # Указанный путь не является папкой

    count = sum(1 for _ in os.scandir(path) if _.is_dir())
    return count


def rename_folder(path, new_name):
    if not os.path.exists(path):
        return -1  # Папка не существует

    new_path = os.path.join(os.path.dirname(path), new_name)
    try:
        os.rename(path, new_path)
    except PermissionError:
        print(f"Ошибка переименования папки: {path} -> {new_path}")
    return new_path


def create_archive(source, destination, total_files=0):
    processed_files = 0

    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED, compresslevel=1) as zipf:
        with tqdm(total=total_files, unit='файл', desc='Архивирование', leave=True, ncols=80) as pbar:
            for root, _, files in os.walk(source):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, source)

                    timestamp = time.mktime((1999, 1, 1, 0, 0, 0, 0, 0, 0))
                    zip_info = zipfile.ZipInfo(arcname, date_time=time.localtime(timestamp)[:6])
                    zipf.writestr(zip_info, open(file_path, 'rb').read())

                    processed_files += 1
                    pbar.update(1)

    return processed_files


def process_chunk(chunk_folders, source, destination, total_files):
    if len(chunk_folders) > 1:
        temp_dir = os.path.join(source, f'temp_{chunk_folders[0]}')
        os.mkdir(temp_dir)
        for folder in chunk_folders:
            folder_path = os.path.join(source, folder)
            shutil.move(folder_path, temp_dir)

        processed_files = create_archive(temp_dir, destination, total_files)
        shutil.rmtree(temp_dir)
    else:
        processed_files = create_archive(os.path.join(source, chunk_folders[0]), destination, total_files)

    return processed_files

def split_and_archive_folders(source, destination, chunk_size):
    if not os.path.exists(archives_path):
        os.makedirs(archives_path)
    folders = os.listdir(source)
    folders.sort()

    archive_count = len(folders) // chunk_size
    remainder = len(folders) % chunk_size

    os.makedirs(destination, exist_ok=True)

    total_files = sum(len(files) for _, _, files in os.walk(source))

    for i in range(archive_count):
        archive_name = f'NextSRV_Arch_{formatted_datetime}_partition_{i+1}.zip'
        archive_path = os.path.join(destination, archive_name)

        start_index = i * chunk_size
        end_index = start_index + chunk_size

        chunk_folders = folders[start_index:end_index]
        chunk_path = os.path.join(source, chunk_folders[0])

        processed_files = process_chunk(chunk_folders, source, archive_path, total_files)
        progress = processed_files / total_files * 100 if total_files > 0 else 100
        print(f"Archive {archive_name} completed: {processed_files}/{total_files} files ({progress:.2f}%)")

    if remainder > 0:
        archive_name = f'NextSRV_Arch_{formatted_datetime}_partition_{archive_count+1}.zip'
        archive_path = os.path.join(destination, archive_name)
        remainder_folders = folders[archive_count*chunk_size:]

        processed_files = process_chunk(remainder_folders, source, archive_path, total_files)
        progress = processed_files / total_files * 100 if total_files > 0 else 100
        print(f"Archive {archive_name} completed: {processed_files}/{total_files} files ({progress:.2f}%)")

    print("Archiving completed")


print("Archlenix v_2.1")
time.sleep(2)
print("Started")
while True:
    folder_path = 'D:\\NextSRV'
    folder_tmp_path = 'D:\\NextSRV_tmp'
    archives_path = 'D:\\NextSRV_Arch'
    transfer_path = 'D:\\NightTransfer'
    chunk_size = 1000

    current_datetime, formatted_datetime = get_formatted_datetime()

    folder_count = count_folders(folder_path)
    if folder_count > 1000:
        new_folder_name = 'NextSRV_tmp'
        renamed_path = rename_folder(folder_path, new_folder_name)
        print(f"Переименовано: {folder_path} -> {new_folder_name}")

        new_folder_path = 'D:\\NextSRV'
        if not os.path.exists(new_folder_path):
            os.mkdir(new_folder_path)
            print(f"Создана новая пустая папка: {new_folder_path}")

        tmp_folder_count = count_folders(folder_tmp_path)
        if tmp_folder_count > 1500:
            split_and_archive_folders(folder_tmp_path, archives_path, chunk_size)
        else:
            if not os.path.exists(archives_path):
                os.makedirs(archives_path)
            archive_name = f'NextSRV_arch_{formatted_datetime}.zip'
            archive_path = os.path.join(archives_path, archive_name)
            create_archive(folder_tmp_path, archive_path)
            print(f"Создан архив: {archive_path}")

        shutil.rmtree(folder_tmp_path)

    check_and_transfer_archives(archives_path, transfer_path)
    time.sleep(10)
