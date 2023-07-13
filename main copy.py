import os
import shutil
import zipfile
from datetime import datetime
import time
import subprocess
from multiprocessing.pool import ThreadPool
from tqdm import tqdm

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
                shutil.rmtree(archives_path)
                print(f"Удалена папка: {archives_path}")
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

    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_STORED) as zipf:
        with tqdm(total=total_files, unit='файл', desc='Архивирование', leave=True, ncols=80) as pbar:
            for root, _, files in os.walk(source):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, source)
                    zipf.write(file_path, arcname)
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

    os.makedirs(destination, exist_ok=True)  # Создать папку, если она не существует

    total_files = sum(len(files) for _, _, files in os.walk(source))

    with ThreadPool() as pool:
        futures = []

        for i in range(archive_count):
            archive_name = f'NextSRV_Arch_{datetime.now().strftime("%Y%m%d_%H%M%S")}_partition_{i+1}.zip'
            archive_path = os.path.join(destination, archive_name)

            start_index = i * chunk_size
            end_index = start_index + chunk_size

            chunk_folders = folders[start_index:end_index]
            chunk_path = os.path.join(source, chunk_folders[0])

            future = pool.apply_async(process_chunk, (chunk_folders, source, archive_path, total_files))
            futures.append(future)

        if remainder > 0:
            archive_name = f'NextSRV_Arch_{datetime.now().strftime("%Y%m%d_%H%M%S")}_partition_{archive_count+1}.zip'
            archive_path = os.path.join(destination, archive_name)
            remainder_folders = folders[archive_count*chunk_size:]

            future = pool.apply_async(process_chunk, (remainder_folders, source, archive_path, total_files))
            futures.append(future)

        for future in futures:
            processed_files = future.get()
            progress = processed_files / total_files * 100 if total_files > 0 else 100
            print(f"Архив {future} завершен: {processed_files}/{total_files} файлов ({progress:.2f}%)")

    print("Архивирование завершено")

print("Archlenix v_1.0")
time.sleep(2)
print("Started")
while True:
    folder_path = 'D:\\NextSRV'
    folder_tmp_path = 'D:\\NextSRV_tmp'
    archives_path = 'D:\\NextSRV_Arch'
    transfer_path = 'D:\\NightTransfer'
    chunk_size = 1000

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
            # Проверка наличия папки NextSRV_Arch и ее создание при отсутствии
            if not os.path.exists(archives_path):
                os.makedirs(archives_path)
            archive_name = f'NextSRV_Arch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            archive_path = os.path.join(archives_path, archive_name)
            create_archive(folder_tmp_path, archive_path)
            print(f"Создан архив: {archive_path}")

        shutil.rmtree(folder_tmp_path)  # Remove the NextSRV_tmp folder

    check_and_transfer_archives(archives_path, transfer_path)
    time.sleep(10)  # Wait for 10 seconds before checking again
