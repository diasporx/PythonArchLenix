import os
import shutil
import zipfile
import time
from tqdm import tqdm
from datetime import datetime

source_folder = "D:\\PrevNightTransfer"
destination_folder = "D:\\PrevSRV"
destination_folder_tmp = "D:\\PrevSRV_tmp"
max_directories = 5000

while True:
    directory_count = len(os.listdir(destination_folder))

    if directory_count < max_directories:
        os.makedirs(destination_folder_tmp, exist_ok=True)

        files_to_process = [file_name for file_name in os.listdir(source_folder) if file_name.endswith(".zip")]

        if not files_to_process:
            print("Нет доступных архивов для распаковки.", datetime.now().strftime("%A %d %B %Y %H:%M:%S"))
            time.sleep(5)  # Добавлено ожидание перед следующей попыткой
            continue

        archive_found = False  # Флаг для отслеживания наличия нормального архива
        total_archives = len(files_to_process)  # Общее количество архивов

        for file_name in files_to_process:
            try:
                with zipfile.ZipFile(os.path.join(source_folder, file_name), 'r') as zip_ref:
                    zip_ref.extractall(destination_folder_tmp)

                with tqdm(total=100, ncols=80, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]', unit='s', desc="Разархивация: {}".format(file_name)) as pbar:
                    shutil.copytree(destination_folder_tmp, destination_folder, dirs_exist_ok=True, ignore=shutil.ignore_patterns('*.zip'))
                    for i in range(100):
                        pbar.update(1)
                        time.sleep(0.01)  # Добавлено небольшое задержку, чтобы обновления статус-бара были заметны
                    pbar.set_postfix(осталось_времени="0 сек", скорость="100%")

                shutil.rmtree(destination_folder_tmp)
                print("Удалена временная папка", datetime.now().strftime("%A %d %B %Y %H:%M:%S"))
                os.remove(os.path.join(source_folder, file_name))
                print("Архив успешно разархивирован и скопирован.", datetime.now().strftime("%A %d %B %Y %H:%M:%S"))
                archive_found = True  # Изменяем флаг, если найден нормальный архив
                break
            except zipfile.BadZipFile:
                print("Архив '{}' поврежден. Попытка использовать следующий архив.".format(file_name), datetime.now().strftime("%A %d %B %Y %H:%M:%S"))
            except Exception as e:
                print("Ошибка при разархивации:", str(e), datetime.now().strftime("%A %d %B %Y %H:%M:%S"))

        if archive_found:
            time.sleep(1)  # Добавленоожидание перед следующей попыткой
            continue  # Возвращаемся к началу цикла

        print("Все доступные архивы повреждены.", datetime.now().strftime("%A %d %B %Y %H:%M:%S"))
        time.sleep(5) # Добавлено ожидание перед следующей попыткой

    else:
        print(len(os.listdir(destination_folder)), " > ", max_directories, datetime.now().strftime("%A %d %B %Y %H:%M:%S"))

    time.sleep(10)
