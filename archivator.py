import os
import shutil
import zipfile
import time
from datetime import datetime
from tqdm import tqdm

def create_archives(folder_path):
    subfolders = [f.path for f in os.scandir(folder_path) if f.is_dir()]
    archive_dir = os.path.join('D:\\', 'NextSRV_BIG_Arch')
    os.makedirs(archive_dir, exist_ok=True)

    for subfolder in subfolders:
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        archive_name = f"NextSRV_BIG_{formatted_datetime}.zip"

        # Определяем общее количество файлов для прогресс-бара
        total_files = sum([len(files) for _, _, files in os.walk(subfolder)])

        with zipfile.ZipFile(os.path.join(archive_dir, archive_name), 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            # Используем tqdm для создания статус-бара
            progress_bar = tqdm(total=total_files, unit='file(s)')

            for root, dirs, files in os.walk(subfolder):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, os.path.relpath(file_path, subfolder))

                    # Увеличиваем значение прогресс-бара после каждого добавленного файла
                    progress_bar.update(1)

            # Завершаем прогресс-бар
            progress_bar.close()

        print(f"Архив {archive_name} успешно создан и перемещен в директорию {archive_dir}.")

        # Удаляем папку после создания архива
        shutil.rmtree(subfolder)
        print(f"Папка {subfolder} успешно удалена.")

def check_and_transfer_archives():
    source_dir = os.path.join('D:\\', 'NextSRV_BIG_Arch')
    target_dir = os.path.join('D:\\', 'NightTransfer_BIG')
    os.makedirs(target_dir, exist_ok=True)

    while True:
        files = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]

        for file in files:
            if file.endswith('.zip'):
                src_path = os.path.join(source_dir, file)
                dst_path = os.path.join(target_dir, f"{file}.transfer")

                # Check if the archive is currently being written (i.e., not locked)
                if not os.path.exists(dst_path):
                    os.rename(src_path, dst_path)
                    new_dst_path = os.path.join(target_dir, f"{file}_{datetime.now().strftime('%H-%M-%S')}.zip.transfer")
                    shutil.move(dst_path, new_dst_path)
                    print(f"Архив {file} успешно перенесен в папку {target_dir}.")

        time.sleep(10)

if __name__ == '__main__':
    folder_path = r'D:\\NextSRV_BIG_tmp'
    create_archives(folder_path)
    check_and_transfer_archives()
