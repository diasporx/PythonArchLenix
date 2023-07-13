import os
import zipfile

def create_zip_archive(folder_path, target_size):
    # Получение списка файлов и папок в указанной папке
    files = []
    for root, _, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            files.append(file_path)

    # Создание архива
    zip_filename = os.path.basename(folder_path) + '.zip'
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        current_size = 0
        for file_path in files:
            file_size = os.path.getsize(file_path)
            if current_size + file_size > target_size:
                break

            zip_file.write(file_path, os.path.relpath(file_path, folder_path))
            current_size += file_size

    print('Архив успешно создан:', zip_filename)

# Пример использования
folder_path = 'D:\\'  # Укажите путь к папке, которую хотите архивировать
target_size = 1 * 1024 * 1024 * 1024  # Размер в байтах (1GB)
create_zip_archive(folder_path, target_size)
