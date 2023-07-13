import os
import random
import string
import datetime
import pywintypes  # Добавлен импорт pywintypes
import win32file   # Добавлен импорт win32file
import time  # Added the import statement for 'time'

folder_path = 'D:\\NextSRV'
file_size = 1024 * 1024 * 1  # 1MB

# Создание папки и файла с меткой времени до 1980 года
def create_folder_with_file(folder_name):
    folder = os.path.join(folder_path, folder_name)
    os.makedirs(folder, exist_ok=True)
    file = os.path.join(folder, 'file.txt')
    with open(file, 'wb') as f:
        f.write(b'0' * file_size)
    
    # Установка времени создания и времени доступа на 1979 год
    c_time = datetime.datetime(1979, 1, 1)
    a_time = datetime.datetime(1979, 1, 1)
    w_time = datetime.datetime(1979, 1, 1)
    time_tuple = c_time.timetuple()
    time_stamp = int(time.mktime(time_tuple))
    
    # Установка времени модификации на 1979 год с использованием pywin32
    file_handle = win32file.CreateFile(
        file,
        win32file.GENERIC_WRITE,
        0,
        None,
        win32file.OPEN_EXISTING,
        win32file.FILE_ATTRIBUTE_NORMAL,
        None
    )
    win32file.SetFileTime(
        file_handle,
        pywintypes.Time(time_stamp),
        pywintypes.Time(a_time.timestamp()),
        pywintypes.Time(w_time.timestamp())
    )
    file_handle.close()

# Генерация уникальных имен папок
def generate_folder_names(num_folders):
    folder_names = set()
    while len(folder_names) < num_folders:
        folder_name = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        folder_names.add(folder_name)
    return list(folder_names)

# Создание папок и файлов
def create_folders_with_files(num_folders):
    folder_names = generate_folder_names(num_folders)
    for folder_name in folder_names:
        create_folder_with_file(folder_name)

# Запуск скрипта
if __name__ == '__main__':
    num_folders = 998  # Количество папок, которые нужно создать
    create_folders_with_files(num_folders)
    print(f'Создано {num_folders} папок в {folder_path}')
