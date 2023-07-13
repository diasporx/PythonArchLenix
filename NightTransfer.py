import os
import time
import paramiko
from tqdm import tqdm

def send_file_to_server(file_path, server_ip, username, password, destination_path):
    try:
        # Создание SSH-клиента
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Подключение к серверу
        ssh_client.connect(server_ip, username=username, password=password)

        # Получение размера файла
        file_size = os.path.getsize(file_path)

        # Отправка файла на сервер
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=os.path.basename(file_path)) as progress_bar:
            def progress_callback(bytes_transferred, file_size):
                progress_bar.update(bytes_transferred - progress_bar.n)

            sftp_client = ssh_client.open_sftp()
            sftp_client.put(file_path, os.path.join(destination_path, os.path.basename(file_path)), callback=progress_callback, confirm=True)
            sftp_client.close()

        # Закрытие SSH-соединения
        ssh_client.close()

        print(f"Файл {file_path} успешно отправлен на сервер {server_ip}")
    except Exception as e:
        print(f"Ошибка при отправке файла на сервер: {e}")

def check_and_send_file():
    folder_path = "D:\\NightTransfer"
    config_file_path = "config_NightTransfer.conf"

    # Проверка наличия папки и файла конфигурации
    if not os.path.exists(folder_path):
        print(f"Папка {folder_path} не существует")
        return

    if not os.path.exists(config_file_path):
        print(f"Файл конфигурации {config_file_path} не существует")
        return

    # Парсинг файла конфигурации
    with open(config_file_path, "r") as config_file:
        server_ip = config_file.readline().strip()
        username = config_file.readline().strip()
        password = config_file.readline().strip()

    # Поиск файлов с расширением .transfer
    transfer_files = [file for file in os.listdir(folder_path) if file.endswith(".transfer")]

    # if len(transfer_files) == 0:
    #     print(f"Файлы с расширением .transfer не найдены в папке {folder_path}")
    #     return

    for transfer_file in transfer_files:
        transfer_file_path = os.path.join(folder_path, transfer_file)

        if os.access(transfer_file_path, os.R_OK):
            # Отправка файла на сервер
            send_file_to_server(transfer_file_path, server_ip, username, password, "D:\\PrevNightTransfer")

            # Удаление файла .transfer
            os.remove(transfer_file_path)
        else:
            print(f"Файл {transfer_file_path} заблокирован другой программой")

# Основной цикл скрипта
while True:
    check_and_send_file()
    time.sleep(10)
