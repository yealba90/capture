
import cv2
import time
import os
from datetime import datetime
from pathlib import Path
from snowflake.connector import connect  # Placeholder for Snowflake connection setup
import threading
import signal
from dotenv import load_dotenv

# Define a global variable to control the loop
stop_threads = False

# Cargar las variables del archivo .env
load_dotenv()

# Obtener las credenciales desde las variables de entorno
user = os.getenv("USER")
password = os.getenv("PASSWORD")
account = os.getenv("ACCOUNT")
role = os.getenv("ROLE")
warehouse = os.getenv("WAREHOUSE")
database = os.getenv("DATABASE")
schema = os.getenv("SCHEMA")

# Function to handle termination signal
def signal_handler(sig, frame):
    global stop_threads
    stop_threads = True
    print("Program stopping...")

# Bind the signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# Configuration parameters for two cameras
CAMERA_CONFIGS = [
    {
        "rtsp_url": 'rtsp://bios:B10s2024!@192.168.1.248:554/live1s1.sdp',
        "save_directory": 'image_storage/camera1',
        "interval": 3600  # Interval time in seconds for capturing images from camera 1 (1 hour)
    },
    {
        "rtsp_url": 'rtsp://bios:B10s2024!@192.168.1.249:554/live1s1.sdp',
        "save_directory": 'image_storage/camera2',
        "interval": 3600  # Interval time in seconds for capturing images from camera 2 (1 hour)
    }
]

# Ensure each save directory exists
for config in CAMERA_CONFIGS:
    Path(config["save_directory"]).mkdir(parents=True, exist_ok=True)


def capture_image(rtsp_url, save_directory):
    cap = cv2.VideoCapture(rtsp_url)
    if not cap.isOpened():
        print(f"Could not open the video stream for {rtsp_url}.")
        return None

    ret, frame = cap.read()
    cap.release()

    if not ret:
        print(f"Failed to capture image from {rtsp_url}.")
        return None

    # Generar el nombre de archivo sin contador y con un '0' al final
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = os.path.join(save_directory, f'image_{timestamp}_0.jpg')
    
    # Guardar la imagen en el directorio especificado
    cv2.imwrite(file_name, frame)
    print(f"Image saved as {file_name}")

    return file_name


def upload_all_images_to_snowflake():
    conn = connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    cursor = conn.cursor()

    for config in CAMERA_CONFIGS:
        save_directory = config["save_directory"]
        
        for file_name in os.listdir(save_directory):
            file_path = os.path.join(save_directory, file_name)
            
            # Procesar solo archivos de imagen que terminen en '_0.jpg'
            if os.path.isfile(file_path) and file_path.endswith("_0.jpg"):
                try:
                    # Ruta de archivo para Snowflake sin compresión
                    snowflake_file_path = f"file://{file_path.replace(os.sep, '/')}"
                    cursor.execute(f"PUT '{snowflake_file_path}' @PIC_SANBERNARDO AUTO_COMPRESS=FALSE")
                    print(f"Uploaded {file_path} to Snowflake stage.")
                    
                    # Cambiar el nombre del archivo para indicar que ha sido subido
                    new_file_path = file_path.replace("_0.jpg", "_1.jpg")
                    os.rename(file_path, new_file_path)
                    print(f"Renamed {file_path} to {new_file_path}")

                except Exception as e:
                    print(f"Failed to upload {file_path} due to {e}")

    conn.close()

def camera_capture_loop(config, camera_index):
    while not stop_threads:
        image_path = capture_image(config["rtsp_url"], config["save_directory"])
        if image_path:
            print(f"Camera {camera_index} captured image: {image_path}")
            upload_all_images_to_snowflake()
        else:
            print(f"Camera {camera_index} skipping upload due to capture error.")

        time.sleep(config["interval"])


# Launch a thread for each camera configuration
threads = []
for i, config in enumerate(CAMERA_CONFIGS, start=1):
    thread = threading.Thread(target=camera_capture_loop, args=(config, i))
    thread.start()
    threads.append(thread)

# Join threads (optional, if you want the main script to wait for their completion)
for thread in threads:
    thread.join()
