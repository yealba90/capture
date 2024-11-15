
import cv2
import time
import os
from datetime import datetime
from pathlib import Path
from snowflake.connector import connect  # Placeholder for Snowflake connection setup
import signal
from dotenv import load_dotenv
import logging
import subprocess

# Define a global variable to control the loop
stop_threads = False

# Cargar las variables del archivo .env
load_dotenv()

# Obtener las credenciales desde las variables de entorno
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("PASSWORD")
account = os.getenv("ACCOUNT")
role = os.getenv("ROLE")
warehouse = os.getenv("WAREHOUSE")
database = os.getenv("DATABASE")
schema = os.getenv("SCHEMA")

# Configuración de los logs
log_directory = "logs"
os.makedirs(log_directory, exist_ok=True)
log_filename = os.path.join(log_directory, f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    filename=log_filename,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Variable global para controlar la ejecución
running = True

# Función para manejar las señales de parada
def signal_handler(sig, frame):
    global running
    print("Signal received, stopping the program gracefully...")
    running = False

# Asignar las señales de interrupción
signal.signal(signal.SIGINT, signal_handler)  # Para Ctrl + C
signal.signal(signal.SIGTERM, signal_handler)  # Para `kill` o detener el proceso

# Configuration parameters for two cameras
CAMERA_CONFIGS = [
    {
        "camera_name": 'machos',
        "rtsp_url": 'rtsp://bios:B10s2024!@10.41.16.161:554/live1s1.sdp',
        "save_directory": 'image_storage/machos',
        "interval": 60  # Interval time in seconds for capturing images from camera 1 (1 hour)
    },
    {
        "camera_name": 'hembras',
        "rtsp_url": 'rtsp://bios:B10s2024!@10.41.16.162:554/live1s1.sdp',
        "save_directory": 'image_storage/hembras',
        "interval": 60  # Interval time in seconds for capturing images from camera 2 (1 hour)
    }
]

# Ensure each save directory exists
for config in CAMERA_CONFIGS:
    Path(config["save_directory"]).mkdir(parents=True, exist_ok=True)


# def capture_image(rtsp_url, save_directory):
#     cap = cv2.VideoCapture(rtsp_url)
#     if not cap.isOpened():
#         print(f"Could not open the video stream for {rtsp_url}.")
#         return None

#     ret, frame = cap.read()
#     cap.release()

#     if not ret:
#         print(f"Failed to capture image from {rtsp_url}.")
#         return None

#     # Generar el nombre de archivo sin contador y con un '0' al final
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     file_name = os.path.join(save_directory, f'image_{timestamp}_0.jpg')
    
#     # Guardar la imagen en el directorio especificado
#     cv2.imwrite(file_name, frame)
#     print(f"Image saved as {file_name}")

#     return file_name


# Función para capturar imágenes
def capture_image(camera_name, rtsp_url, save_directory):
    try:
        cap = cv2.VideoCapture(rtsp_url)
        if not cap.isOpened():
            raise ConnectionError(f"Could not open the video stream for {rtsp_url}")

        ret, frame = cap.read()
        cap.release()

        if not ret:
            raise ValueError(f"Failed to capture image from {rtsp_url}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = os.path.join(save_directory, f'{camera_name}_{timestamp}_0.jpg')
        cv2.imwrite(file_name, frame)
        print(f"Image saved as {file_name}")
        return file_name
    except Exception as e:
        logging.error(f"Error in capture_image: {e}")
        return None


# def upload_all_images_to_snowflake():
#     conn = connect(
#         user=user,
#         password=password,
#         account=account,
#         role=role,
#         warehouse=warehouse,
#         database=database,
#         schema=schema
#     )
#     cursor = conn.cursor()

#     for config in CAMERA_CONFIGS:
#         save_directory = config["save_directory"]
        
#         for file_name in os.listdir(save_directory):
#             file_path = os.path.join(save_directory, file_name)
            
#             # Procesar solo archivos de imagen que terminen en '_0.jpg'
#             if os.path.isfile(file_path) and file_path.endswith("_0.jpg"):
#                 try:
#                     # Ruta de archivo para Snowflake sin compresión
#                     snowflake_file_path = f"file://{file_path.replace(os.sep, '/')}"
#                     cursor.execute(f"PUT '{snowflake_file_path}' @PIC_SANBERNARDO AUTO_COMPRESS=FALSE")
#                     print(f"Uploaded {file_path} to Snowflake stage.")
                    
#                     # Cambiar el nombre del archivo para indicar que ha sido subido
#                     new_file_path = file_path.replace("_0.jpg", "_1.jpg")
#                     os.rename(file_path, new_file_path)
#                     print(f"Renamed {file_path} to {new_file_path}")

#                 except Exception as e:
#                     print(f"Failed to upload {file_path} due to {e}")

#     conn.close()

# Función para subir imágenes a Snowflake
def upload_all_images_to_snowflake():
    try:
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
                if os.path.isfile(file_path) and file_path.endswith("_0.jpg"):
                    try:
                        snowflake_file_path = f"file://{file_path.replace(os.sep, '/')}"
                        cursor.execute(f"PUT '{snowflake_file_path}' @PIC_SANBERNARDO AUTO_COMPRESS=FALSE")
                        new_file_path = file_path.replace("_0.jpg", "_1.jpg")
                        os.rename(file_path, new_file_path)
                        print(f"Uploaded {file_path} and renamed to {new_file_path}")
                    except Exception as upload_error:
                        logging.error(f"Failed to upload {file_path}: {upload_error}")
        conn.close()
    except Exception as e:
        logging.error(f"Error in upload_all_images_to_snowflake: {e}")


# Función para verificar y actualizar el repositorio
def check_for_updates():
    try:
        result = subprocess.run(["git", "pull"], capture_output=True, text=True)
        if "Already up to date" not in result.stdout:
            print("Repository updated. Restarting the program...")
            logging.info("Repository updated. Restarting the program...")
            return True
    except Exception as e:
        logging.error(f"Failed to check for updates: {e}")
    return False


# # Bucle principal para ejecutar el programa con reintentos
# def main():
#     global running
#     while running:
#         try:
#             # Ejecutar la captura de imagen y la carga en Snowflake
#             for config in CAMERA_CONFIGS:
#                 file_name = capture_image(config["rtsp_url"], config["save_directory"])
#                 if file_name:
#                     upload_all_images_to_snowflake()
#                 time.sleep(config["interval"])

#             # Espera antes del próximo ciclo
#             time.sleep(10)
#         except Exception as e:
#             # Registrar cualquier error no manejado y esperar antes de reintentar
#             logging.error(f"Unhandled error in main loop: {e}")
#             print("Error detected. Retrying in 30 seconds...")
#             time.sleep(30)  # Espera antes de reintentar

# Bucle principal para ejecutar el programa con reintentos y pausas entre capturas
def main():
    global running
    while running:
        # Verificar si hay actualizaciones en el repositorio
        if check_for_updates():
            # Si hay actualizaciones, reiniciar el programa
            os.execv(__file__, ["python3"] + sys.argv)

        try:
            # Ejecutar la captura de imagen y la carga en Snowflake para cada cámara
            for config in CAMERA_CONFIGS:
                file_name = capture_image(config["camera_name"], config["rtsp_url"], config["save_directory"])
                if file_name:
                    upload_all_images_to_snowflake()

                # Pausa entre capturas basada en el intervalo configurado para cada cámara
            print("Esperando ejecucion")    
            time.sleep(300) # Intervalo de tiempo para envio de imagenes cada hora

        except Exception as e:
            # Registrar cualquier error no manejado y esperar antes de reintentar
            logging.error(f"Unhandled error in main loop: {e}")
            print("Error detected. Retrying in 30 seconds...")
            time.sleep(30)  # Espera antes de reintentar


if __name__ == "__main__":
    main()
    print("Program stopped.")




