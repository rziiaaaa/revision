import socket
import time
import random
from datetime import datetime

def generate_log_line():
    # Données basées sur l'énoncé du TP
    ips = ["192.168.1.100", "192.168.4.78", "192.168.3.56", "10.0.0.5", "172.16.0.20"]
    urls = [
        "/products/lipstick", "/products/foundation", "/products/mascara",
        "/products/skincare/cream", "/products/skincare/sunscreen",
        "/products/hair/shampoo", "/products/hair/conditioner",
        "/cart", "/checkout", "/user/login"
    ]
    methods = ["GET", "POST"]
    codes = [200, 200, 200, 404, 500, 301, 403] # On met plus de 200 pour le réalisme
    
    ip = random.choice(ips)
    timestamp = datetime.now().strftime('%d/%b/%Y:%H:%M:%S +0000')
    method = random.choice(methods)
    url = random.choice(urls)
    # Ajout d'un ID produit aléatoire pour les URLs products [cite: 12, 18]
    if "/products/" in url:
        url += f"?id={random.randint(1000, 9999)}"
    
    code = random.choice(codes)
    size = random.randint(300, 10000)
    
    # Format standard : IP--[Date] "Method URL HTTP/1.1" Status Size [cite: 35]
    return f'{ip}--[{timestamp}] "{method} {url} HTTP/1.1" {code} {size}\n'

def start_generator():
    host = '0.0.0.0'
    port = 9998
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Évite les erreurs de port déjà utilisé
    server_socket.bind((host, port))
    server_socket.listen(1)
    
    print(f"Générateur de logs en attente sur le port {port}...")
    
    while True:
        conn, addr = server_socket.accept()
        print(f"Spark est connecté depuis {addr}")
        try:
            while True:
                log_line = generate_log_line()
                conn.send(log_line.encode('utf-8'))
                print(f"Log envoyé: {log_line.strip()}")
                time.sleep(1) # Envoi d'un log chaque seconde
        except (ConnectionResetError, BrokenPipeError):
            print("Spark s'est déconnecté. En attente d'une nouvelle connexion...")
        finally:
            conn.close()

if __name__ == "__main__":
    start_generator()