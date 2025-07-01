import os
import argparse
import random
import string

def generate_readable_data(chunk_size):
    """Génère une chaîne ASCII aléatoire de taille chunk_size"""
    return ''.join(random.choices(string.ascii_letters + string.digits + ' \n', k=chunk_size)).encode('utf-8')

def generate_binary_data(chunk_size):
    """Génère des octets aléatoires de taille chunk_size"""
    return os.urandom(chunk_size)

def generate_file(file_path, size_gb, readable=False):
    chunk_size = 1024 * 1024  # 1 MB
    total_chunks = int(size_gb * 1024)  # Nombre de chunks de 1MB à écrire

    with open(file_path, 'wb') as f:
        for i in range(total_chunks):
            data = generate_readable_data(chunk_size) if readable else generate_binary_data(chunk_size)
            f.write(data)
            if i % 100 == 0:
                print(f"Progress: {i}/{total_chunks} MB written", end='\r')

    print(f"\nFichier généré : {file_path} ({size_gb} Go)")

def main():
    parser = argparse.ArgumentParser(description="Générer un fichier de taille personnalisée.")
    parser.add_argument("--size", type=float, required=True, help="Taille du fichier en Go (ex: 0.5, 1, 2)")
    parser.add_argument("--output", type=str, required=True, help="Chemin de sortie du fichier")
    parser.add_argument("--readable", action="store_true", help="Génère des données lisibles (texte ASCII)")

    args = parser.parse_args()
    generate_file(args.output, args.size, args.readable)

if __name__ == "__main__":
    main()
