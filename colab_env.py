# colab use jupyter notebook, eğer localde postgre sql, dbeaver vs gibi programlara erişimini yoksa 
bu proje colab ve drive iş birliği ile (ya da colab localden de cekebilir dosyaları, burada COPY önemine değin) aşağıdaki bloklar çalıştırılarak da implement edilebilir.


%%bash
echo test
# 00_SETUP

ps aux | grep -E 'python|jupyter|postgres|psql' | grep -v grep

pkill -f psql || true
pkill -f postgres || true

echo "cleanup done."

%%bash
set -e
# 01_SETUP
# 1) PostgreSQL install
apt-get update -y
apt-get install -y postgresql postgresql-contrib

# 2) Start cluster (Colab’ta systemd yok; pg_ctlcluster ile başlatıyoruz)
pg_ctlcluster 14 main start || true

# 3) Quick health check
sudo -u postgres psql -c "select version();"
sudo -u postgres psql -c "show data_directory;"
sudo -u postgres psql -c "select now();"

# 02_SETUP - Copy the dataset from Drive
from google.colab import drive
import os
import shutil

drive.mount('/content/drive')

DATA_DIR = "/content/data"
DRIVE_DIR = "/content/drive/MyDrive/retail_dw_data"

os.makedirs(DATA_DIR, exist_ok=True)
print("CSV files have been copying on Colab disk...")
shutil.copy(f"{DRIVE_DIR}/01_empty_95_off.csv", DATA_DIR)
shutil.copy(f"{DRIVE_DIR}/02_empty_95_on.csv", DATA_DIR)
shutil.copy(f"{DRIVE_DIR}/03_empty_5_off.csv", DATA_DIR)
shutil.copy(f"{DRIVE_DIR}/04_empty_5_on.csv", DATA_DIR)
print("All of them is copied! Files ready.")


# 04_SETUP -Permissions: Allow Colab for reaching out your Drive, otherwise, you'll get an error
%%bash
sudo chmod o+rx /content
sudo chmod o+rx /content/data
sudo chmod o+r /content/data/01_empty_95_off.csv
sudo chmod o+r /content/data/02_empty_95_on.csv
sudo chmod o+r /content/data/03_empty_5_off.csv
sudo chmod o+r /content/data/04_empty_5_on.csv




  
