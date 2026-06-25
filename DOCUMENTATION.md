# Documentation du Projet : Plateforme Lakehouse pour l'Analytique E-Commerce

## Architecture Medallion (Bronze → Silver → Gold)

Ce projet implémente une architecture **Medallion** (ou « Médaillon ») en trois couches avec **PySpark**, **Delta Lake**, et une orchestration via **Docker Compose**. Les données synthétiques d'e-commerce (clients, produits, commandes, paiements, livraisons, événements web) sont générées avec des défauts intentionnels, nettoyées, puis organisées en **schéma en étoile** pour l'analyse.

---

## 1. Structure du Projet

```
.
├── config.py                      # Configuration centrale (chemins, volumes, seuils qualité)
├── spark_session.py               # Factory SparkSession (config Spark/Delta)
├── docker-compose.yml             # Orchestration Docker (pipeline + services de requêtage)
├── Dockerfile.pipeline            # Image pour le pipeline ETL (Python 3.12 + Java 21 + PySpark)
├── Dockerfile.spark-thrift        # Image pour le serveur JDBC Spark Thrift
├── requirements.txt               # Dépendances Python
│
├── src/
│   ├── data_generation/           # Phase 1 : Génération de données synthétiques
│   │   ├── generate_all.py        #   Orchestrateur : lance les 6 générateurs
│   │   ├── generate_customers.py  #   10K clients
│   │   ├── generate_products.py   #   1K produits
│   │   ├── generate_orders.py     #   50K commandes
│   │   ├── generate_payments.py   #   50K paiements
│   │   ├── generate_deliveries.py #   50K livraisons
│   │   └── generate_web_events.py #   20K événements web
│   ├── bronze/
│   │   └── ingest_to_bronze.py    # Phase 2 : Ingestion brute en Delta Lake
│   ├── silver/
│   │   └── transform_to_silver.py # Phase 3 : Nettoyage et déduplication
│   └── gold/
│       └── build_gold_star_schema.py # Phase 4 : Schéma en étoile
│
├── scripts/
│   ├── setup_metabase.py          # Configuration automatique Metabase
│   ├── streamlit_app.py           # Dashboard Streamlit (lecture directe Parquet)
│   └── start-thrift.sh            # Script de démarrage Spark Thrift Server
│
├── tests/
│   ├── test_bronze.py             # 24 tests unitaires Bronze
│   ├── test_silver.py             # 18 tests unitaires Silver
│   ├── test_gold.py               # 9 tests unitaires Gold
│   ├── test_e2e.py                # 6 tests de bout en bout
│   └── conftest.py                # Fixtures SparkSession partagées
│
└── data/
    ├── raw/                       # CSV/JSON générés
    ├── bronze/                    # Tables Delta brutes (mode append)
    ├── silver/                    # Tables Delta nettoyées (mode overwrite)
    └── gold/                      # Schéma en étoile Delta (mode overwrite)
```

---

## 2. Flux de Données (Data Pipeline)

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │  Phase 1 : Génération                                               │
 │  src/data_generation/generate_all.py                                │
 │  ┌──────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐ ┌───────────┐  │
 │  │customers │ │ products │ │ orders │ │ payments │ │deliveries │  │
 │  │ 10K rows │ │  1K rows │ │ 50K    │ │ 50K      │ │ 50K       │  │
 │  └────┬─────┘ └────┬─────┘ └───┬────┘ └────┬─────┘ └─────┬─────┘  │
 │       │            │           │           │             │         │
 │       ▼            ▼           ▼           ▼             ▼         │
 │  data/raw/customers.csv  ...  data/raw/orders.csv  ...              │
 └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
 ┌─────────────────────────────────────────────────────────────────────┐
 │  Phase 2 : Bronze (Ingestion brute)                                 │
 │  src/bronze/ingest_to_bronze.py                                     │
 │                                                                     │
 │  Lecture CSV/JSON → Schéma explicite → Colonnes d'audit            │
 │  (_ingestion_ts, _source_file, _row_hash, _table_name)              │
 │  → Écriture en mode APPEND dans data/bronze/{table}/               │
 │                                                                     │
 │  Formats supportés : Delta Lake (format="delta")                   │
 └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
 ┌─────────────────────────────────────────────────────────────────────┐
 │  Phase 3 : Silver (Nettoyage)                                       │
 │  src/silver/transform_to_silver.py                                  │
 │                                                                     │
 │  Pour chaque table Bronze :                                         │
 │  1. Déduplication (row_number + partitionBy id)                     │
 │  2. Normalisation des dates (4 formats : yyyy-MM-dd, MM/dd/yyyy,   │
 │     dd-MM-yyyy, yyyyMMdd)                                           │
 │  3. Remplissage des valeurs NULL                                    │
 │  4. Correction des valeurs négatives (abs, ou 0.01)                 │
 │  5. Validation des clés étrangères (jointure avec tables de ref)   │
 │  → Écriture en mode OVERWRITE dans data/silver/{table}/            │
 └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
 ┌─────────────────────────────────────────────────────────────────────┐
 │  Phase 4 : Gold (Schéma en étoile)                                  │
 │  src/gold/build_gold_star_schema.py                                 │
 │                                                                     │
 │  Dimensions :                                                       │
 │  ┌──────────────┐  ┌─────────────┐  ┌──────────┐  ┌──────────────┐ │
 │  │ dim_customer │  │ dim_product │  │ dim_date │  │ dim_location │ │
 │  │ SCD Type 2   │  │             │  │ 23K jours │  │              │ │
 │  └──────┬───────┘  └──────┬──────┘  └────┬─────┘  └──────┬───────┘ │
 │         │                 │              │               │         │
 │         └──────┬──────────┴──────┬───────┴───────┬───────┘         │
 │                │                 │               │                 │
 │        ┌───────▼─────────────────▼───────────────▼────────┐        │
 │        │                   fact_order                      │        │
 │        │  customer_key | product_key | date_key | loc_key  │        │
 │        │  quantity | total_amount | order_status           │        │
 │        └────────────────────┬──────────────────────────────┘        │
 │                             │                                       │
 │        ┌────────────────────▼──────────────────────┐                │
 │        │  fact_payment      │    fact_delivery      │                │
 │        │  order_key         │    order_key          │                │
 │        │  payment_method    │    carrier            │                │
 │        │  payment_status    │    estimated_days     │                │
 │        │  amount            │    actual_days        │                │
 │        └────────────────────┴──────────────────────┘                │
 │                                                                     │
 │  → Écriture en mode OVERWRITE dans data/gold/{table}/              │
 └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
 ┌─────────────────────────────────────────────────────────────────────┐
 │  Visualisation                                                     │
 │                                                                     │
 │  Option 1 (recommandé) : Streamlit                                 │
 │  streamlit run scripts/streamlit_app.py                             │
 │  → Lit les fichiers Parquet Delta directement (pas de Spark)       │
 │  → http://localhost:8501                                            │
 │                                                                     │
 │  Option 2 : Spark Thrift + Metabase                                 │
 │  docker compose --profile query up -d                               │
 │  python scripts/setup_metabase.py                                   │
 │  → Serveur JDBC sur port 10000, Metabase sur http://localhost:3000  │
 └─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Explication du Code par Fichier

### 3.1 `config.py` — Configuration centrale

Définit les chemins des données, les volumes par table, et les seuils de qualité :

```python
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# Répertoires par couche
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DATA_DIR = PROJECT_ROOT / "data" / "gold"

# Configuration des sources (volume par run)
SOURCE_CONFIG = {
    "customers":  { "file": "customers.csv",  "row_count": 10_000 },
    "products":   { "file": "products.csv",   "row_count": 1_000 },
    "orders":     { "file": "orders.csv",     "row_count": 50_000 },
    "payments":   { "file": "payments.csv",   "row_count": 50_000 },
    "deliveries": { "file": "deliveries.csv", "row_count": 50_000 },
    "web_events": { "file": "web_events.json","row_count": 20_000 },
}

# Seuils de qualité
QUALITY_CONFIG = {
    "max_null_fraction": 0.05,        # max 5% de nulls
    "max_duplicate_fraction": 0.02,   # max 2% de doublons
    "max_fk_violation_fraction": 0.01,# max 1% de FK invalides
}
```

---

### 3.2 `spark_session.py` — Factory SparkSession

Configure Spark en mode local avec Delta Lake et des réglages pour machine à mémoire limitée :

```python
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "LakehouseETL") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[1]")                          # 1 seul cœur
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4") # 4 partitions shuffle
        .config("spark.network.timeout", "120s")      # timeout réseau long
        .config("spark.executor.heartbeatInterval", "30s")
        .getOrCreate()
    )
```

**Pourquoi ces réglages ?** Sur une machine avec 2,6 Go de RAM, il faut limiter au maximum la parallélisation (`local[1]`, 4 partitions shuffle) et augmenter les timeouts pour éviter les échecs liés au manque de mémoire.

---

### 3.3 `src/data_generation/generate_all.py` — Orchestrateur de génération

Ce script :
1. Crée le dossier `data/raw/`
2. Génère clients et produits (sans dépendances FK)
3. Charge les IDs générés pour créer les relations FK
4. Génère commandes (référence clients + produits)
5. Charge les IDs des commandes
6. Génère paiements et livraisons (référence commandes)
7. Génère événements web (référence produits)

```python
def generate_all() -> dict[str, Path]:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Indépendants
    customers_path = generate_customers(10_000)
    products_path = generate_products(1_000)

    # Chargement des IDs pour les FK
    cust_ids = [...]   # chargé depuis customers.csv
    prod_ids = [...]   # chargé depuis products.csv

    # Dépend des IDs ci-dessus
    orders_path = generate_orders(50_000, customer_ids=cust_ids, product_ids=prod_ids)

    ord_ids = [...]    # chargé depuis orders.csv

    payments_path = generate_payments(50_000, order_ids=ord_ids)
    deliveries_path = generate_deliveries(50_000, order_ids=ord_ids)
    web_events_path = generate_web_events(20_000, product_ids=prod_ids)

    return { "customers": customers_path, "products": products_path, ... }
```

---

### 3.4 `src/data_generation/generate_customers.py` — Générateur de clients

Exemple typique de générateur avec défauts intentionnels :

```python
import csv, random
from faker import Faker

fake = Faker()

# 4 formats de date différents pour tester la normalisation Silver
DATE_FORMATS = [
    lambda d: d.strftime("%Y-%m-%d"),     # standard
    lambda d: d.strftime("%m/%d/%Y"),     # américain
    lambda d: d.strftime("%d-%m-%Y"),     # européen
    lambda d: d.strftime("%Y%m%d"),       # compact
]

def generate_customers(row_count: int = 50_000) -> Path:
    with open("data/raw/customers.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "first_name", "last_name", "email", ...])

        for i in range(row_count):
            # 5% de chances de créer un DOUBLON
            if i > 0 and random.random() < 0.05:
                cust_id = base_ids[random.randint(0, i - 1)]  # ID déjà utilisé
            else:
                cust_id = base_ids[i]

            # 3% de chances d'avoir un email NULL
            email = None if random.random() < 0.03 else fake.email()

            # Format de date aléatoire
            reg_date_str = random.choice(DATE_FORMATS)(reg_date)

            writer.writerow([cust_id, first_name, last_name, email, ...])

    return filepath
```

**Défauts intentionnels générés :**
- **~5% de doublons** sur les IDs
- **~3% de valeurs NULL** sur certains champs (email, catégorie)
- **4 formats de date différents** pour tester la normalisation
- **~2% de FK orphelines** (commandes référant des clients inexistants)
- **Valeurs négatives** sur les prix/quantités

---

### 3.5 `src/bronze/ingest_to_bronze.py` — Ingestion Bronze

Cette étape lit les fichiers CSV/JSON, applique un schéma explicite, ajoute des colonnes d'audit et écrit en **mode append** dans Delta Lake.

```python
from pyspark.sql.functions import current_timestamp, input_file_name, md5, concat_ws, lit

# Schémas explicites pour chaque table (exemple : clients)
SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType(), False),   # NOT NULL
        StructField("first_name", StringType(), True),     # nullable
        StructField("email", StringType(), True),
        StructField("registration_date", StringType(), True),
        # ...
    ]),
    # ... autres tables
}

def add_audit_columns(df, table_name):
    """Ajoute 4 colonnes d'audit à chaque ligne."""
    return (df
        .withColumn("_ingestion_ts", current_timestamp())  # date d'ingestion
        .withColumn("_source_file", input_file_name())      # fichier source
        .withColumn("_row_hash", md5(concat_ws("||", *df.columns)))  # empreinte
        .withColumn("_table_name", lit(table_name))        # nom de la table
    )

def ingest_table(spark, table_name):
    df = read_source(spark, table_name)      # lecture CSV/JSON
    raw_count = df.count()
    df_with_audit = add_audit_columns(df, table_name)

    # Écriture en mode APPEND dans Delta Lake
    df_with_audit.write.mode("append").format("delta") \
        .option("mergeSchema", "true") \
        .save(f"data/bronze/{table_name}")

    return {"table": table_name, "raw_count": raw_count}
```

**Pourquoi `mode("append")` ?** Les données brutes s'accumulent run après run. Chaque exécution du pipeline ajoute ses données à la couche Bronze. Cela permet de garder l'historique complet.

---

### 3.6 `src/silver/transform_to_silver.py` — Transformation Silver

C'est l'étape la plus importante. Elle nettoie les données Bronze :

```python
from pyspark.sql.functions import coalesce, col, lit, row_number, to_date, when
from pyspark.sql.window import Window

# 4 formats de date à essayer
DATE_PATTERNS = ["yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy", "yyyyMMdd"]

def parse_date_multi(df, col_name, target_name=None):
    """Tente plusieurs formats de date et prend le premier qui fonctionne."""
    target = target_name or col_name
    parsed = coalesce(*[to_date(col(col_name), p) for p in DATE_PATTERNS])
    return df.withColumn(target, parsed)

def dedup_by_key(df, key, order_col="_ingestion_ts"):
    """Garde la version la plus récente de chaque enregistrement (par date d'ingestion)."""
    w = Window.partitionBy(key).orderBy(col(order_col).desc())
    return df.withColumn("_rn", row_number().over(w)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")

def fill_nulls(df, fills):
    """Remplace les NULL par des valeurs par défaut."""
    for col_name, default in fills.items():
        df = df.fillna({col_name: default})
    return df

def validate_fk(df, ref_path, fk_col, ref_col, spark):
    """Garde seulement les lignes dont la FK existe dans la table de référence."""
    ref_df = spark.read.format("delta").load(ref_path).select(ref_col).distinct()
    return df.join(ref_df.hint("broadcast"), col(fk_col) == col(ref_col), "inner")
```

**Exemple pour les clients :**
```python
def transform_customers(bronze_path, silver_path, spark):
    df = spark.read.format("delta").load(bronze_path)

    df = dedup_by_key(df, "customer_id")        # dédoublonnage
    df = dedup_by_key(df, "email")              # email unique aussi
    df = parse_date_multi(df, "registration_date")
    df = parse_date_multi(df, "birth_date")
    df = fill_nulls(df, {                        # valeurs par défaut
        "email": "unknown@email.com",
        "first_name": "Unknown",
        "loyalty_tier": "Bronze",
    })
    df = add_silver_metadata(df)

    df.write.mode("overwrite").format("delta").save(silver_path)
```

**Traitements spécifiques par table :**
| Table | Traitement spécial |
|-------|-------------------|
| `customers` | Dédoublonnage sur customer_id ET email ; remplissage des champs d'adresse |
| `products` | Correction des prix/côuts négatifs → 0.01 ; stock négatif → 0 |
| `orders` | `total_amount = quantity * unit_price` ; validation FK clients + produits |
| `payments` | Validation FK commandes |
| `deliveries` | Validation FK commandes |

**Pourquoi `mode("overwrite")` ?** La couche Silver est régénérée à chaque run. Elle traite l'ensemble des données Bronze (accumulées) et produit un snapshot nettoyé.

---

### 3.7 `src/gold/build_gold_star_schema.py` — Schéma en étoile Gold

Construit le schéma en étoile à partir des tables Silver.

**Tables de dimension :**

```python
def build_dim_customer(spark):
    df = spark.read.format("delta").load("data/silver/customers")

    # Clé de substitution auto-incrémentée
    df = df.withColumn("customer_key",
        row_number().over(Window.orderBy("customer_id")).cast(IntegerType()))

    # SCD Type 2 : suivi dans le temps
    df = df.withColumn("valid_from", lit(time.strftime("%Y-%m-%d")))
    df = df.withColumn("valid_to", lit("9999-12-31"))
    df = df.withColumn("is_current", lit(True))

    return df.select("customer_key", "customer_id", "first_name", "last_name", ...)

def build_dim_date(spark, start="2020-01-01", end="2026-12-31"):
    """Génère 23 013 jours avec toutes les dimensions temporelles."""
    days = spark.sql(
        f"SELECT sequence(date'{start}', date'{end}', interval 1 day) as days"
    ).selectExpr("explode(days) as date_key")

    return days.select(
        col("date_key"),
        year("date_key").alias("year"),
        month("date_key").alias("month"),
        dayofmonth("date_key").alias("day"),
        quarter("date_key").alias("quarter"),
        when(dayofweek("date_key").isin(1, 7), True).otherwise(False).alias("is_weekend"),
    ).withColumn("year_month", concat(col("year"), lit("-"), lpad(col("month"), 2, "0")))
```

**Tables de faits :**

```python
def build_fact_order(spark):
    orders = spark.read.format("delta").load("data/silver/orders")
    customers = spark.read.format("delta").load("data/gold/dim_customer")
    products = spark.read.format("delta").load("data/gold/dim_product")

    # Jointure avec les dimensions pour obtenir les clés de substitution
    fact = (orders
        .join(customers.select("customer_id", "customer_key"), on="customer_id")
        .join(products.select("product_id", "product_key"), on="product_id"))

    return fact.select(
        "order_id", "customer_key", "product_key",
        to_date("order_date").alias("date_key"),
        "quantity", "unit_price", "total_amount", "order_status"
    )
```

**Ordre de construction :**
1. `dim_date` (aucune dépendance)
2. `dim_customer` (lit Silver/customers)
3. `dim_product` (lit Silver/products)
4. `dim_location` (adresses distinctes depuis Silver/customers)
5. `fact_order` (dépend de dim_customer + dim_product)
6. `fact_payment` (dépend de fact_order)
7. `fact_delivery` (dépend de fact_order)

---

### 3.8 `docker-compose.yml` — Orchestration Docker

```yaml
services:
  pipeline:                          # Exécute l'ETL complet
    build: { dockerfile: Dockerfile.pipeline }
    entrypoint: /bin/bash
    command: |
      python src/data_generation/generate_all.py
      python src/bronze/ingest_to_bronze.py
      python src/silver/transform_to_silver.py
      python src/gold/build_gold_star_schema.py
    volumes:
      - ./src:/opt/airflow/src       # Code monté en volume
      - ./data:/opt/airflow/data     # Données persistantes
      - ./config.py:/opt/airflow/config.py
      - ./spark_session.py:/opt/airflow/spark_session.py
    environment:
      JAVA_HOME: /usr/lib/jvm/java-21-openjdk-amd64
    restart: "no"                    # S'exécute une fois et s'arrête

  spark-thrift:                      # Serveur JDBC pour requêter les données
    profiles: ["query"]              # Démarrage optionnel
    build: { dockerfile: Dockerfile.spark-thrift }
    ports: ["10000:10000"]
    volumes: ["./data:/opt/airflow/data"]

  metabase:                          # Interface BI (optionnelle)
    profiles: ["query"]
    image: metabase/metabase:latest
    ports: ["3000:3000"]
    environment:
      JAVA_OPTS: "-Xmx256m -XX:+UseG1GC"
    depends_on: [spark-thrift]
```

**Fonctionnement :**
- `docker compose run --rm pipeline` → exécute les 4 phases et s'arrête
- `docker compose --profile query up -d` → démarre Spark Thrift + Metabase

---

### 3.9 `Dockerfile.pipeline` — Image du Pipeline

```dockerfile
FROM python:3.12-slim

# Installation de Java 21 (nécessaire pour PySpark)
RUN apt-get update && apt-get install -y openjdk-21-jre

WORKDIR /opt/airflow

# Dépendances Python
COPY requirements.txt .
RUN pip install pyspark==3.5.1 delta-spark==3.1.0 faker==24.4.0 pytest chispa
```

---

### 3.10 `Dockerfile.spark-thrift` — Image Spark Thrift

```dockerfile
FROM apache/spark:3.5.1

# Ajout des JARs Delta Lake
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/...jar
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/...jar

COPY scripts/start-thrift.sh /opt/spark/start-thrift.sh
EXPOSE 10000
ENTRYPOINT ["/opt/spark/start-thrift.sh"]
```

### 3.11 `scripts/start-thrift.sh` — Démarrage Thrift

```bash
SPARK_NO_DAEMONIZE=true \
/opt/spark/sbin/start-thriftserver.sh \
  --master local[2] \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.driver.memory=512m
```

---

### 3.12 `scripts/streamlit_app.py` — Dashboard Streamlit

Le dashboard lit les fichiers Parquet Delta directement avec **pyarrow** (pas besoin de Spark). Il est donc très léger en mémoire.

```python
import streamlit as st
import pyarrow.dataset as ds
import pandas as pd

GOLD = Path("data/gold")

@st.cache_data
def load_table(name):
    path = str(GOLD / name)
    return ds.dataset(path, format="parquet").to_table().to_pandas()

# Chargement des 7 tables Gold
df_customers = load_table("dim_customer")
df_products = load_table("dim_product")
df_dates = load_table("dim_date")
df_orders = load_fact_orders()          # seulement les colonnes nécessaires
df_payments = load_fact_payments()
df_deliveries = load_fact_deliveries()

# Filtres dans la sidebar
with st.sidebar:
    years = st.multiselect("Année(s)", df_dates["year"].unique())
    categories = st.multiselect("Catégorie(s)", df_products["category"].unique())
    tiers = st.multiselect("Niveau fidélité", df_customers["loyalty_tier"].unique())

# KPIs
st.metric("Revenu Total", f"${df_orders_filtered['total_amount'].sum():,.0f}")
st.metric("Nombre de Commandes", f"{df_orders_filtered['order_id'].nunique():,}")

# Graphiques (via st.bar_chart, st.line_chart)
st.bar_chart(top_customers.set_index("full_name")["total_amount"])
st.line_chart(monthly.set_index("ym")["total_amount"])
```

**Particularité technique :** Les fichiers Delta Lake sont des fichiers Parquet standard avec un répertoire `_delta_log/` pour le suivi des versions. `pyarrow.dataset` lit les fichiers Parquet directement, ignorant le log Delta. Cela fonctionne car les fichiers de données actuels sont dans le répertoire racine de la table.

---

### 3.13 `scripts/setup_metabase.py` — Configuration Metabase

Script qui automatise la configuration de Metabase via son API REST :

```python
# 1. Attendre que Metabase soit prêt
def wait_for_metabase(max_retries=120, interval=3):
    for i in range(max_retries):
        resp = http("/api/health")
        if resp and resp.get("status") == "ok":
            return True
        time.sleep(interval)

# 2. Créer l'utilisateur admin (ou utiliser existant)
session = http("/api/session", data={"username": "admin@example.com", "password": "Metabase!2026"})

# 3. Ajouter la base de données SparkSQL
http("/api/database", data={
    "engine": "sparksql",
    "name": "Gold Layer (SparkSQL)",
    "details": { "host": "spark-thrift", "port": 10000, "dbname": "default" },
    "is_full_sync": False,
})

# 4. Créer les questions (5 graphiques) et le dashboard
card_ids = create_questions(token, db_id)    # 5 questions SQL
create_dashboard(token, card_ids)             # 1 dashboard avec mise en page
```

---

## 4. Qualité des Données (Data Quality)

Le projet simule volontairement des problèmes de qualité de données pour démontrer les capacités de nettoyage de la couche Silver :

| Problème | Comment | Taux | Géré dans |
|----------|---------|------|-----------|
| **Doublons** | `random.random() < 0.05` réutilise un ID existant | ~5% | `dedup_by_key()` |
| **Valeurs NULL** | `None if random.random() < 0.03` | ~3% | `fill_nulls()` |
| **Formats de date** | 4 formats aléatoires (`yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy`, `yyyyMMdd`) | 25% chacun | `parse_date_multi()` |
| **FK orphelines** | Génération d'ordres avec IDs aléatoires | ~2% | `validate_fk()` |
| **Valeurs négatives** | Prix négatifs, stock négatif | ~1% | Correction dans Silver |

---

## 5. Tests Unitaires

57 tests utilisant `pytest` + `chispa` (bibliothèque de test pour DataFrames PySpark) :

```bash
# Exécuter tous les tests
cd /opt/airflow && python -m pytest tests/ -v

# Tests par couche
python -m pytest tests/test_bronze.py -v   # 24 tests
python -m pytest tests/test_silver.py -v   # 18 tests
python -m pytest tests/test_gold.py -v     # 9 tests
python -m pytest tests/test_e2e.py -v      # 6 tests
```

**Exemple de test (`tests/test_silver.py`) :**

```python
def test_dedup_removes_duplicates(spark):
    data = [
        ("1", "Alice", "2024-01-02"),
        ("1", "Bob",   "2024-01-01"),  # doublon, version plus ancienne
    ]
    df = spark.createDataFrame(data, ["id", "name", "_ingestion_ts"])
    result = dedup_by_key(df, "id")
    assert result.count() == 1
    assert result.collect()[0]["name"] == "Alice"  # garde la plus récente
```

---

## 6. Commandes d'Utilisation

### Pipeline complet (génération + ETL)

```bash
# Tout en un
docker compose run --rm pipeline
```

### Visualisation avec Streamlit (recommandé)

```bash
# Installer les dépendances (une fois)
pip install streamlit pandas pyarrow

# Lancer le dashboard
export PATH=$HOME/.local/bin:$PATH
streamlit run scripts/streamlit_app.py --server.port 8501 --server.headless true

# Ouvrir http://localhost:8501
```

### Visualisation avec Metabase (nécessite plus de RAM)

```bash
# Démarrer les services
docker compose --profile query up -d

# Attendre ~2 min, puis configurer
python scripts/setup_metabase.py

# Ouvrir http://localhost:3000
# Login : admin@example.com / Metabase!2026
```

### Nettoyage

```bash
# Arrêter les services de requêtage
docker compose --profile query down

# Supprimer toutes les données et repartir de zéro
rm -rf data/raw data/bronze data/silver data/gold
```

---

## 7. Contraintes et Limitations

### Mémoire (2,6 Go RAM)

La machine hôte a seulement 2,6 Go de RAM. Cela impose des contraintes fortes :

| Service | Mémoire allouée | Problème si dépassé |
|---------|-----------------|---------------------|
| Pipeline PySpark | `local[1]`, 4 partitions | OOM sur Silver si trop de données |
| Spark Thrift | `-Xmx512m` | Heartbeat timeout → crash |
| Metabase | `-Xmx256m` | Reste bloqué à l'initialisation |

**Conséquence :** Il est impossible d'exécuter le pipeline ET les services de requêtage (Spark Thrift + Metabase) simultanément. La solution Streamlit contourne ce problème en lisant les fichiers Parquet sans Spark.

### Format Delta Lake

Les données sont stockées au format **Delta Lake**. Bien que ce soit du Parquet sous-jacent, la lecture directe par pyarrow ne tient pas compte du `_delta_log`. Cela fonctionne car nos tables sont écrasées à chaque run (`mode("overwrite")`), donc les fichiers obsolètes sont remplacés.

---

## 8. Architecture Technique

```
┌─────────────────────────────────────────────────────────────┐
│                     Machine Hôte (2.6 Go RAM)               │
│                                                             │
│  ┌────────────────┐    ┌──────────────┐                    │
│  │ Docker Engine   │    │ Python Host  │                    │
│  │                 │    │              │                    │
│  │  ┌──────────┐   │    │  Streamlit   │                    │
│  │  │ Pipeline  │   │    │  Dashboard   │                    │
│  │  │ (spark)   │   │    │  (pyarrow)   │                    │
│  │  └──────────┘   │    └──────┬───────┘                    │
│  │                 │           │                            │
│  │  ┌──────────┐   │           │                            │
│  │  │Thrift    │   │           │                            │
│  │  │Server    │   │           │                            │
│  │  └──────────┘   │           │                            │
│  │                 │           │                            │
│  │  ┌──────────┐   │           │                            │
│  │  │Metabase  │   │           │                            │
│  │  └──────────┘   │           │                            │
│  │                 │           │                            │
│  └─────────────────┘           │                            │
│         │                      │                            │
│         ▼                      ▼                            │
│  ┌────────────────────────────────────┐                     │
│  │         data/gold/ (Parquet)       │                     │
│  │  dim_customer  fact_order          │                     │
│  │  dim_product   fact_payment        │                     │
│  │  dim_date      fact_delivery        │                     │
│  │  dim_location                      │                     │
│  └────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────┘
```

---

## 9. Schéma des Tables Gold

### Dimensions

**`dim_customer`** (SCD Type 2)
| Colonne | Type | Description |
|---------|------|-------------|
| `customer_key` | INT | Clé de substitution |
| `customer_id` | STRING | UUID d'origine |
| `first_name`, `last_name` | STRING | Nom |
| `email` | STRING | Email |
| `loyalty_tier` | STRING | Bronze/Silver/Gold/Platinum |
| `valid_from` | DATE | Début de validité |
| `valid_to` | DATE | Fin de validité (9999-12-31 si actif) |
| `is_current` | BOOLEAN | True si version active |

**`dim_product`**
| Colonne | Type | Description |
|---------|------|-------------|
| `product_key` | INT | Clé de substitution |
| `category` | STRING | Catégorie produit |
| `price` | DOUBLE | Prix unitaire |
| `supplier` | STRING | Fournisseur |

**`dim_date`** (23 013 jours de 2020 à 2026)
| Colonne | Type | Description |
|---------|------|-------------|
| `date_key` | DATE | La date |
| `year` | INT | Année |
| `month` | INT | Mois (1-12) |
| `quarter` | INT | Trimestre (1-4) |
| `is_weekend` | BOOLEAN | True si samedi/dimanche |
| `year_month` | STRING | "2024-01" pour les graphiques |

**`dim_location`**
| Colonne | Type | Description |
|---------|------|-------------|
| `location_key` | INT | Clé de substitution |
| `city` | STRING | Ville |
| `state` | STRING | État |
| `country` | STRING | Pays |

### Faits

**`fact_order`** (centrale, ~3M lignes cumulées)
| Colonne | Type | Description |
|---------|------|-------------|
| `order_id` | STRING | ID commande |
| `customer_key` | INT | → dim_customer |
| `product_key` | INT | → dim_product |
| `date_key` | DATE | → dim_date |
| `quantity` | INT | Quantité commandée |
| `total_amount` | DOUBLE | Montant total |

**`fact_payment`**
| Colonne | Type | Description |
|---------|------|-------------|
| `payment_id` | STRING | ID paiement |
| `order_id` | STRING | → fact_order |
| `amount` | DOUBLE | Montant |
| `payment_method` | STRING | Carte, PayPal, Virement... |
| `payment_status` | STRING | completed/pending/failed |

**`fact_delivery`**
| Colonne | Type | Description |
|---------|------|-------------|
| `delivery_id` | STRING | ID livraison |
| `order_id` | STRING | → fact_order |
| `carrier` | STRING | Transporteur |
| `estimated_days` | INT | Jours estimés |
| `actual_days` | INT | Jours réels |

---

## 10. Exemples de Requêtes Analytiques

Les requêtes ci-dessous peuvent être exécutées dans Streamlit (via pandas) ou dans Metabase (via Spark SQL) :

### Top 10 clients par dépense totale
```sql
SELECT dc.customer_id,
       CONCAT(dc.first_name, ' ', dc.last_name) AS full_name,
       ROUND(SUM(fo.total_amount), 2) AS total_spend
FROM fact_order fo
JOIN dim_customer dc ON fo.customer_key = dc.customer_key
GROUP BY dc.customer_id, dc.first_name, dc.last_name
ORDER BY total_spend DESC
LIMIT 10;
```

### Revenu mensuel
```sql
SELECT CONCAT(dd.year, '-', LPAD(CAST(dd.month AS STRING), 2, '0')) AS month,
       ROUND(SUM(fo.total_amount), 2) AS revenue
FROM fact_order fo
JOIN dim_date dd ON fo.date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;
```

### Revenu par catégorie de produit
```sql
SELECT dp.category, ROUND(SUM(fo.total_amount), 2) AS revenue
FROM fact_order fo
JOIN dim_product dp ON fo.product_key = dp.product_key
GROUP BY dp.category
ORDER BY revenue DESC;
```

### Taux de succès des paiements par méthode
```sql
SELECT payment_method,
       COUNT(*) AS total,
       SUM(CASE WHEN payment_status = 'completed' THEN 1 ELSE 0 END) AS completed,
       ROUND(SUM(CASE WHEN payment_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS success_pct
FROM fact_payment
GROUP BY payment_method
ORDER BY success_pct DESC;
```

### Performance des livraisons par transporteur
```sql
SELECT carrier,
       ROUND(AVG(estimated_days), 1) AS avg_estimated,
       ROUND(AVG(actual_days), 1) AS avg_actual,
       ROUND(AVG(actual_days - estimated_days), 1) AS avg_delay
FROM fact_delivery
GROUP BY carrier
ORDER BY avg_delay;
```
