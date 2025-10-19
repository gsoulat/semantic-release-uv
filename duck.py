import os

import duckdb

# Définir le chemin de la base de données locale
chemin_bdd = "my_bdd.duckdb"

# Créer/Se connecter à la base de données locale
conn = duckdb.connect(chemin_bdd)

print(f"Base de données créée/ouverte : {chemin_bdd}")
print(
    f"Taille du fichier : {os.path.getsize(chemin_bdd) if os.path.exists(chemin_bdd) else 0} octets"
)

# Créer plusieurs tables
conn.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY,
        nom VARCHAR,
        email VARCHAR,
        date_inscription DATE
    )
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS commandes (
        id INTEGER PRIMARY KEY,
        client_id INTEGER,
        montant DECIMAL(10,2),
        date_commande DATE,
        FOREIGN KEY (client_id) REFERENCES clients(id)
    )
""")

# Insérer des données
conn.execute("""
    INSERT INTO clients VALUES 
    (1, 'Dupont', 'dupont@email.com', '2024-01-15'),
    (2, 'Martin', 'martin@email.com', '2024-02-20'),
    (3, 'Bernard', 'bernard@email.com', '2024-03-10')
    ON CONFLICT DO NOTHING
""")

conn.execute("""
    INSERT INTO commandes VALUES 
    (1, 1, 150.50, '2024-04-01'),
    (2, 1, 200.00, '2024-04-15'),
    (3, 2, 75.25, '2024-04-10'),
    (4, 3, 300.00, '2024-04-20')
    ON CONFLICT DO NOTHING
""")

# Vérifier les tables créées
print("\nTables dans la base de données:")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    print(f"  - {table[0]}")

# Quelques requêtes
print("\nClients:")
print(conn.execute("SELECT * FROM clients").fetchdf())

print("\nCommandes par client:")
print(
    conn.execute("""
    SELECT c.nom, COUNT(co.id) as nb_commandes, SUM(co.montant) as total
    FROM clients c
    LEFT JOIN commandes co ON c.id = co.client_id
    GROUP BY c.nom
""").fetchdf()
)

# Commit et fermer
conn.commit()
conn.close()

print(f"\n✓ Base de données sauvegardée dans : {chemin_bdd}")
print(f"✓ Taille finale : {os.path.getsize(chemin_bdd)} octets")
