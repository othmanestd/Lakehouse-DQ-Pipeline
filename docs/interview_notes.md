# Lakehouse-DQ-Pipeline - Notes pour Entretien

## Pitch (30 secondes)
"J'ai construit une architecture Lakehouse Bronze/Silver/Gold sur Databricks
pour traiter des transactions e-commerce. Le point fort du projet est le
framework de Data Quality custom que j'ai developpe : il valide chaque batch
avec des checks configurables (not_null, unique, positive, range, values_in_set)
et genere un rapport HTML automatique. Les records rejetes sont quarantines
dans un layer separe pour investigation."

## Questions frequentes

### "Pourquoi un framework DQ custom plutot que Great Expectations ?"
Great Expectations est excellent en production, mais pour ce projet j'ai
voulu montrer ma comprehension des patterns sous-jacents. Mon framework
est plus leger et integre nativement avec PySpark/Delta Lake. En entreprise,
j'utiliserais Great Expectations ou Soda Core.

### "Expliquez l'architecture Bronze/Silver/Gold."
- Bronze = donnees brutes, aucune transformation, append-only. On garde tout.
- Silver = donnees nettoyees : dedup, types corriges, DQ checks, enrichissement.
- Gold = agregations business-ready : revenue, CLV, product performance.
Chaque couche est en Delta Lake avec son propre schema et partitionnement.

### "Comment gerez-vous les donnees rejetees ?"
Les records qui echouent aux DQ checks critiques (null transaction_id,
quantity negative) vont dans un layer 'quarantine' separe. Pas de perte
de donnees. L'equipe data peut investiguer et reprocesser si necessaire.

### "Comment partitionnez-vous les donnees ?"
Silver partitionne par order_month (cardinalite moyenne, ~6-12 partitions).
Gold partitionne par order_date_key (pour les dashboards daily).
Jamais par customer_id (cardinalite trop haute = small file problem).

### "Comment testez-vous la qualite des donnees ?"
Tests unitaires pytest avec SparkSession locale. Chaque fonction DQ
est testee independamment avec des DataFrames construits en memoire.
Les tests couvrent les cas nominaux et les edge cases (nulls, doublons,
valeurs hors range).
