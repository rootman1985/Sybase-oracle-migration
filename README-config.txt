Pour une nouvelle table, ajoutez une ligne dans tables.csv.

Exemple :
dbo,MA_TABLE,REPORTING,MA_TABLE,IdMaTable,true

Le moteur récupère les colonnes par JDBC. La première colonne est utilisée comme
ID si id_column est vide.

Si les noms ne correspondent pas, ajoutez-les dans column-mappings.csv :
MA_TABLE,NomSource,NOM_CIBLE

Les noms sont comparés sans tenir compte de la casse ni des underscores.
