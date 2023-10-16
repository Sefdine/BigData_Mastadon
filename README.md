# BigData_Mastadon

## Analyse des interactions utilisateur sur Mastodon : Utilisation de MapReduce, HBase et Apache Airflow

**Description :** Ce projet vise à analyser en profondeur les données d'interaction des utilisateurs sur la plateforme *Mastodon*, un réseau social décentralisé à code source ouvert. En utilisant la puissance de MapReduce pour le traitement distribué des données, nous visons à extraire des informations pertinentes sur les schémas d'engagement des utilisateurs, la popularité du contenu et d'autres métriques clés. Les données traitées seront stockées de manière efficace dans HBase, une base de données NoSQL hautement évolutive, offrant une gestion efficace des données non structurées. Pour garantir un flux de travail fluide et efficace, nous intégrerons Apache Airflow, une plateforme de gestion des flux de travail et d'orchestration, permettant ainsi une automatisation robuste et une surveillance en temps réel du processus d'analyse des données. Cette approche holistique permettra une compréhension approfondie des comportements des utilisateurs sur Mastodon, ouvrant ainsi la voie à des insights significatifs pour les responsables marketing et les développeurs de produits.

### Objectifs du projet :
On peut classer les objectifs dans deux catégories :

- Analyser l'engagement des utilisateurs

    + Identifier les utilisateurs ayant le plus d'abonnés
    + Calculer le taux d'engagement des utilisateurs
    + Étudier la croissance des utilisateurs au fil du temps
    + Recenser les utilisateurs mentionnés dans les balises les plus utilisées
- Identifier la popularité du contenu

    + Identifier les sites web externes les plus partagés
    + Répartir les publications en fonction de leur langue
    + Compter le nombre de publications avec des contenus multimédias joints
    + Identifier les balises les plus fréquemment utilisées