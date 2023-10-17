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

### What is Mastodon ?
Mastodon is an open-source social media.
It offers a comprehensive API (Application Programming Interface) that allows developers to interact with various aspects of the platform. Here's a brief summary of what the Mastodon API encompasses:

- Mastodon's API provides secure authentication methods, allowing developers to implement secure user authentication and access control.

- The API enables the management of user accounts, including user profile data, preferences, and settings.

- Developers can create, retrieve, and manage toots (Mastodon's equivalent of tweets) through the API, including features like posting, fetching, and deleting toots.

- The API allows access to notifications, enabling developers to fetch and manage notifications such as mentions, likes, and reposts.

- Mastodon's API provides access to various timelines, including the home timeline, local timeline, and federated timeline, allowing developers to retrieve and interact with posts from different timelines.

- The API facilitates interactions between users, including following/unfollowing users, liking toots, and reposting (boosting) content.

- Mastodon's API supports search functionality, enabling users to search for specific content, users, or hashtags within the Mastodon network.

- The API offers streaming capabilities, allowing developers to implement real-time updates for activities such as new toots, notifications, and other interactions.

- Mastodon's API provides information about instances and federation, enabling developers to retrieve data about instances, their policies, and the federated network of instances.