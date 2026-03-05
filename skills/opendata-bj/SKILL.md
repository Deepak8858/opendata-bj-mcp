# Skill: Benin Open Data Navigation

## Description
Directives pour la recherche et la consommation des données publiques du Bénin via le portail `donneespubliques.gouv.bj`.

## Outils disponibles
- `rechercher_datasets`: Point d'entrée principal pour découvrir les données.
- `consulter_dataset`: Permet d'obtenir les URLs de téléchargement et les descriptions détaillées.
- `lister_organisations`: Filtre les recherches par institution officielle.
- `publier_datasets_bulk`: Outil d'administration pour l'upload de masse.

## Bonnes Pratiques
- **Toujours rechercher avant de consulter** : Utiliser `rechercher_datasets` pour trouver l'ID précis avant d'appeler `consulter_dataset`.
- **Vérifier les formats** : Toujours vérifier si la ressource est en CSV, PDF ou JSON avant de proposer un traitement.
- **Respecter les limites** : Ne pas surcharger l'API du gouvernement avec des requêtes trop fréquentes.
