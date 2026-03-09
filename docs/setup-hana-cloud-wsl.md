
## Bug : $select cause 404 sur sandbox SAP
- Tout appel OData avec $select retourne 404 sur la sandbox gratuite
- Fix : ne pas utiliser $select, récupérer tous les champs
- La pagination via __next ne fonctionne pas sur le plan sandbox gratuit (50 rows max)
