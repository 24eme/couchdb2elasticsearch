Pour recréer la base elastic search pour l'exemple region_prod.
Aller sur http://x.x.x.x:9200 et supprimer la base d'elasticsearch region_prod

Puis aller dans le répertoire du projet pour supprimer le fichier seq :

    cd couchdb2elasticsearch
    rm seqs/region_prod.seq

on stoppe les indexations en cours puis on supprime les fichiers lock dans le même temps : 

    bash bin/kill_couchdb2elasticsearch.sh | while read lock; do rm "$lock"; done;

puis le crontab reprends et la réindexation se relance
