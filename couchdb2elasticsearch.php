<?php

include("config.php");

$elastic_buffer = array();
$cpt = 0;
readLockFile();

while(1) {

    $url = $couchdb_url_db.'/_changes?feed=continuous&include_docs=true&timeout=30000&limit='.$COMMITER;
    if (isset($last_seq) && $last_seq) {
        $url .= '&since='.$last_seq;
    }
    //Récupère les derniers changements
    $changes = fopen($url, 'r');
    echo "$url\n";

    $cpt = 0;
    //Pour chaque changement, on récupére le document couchdb
    while($changes && ($l = fgets($changes))) {
        $cpt++;

        //Decode le json fourni par couchdb
        $change = json_decode($l);
        if (!$change) {
            echo "pb json : $l\n";
            continue;
        }

        //Si un doc couchdb a un last_seq, c'est qu'il nous demande de forcer la sequence
        //Et on repartira de là
        if (isset($change->last_seq)) {
            storeSeq($change->last_seq);
            break;
        }

        $last_seq = $change->seq;
        //Suppression si le doc a été supprimé par couchdb
        if (isset($change->deleted)) {
            deleteIndexer($change);
            continue;
        }
        //Sinon insère ou met à jour le document
        updateIndexer($change);

        //Si on n'a inséré $COMMITER docs, on commit et sauve cette valeur
        if ($cpt > $COMMITER) {
            storeSeq($last_seq);
        }

    }
    if ($changes) {
        fclose($changes);
        $changes = null;
    }
    storeSeq($last_seq);
}
fclose($lock);

//Commit les données puis sauve
function storeSeq($seq) {
  echo "storeSequence (1) : $seq\n";
  global $lock, $cpt, $lock_seq_file;
  if (commitIndexer()) {
    fclose($lock);
    $lock = fopen($lock_seq_file, 'w+');
    fwrite($lock, $seq."\n");
    $cpt = 0;
  }
}

function readLockFile() {
  global $last_seq, $lock_seq_file, $lock, $changes;
  $lock = fopen($lock_seq_file, 'a+');
  if (!$lock) die('error with lock file');
  fseek($lock, 0);
  $last_seq = rtrim(fgets($lock));
  //On s'assure qu'on revient au last_seq sauvé
  if ($changes) {
    fclose($changes);
    $changes = null;
  }
  return $last_seq;
}

function commitIndexer() {
    global $elastic_url_db, $elastic_buffer;
    if (!count($elastic_buffer)) {
        return true;
    }
    $data = implode("\n", $elastic_buffer);
    echo "commitIndexer\n";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $elastic_url_db."/_bulk");
    curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: application/json','Content-Length: ' . strlen($data)));
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT');
    curl_setopt($ch, CURLOPT_POSTFIELDS,$data);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    $response  = curl_exec($ch);
    $json_response = json_decode($response);
    if (!isset($json_response->errors)) {
        echo "json : no error ";
        print_r($json_response);
        echo "\n";
        throw new Exception("bad response (indexer): network problem ?");
    }elseif ($json_response->errors) {
        echo "ERROR : ";
        print_r($response); echo "\n";
    }
    curl_close($ch);
    $elastic_buffer = array();
    return true;
}

function updateIndexer($change) {
    echo "updateIndexer (1) : ".$change->id."\n";
    //Not views
    if (isset($change->doc->views)) {
        return ;
    }
    unset($change->doc->_attachments);
    unset($change->doc->droits);
    if ($change->doc->type == "Facture") {
        unset($change->doc->lignes);
        unset($change->doc->origines);
    }
    if ($change->doc->type == "Generation") {
        unset($change->doc->fichiers);
    }
    if ($change->doc->type == "SV12") {
        $contrats = array();
        foreach($change->doc->contrats as $k => $c) {
            $contrats[] = $c;
        }
        $change->doc->contrats = $contrats;
        $produits = array();
        foreach($change->doc->totaux->produits as $k => $p) {
            $produits[] = $p;
        }
        $change->doc->totaux->produits = $produits;
    }
    if ($change->doc->type == "DRM") {
        unset($change->doc->declaration->certifications);
        unset($change->doc->favoris);
        $mouvements = array();
        $drmmvt = array("doc" => array("type" => "DRMMVT", "region" => $change->doc->region , "campagne" => $change->doc->campagne, "type_creation"=> $change->doc->type_creation,
                        "periode" => $change->doc->periode, "teledeclare" => $change->doc->teledeclare, "version" => $change->doc->version, "numero_archive" => $change->doc->numero_archive,
                        "declarant" => $change->doc->declarant, "identifiant" => $change->doc->identifiant, "mode_de_saisie" => $change->doc->mode_de_saisie, "valide" => $change->doc->valide,
                        "mouvements" => null, "drmid" => $change->doc->_id));
        foreach($change->doc->mouvements as $tiers => $t_mouvements) {
            foreach($t_mouvements as $id => $mvt) {
                $mouvements[] = $mvt;
                $keys = explode('/', $mvt->produit_hash);
                $mvt->certification = $keys[3];
                $mvt->genre = $keys[5];
                $mvt->appellation = $keys[7];
                $mvt->mention = $keys[9];
                $mvt->lieu = $keys[11];
                $mvt->couleur = $keys[13];
                $mvt->cepage = $keys[15];
                $mvt->id = $change->id."-".$id;
                $mvt->date = substr($change->doc->periode, 0, 4).'-'.substr($change->doc->periode, 4, 2).'-15';
                $drmmvt["doc"]["mouvements"] = $mvt;
                emit($change->id."-".$id, $drmmvt, "DRMMVT");
            }
        }
        $change->doc->mouvements = $mouvements;
        if (isset($change->doc->crds)) {
            $crds = array();
            foreach($change->doc->crds as $type => $t_crds) {
                foreach($t_crds as $id => $crd) {
                    $crds[] = $crd;
                }
            }
            $change->doc->crds = $crds;
        }
    }
    emit($change->id, $change, strtoupper($change->doc->type));
}
function emit($id, $object, $type) {
    global $elastic_buffer;
    $data_json = json_encode($object);
    $header = '{ "index" : { "_type" : "'.$type.'", "_id" : "'.$id.'" } }';
    $elastic_buffer[] = $header."\n".$data_json;
}

function deleteIndexer($change) {
    global $elastic_url_db, $elastic_buffer;
    echo "deleteIndexer (1) : ".$change->id."\n";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $elastic_url_db."/_search?q=id:".$change->id);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    $result = curl_exec($ch);
    $json = json_decode($result);
    curl_close($ch);
    if (!$json || !isset($json->hits)) {
        throw new Exception("bad response (delete) : network problem ?");
    }
    if(isset($json->hits->hits[0]) && ($json->hits->hits[0]->_id == $change->id)) {
        $elastic_buffer[] = '{ "delete" : { "_type" : "'.$json->hits->hits[0]->doc->type.'", "_id" : "'.$change->id.'" } }'."\n";
    }
}
