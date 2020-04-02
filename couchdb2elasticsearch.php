<?php

if (!isset($argv[1]) || !preg_match('/config.*php$/', $argv[1])) {
    echo "ERROR: config file missing in arguments\n";
    echo "\n";
    echo "USAGE : ".$argv[0]." <a_config_file.php>\n";
    echo "\n";
    exit(1);
}
$verbose = 0;
if (isset($argv[2])) {
    $verbose = 1;
}
$config_file = $argv[1];
include($config_file);
$lock_file_path = "/tmp/couchdb2elasticsearch_".$config_file.".lock";

if (!isset($couchdb_url_db) || !isset($elastic_url_db) || !isset($seq_file_path) || !isset($COMMITER) ){
    echo "ERROR : config variable missing.\n";
    echo "Check your configuration file\n";
    exit(2);
}

if (file_exists($lock_file_path)) {
    if ($verbose) echo "ERROR : indexer already running for $lock_file_path\n";
    exit(3);
}
touch($lock_file_path);

$elastic_buffer = array();
$noactivity = 0;
readSeqFile();

while(1) {

    $url = $couchdb_url_db.'/_changes?feed=continuous&include_docs=true&timeout=30000&limit='.($COMMITER * 3.5);
    if (isset($last_seq) && $last_seq) {
        $url .= '&since='.$last_seq;
    }
    //Récupère les derniers changements
    $changes = fopen($url, 'r');
    if ($verbose) echo "$url\n";

    $cpt = 0;
    //Pour chaque changement, on récupére le document couchdb
    while($changes && ($l = fgets($changes))) {
        if(!str_replace("\n", "", $l)) {
            if ($verbose) echo "Empty response\n";
            continue;
        }
        //Decode le json fourni par couchdb
        $change = json_decode($l);
        if (!$change) {
            echo "ERROR : pb json : $l\n";
            continue;
        }

        //Si change a last_seq, c'est qu'on est en timeout
        //On s'arrête et on repartira de là
        if (isset($change->last_seq)) {
            storeSeq($change->last_seq);
            break;
        }

        $cpt++;
        $noactivity = 0;

        $last_seq = $change->seq;
        if (isset($change->deleted)) {
            //Suppression si le doc a été supprimé par couchdb
            deleteIndexer($change);
        }else {
            //Sinon insère ou met à jour le document
            updateIndexer($change);
        }

        //Si on n'a inséré $COMMITER docs, on commit et sauve cette valeur
        if (!($cpt % $COMMITER)) {
            storeSeq($last_seq);
        }

    }
    if ($changes) {
        fclose($changes);
        $changes = null;
    }
    if (!$cpt) {
        $noactivity++;
        if ($verbose) echo "NO activity: $noactivity\n";
        if (!($noactivity % 10)) {
            break;
        }
        continue;
    }
    storeSeq($last_seq);
}
fclose($seqfile);
unlink($lock_file_path);

//Commit les données puis sauve
function storeSeq($seq) {
  global $seqfile, $cpt, $seq_file_path, $verbose;
  if ($verbose) echo "storeSequence (1) : $seq ($cpt)\n";
  if (commitIndexer()) {
    fclose($seqfile);
    $seqfile = fopen($seq_file_path, 'w+');
    fwrite($seqfile, $seq."\n");
    $cpt = 0;
  }
}

function readSeqFile() {
  global $last_seq, $seq_file_path, $seqfile, $changes, $lock_file_path;
  $seqfile = fopen($seq_file_path, 'a+');
  if (!$seqfile) {
      unlink($lock_file_path);
      die("ERROR : could no open lock file $seq_file_path\n");
  }
  fseek($seqfile, 0);
  $last_seq = rtrim(fgets($seqfile));
  //On s'assure qu'on revient au last_seq sauvé
  if ($changes) {
    fclose($changes);
    $changes = null;
  }
  return $last_seq;
}

function commitIndexer() {
    global $elastic_url_db, $elastic_buffer, $verbose, $lock_file_path;
    if (!count($elastic_buffer)) {
        return true;
    }
    $data = implode("\n", $elastic_buffer)."\n";
    if ($verbose) echo "commitIndexer\n";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $elastic_url_db."/_bulk");
    curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: application/json','Content-Length: ' . strlen($data)));
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT');
    curl_setopt($ch, CURLOPT_POSTFIELDS,$data);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    $response  = curl_exec($ch);
    $json_response = json_decode($response);
    if ($verbose) echo "RESPONSE : ".$response;
    if (!isset($json_response->errors)) {
        echo "ERROR json : ";
        print_r($json_response);
        echo "\nDATA :";
        print_r($data);
        echo "\n";
        unlink($lock_file_path);
        throw new Exception("bad response (indexer): network problem ?");
    }elseif ($json_response->errors) {
        if (count($elastic_buffer) == 1) {
            echo "ERROR : ";
            print_r($response);
            echo "\nDATA :";
            print_r($data);
            echo "\n";
        }else{
            $buffers = $elastic_buffer;
            foreach($buffers as $b) {
                $elastic_buffer = array($b);
                commitIndexer();
            }
        }
    }
    curl_close($ch);
    $elastic_buffer = array();
    return true;
}

function updateIndexer($change) {
    global $verbose;
    if ($verbose) echo "updateIndexer (1) : ".$change->id."\n";
    //Not views
    if (preg_match('/^_/', $change->id)) {
        return ;
    }
    if (!isset($change->doc->type)) {
        echo "ERROR: no type for : ";
        print_r($change);
        echo "\n";
        return ;
    }
    if ($change->doc->type == "Configuration") {
        return ;
    }
    if ($change->doc->type == "Current") {
        return ;
    }
    if ($change->doc->type == "Comptabilite") {
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
    if ($change->doc->type == "Annuaire") {
        foreach(array('caves_cooperatives', 'commerciaux', 'negociants', 'recoltants', 'representants') as $type) {
            $types = array();
            foreach($change->doc->{$type} as $id => $raison)  {
                $types[] = array('id' => $id, 'raison_sociale'=> $raison);
            }
            $change->doc->{$type} = $types;
        }
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
        $mouvements = array();
        foreach($change->doc->mouvements as $tiers => $t_mouvements) {
            foreach($t_mouvements as $id => $mvt) {
                $mvt->tiers = $tiers;
                $mvt->id = $id;
                $mouvements[] = $mvt;
            }
        }
        $change->doc->mouvements = $mouvements;
    }
    if ($change->doc->type == "CSVDRM") {
        $erreurs = array();
        foreach($change->doc->erreurs as $id => $e) {
            $e->id = $id;
            $erreurs[] = $e;
        }
        $change->doc->erreurs = $erreurs;
    }
    if ($change->doc->type == "Societe") {
        $contacts = array();
        foreach($change->doc->contacts as $id => $c) {
            $c->id = $id;
            $contacts[] = $c;
        }
        $change->doc->contacts = $contacts;
        $etablissements = array();
        foreach($change->doc->etablissements as $id => $e) {
            $e->id = $id;
            $etablissements[] = $e;
        }
        $change->doc->etablissements = $etablissements;
    }
    if ($change->doc->type == "MouvementsFacture") {
        $mouvements = array();
        foreach($change->doc->mouvements as $tiers => $t_mouvements) {
            foreach($t_mouvements as $id => $mvt) {
                $mvt->tiers = $tiers;
                $mvt->id = $id;
                $mouvements[] = $mvt;
            }
        }
        $change->doc->mouvements = $mouvements;
    }
    if ($change->doc->type == "Vrac") {
        $keys = explode('/', $change->doc->produit);
        if (!count($keys)) {
            echo "ERROR : no product keys found : ";
            print_r($change);
            echo "\n";
            return;
        }
        $change->doc->certification = $keys[3];
        $change->doc->genre = $keys[5];
        $change->doc->appellation = $keys[7];
        $change->doc->mention = $keys[9];
        $change->doc->lieu = $keys[11];
        $change->doc->couleur = $keys[13];
        $change->doc->cepage = $keys[15];
    }
    if ($change->doc->type == "DRM") {
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
                emit($change->id."-".$id, $drmmvt, "DRMMVT", $change->id);
            }
        }
        $mvt = array();
        $mvt['region'] = $change->doc->region;
        $mvt['region_destinataire'] = $change->doc->region;
        $mvt['date'] = substr($change->doc->periode, 0, 4).'-'.substr($change->doc->periode, 4, 2).'-15';
        $mvt['date_version'] = $change->doc->valide->date_saisie;
        foreach($change->doc->declaration->certifications as $kc => $c) {
            foreach($c->genres as $kg => $g) {
                foreach($g->appellations as $ka => $a) {
                    foreach($a->mentions as $km => $m) {
                        foreach($m->lieux as $kl => $l) {
                            foreach($l->couleurs as $kcoul => $coul) {
                                foreach($coul->cepages as $kcep => $cep) {
                                    $produit_hash = '/declaration/certifications/'.$kc.'/genres/'.$kg.'/appellations/'.$ka.'/mentions/'.$km.'/lieux/'.$kl.'/couleurs/'.$kcoul.'/cepages\/'.$kcep;
                                    if (!isset($cep->details)) {
                                        continue;
                                    }
                                    foreach($cep->details as $kd => $detail) {
                                        $mvt['categorie'] = 'stocks';
                                        $mvt['produit_hash'] = $produit_hash;
                                        $mvt['produit_libelle'] = $detail->produit_libelle;
                                        $mvt['type_drm'] = 'SUSPENDU';
                                        $mvt['type_drm_libelle'] = 'Suspendu';
                                        $mvt['vrac_numero'] = null;
                                        $mvt['vrac_destinataire'] = null;
                                        $mvt['detail_identifiant'] = null;
                                        $mvt['detail_libelle'] = null;
                                        $mvt['certification'] = $kc;
                                        $mvt['genre'] = $kg;
                                        $mvt['appellation'] = $ka;
                                        $mvt['mention'] =  $km;
                                        $mvt['lieu'] =  $kl;
                                        $mvt['couleur'] =  $kcoul;
                                        $mvt['cepage'] =  $kcep;
                                        $mvt['cvo'] = $detail->cvo->taux;
                                        foreach(array('total', 'total_debut_mois') as $s) {
                                            $mvt['id'] = 'DRM-'.$change->doc->identifiant.'-'.$change->doc->periode.'-'.md5($detail->produit_libelle.' '.$s);
                                            $mvt['type_hash'] = $s;
                                            $mvt['type_libelle'] = $s;
                                            $mvt['volume'] = $detail->{$s};
                                            $drmmvt["doc"]["mouvements"] = $mvt;
                                            emit($mvt['id'], $drmmvt, "DRMMVT", $change->id);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        unset($change->doc->declaration->certifications);
        $change->doc->mouvements = $mouvements;
        if (isset($change->doc->crds)) {
            $crds = array();
            foreach($change->doc->crds as $type => $t_crds) {
                foreach($t_crds as $id => $crd) {
                    $crd->id = $id;
                    $crds[] = $crd;
                }
            }
            $change->doc->crds = $crds;
        }
        if (isset($change->doc->releve_non_apurement)) {
            $releve_non_apurements = array();
            foreach($change->doc->releve_non_apurement as $id => $r) {
                    $r->id = $id;
                    $releve_non_apurements[] = $r;
            }
            $change->doc->releve_non_apurement = $releve_non_apurements;
        }
    }
    if ($change->doc->type == "Habilitation") {
        $declaration = array();
        foreach($change->doc->declaration as $key => $d) {
            $d->produit_hash = $key;
            $declaration[] = $d;
        }
        $change->doc->declaration = $declaration;
    }
    if ($change->doc->type == "DRev") {
        $declaration = array();
        foreach($change->doc->declaration as $pkey => $details) {
            foreach($details as $dkey => $d) {
                $d->produit_hash = $pkey;
                $d->detail_hash = $dkey;
                $declaration[] = $d;
            }
        }
        $change->doc->declaration = $declaration;
    }
    if ($change->doc->type == "DR" || $change->doc->type == "SV11" || $change->doc->type == "SV12") {
        if (isset($change->doc->mouvements)) {
            $mouvements = array();
            foreach($change->doc->mouvements as $tkey => $tiers) {
                foreach($tiers as $mkey => $mvt) {
                    $mvt->mouvement_key = $mkey;
                    $mvt->tiers_key = $tkey;
                    $mouvements[] = $mvt;
                }
            }
            $change->doc->mouvements = $mouvements;
        }
    }
    emit($change->id, $change, strtoupper($change->doc->type));
}
function emit($id, $object, $type, $origin = null) {
    global $elastic_buffer, $verbose;
    if ($verbose) echo "emit($id)\n";
    if (!$origin) {
        $origin = $id;
    }
    if (is_array($object)) {
        $object["source"] = $origin;
        $object["id"] = $id;
    }else{
        $object->source = $origin;
        $object->id = $id;
    }
    $data_json = json_encode($object);
    $header = '{ "index" : { "_type" : "'.$type.'", "_id" : "'.$id.'" } }';
    $elastic_buffer[] = $header."\n".$data_json;
}

function deleteIndexer($change) {
    global $elastic_url_db, $elastic_buffer, $verbose, $lock_file_path;
    if ($verbose) echo "deleteIndexer (1) : ".$change->id."\n";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $elastic_url_db."/_search?q=source:".$change->id);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    $result = curl_exec($ch);
    $json = json_decode($result);
    curl_close($ch);
    if (!$json || !isset($json->hits)) {
        echo "ERROR elastic: ";
        print_r($result);
        echo "\nURL: ".$elastic_url_db."/_search?q=source:".$change->id."\n";
        unlink($lock_file_path);
        throw new Exception("bad response (search for delete) : network problem ?");
    }
    foreach($json->hits->hits as $hit) {
        if (!isset($hit->source) || ($hit->source != $change->id)) {
            continue;
        }
        $elastic_buffer[] = '{ "delete" : { "_type" : "'.$hit->doc->type.'", "_id" : "'.$hit->id.'" } }'."\n";
    }
}
