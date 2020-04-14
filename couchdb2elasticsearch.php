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
$error_nb = 0;
readSeqFile();

while(1) {

    $url = $couchdb_url_db.'/_changes?feed=continuous&include_docs=true&timeout=30000&limit='.intval($COMMITER * 3.5);
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

        //Not views
        if (isset($change->id) && preg_match('/^_/', $change->id)) {
            continue;
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
    global $elastic_url_db, $elastic_buffer, $verbose, $lock_file_path,$error_nb;
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
            $error_nb++;
            if ($error_nb > 100) {
                unlink($lock_file_path);
                throw new Exception("too many errors");
            }
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
    if (preg_match('/\\/', $change->id)) {
        return;
    }
    if ($verbose) echo "deleteIndexer (1) : ".$change->id."\n";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $elastic_url_db."/_search?q=source:".urlencode($change->id));
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

function updateIndexer($change) {
    global $verbose;
    if ($verbose) echo "updateIndexer (1) : ".$change->id."\n";
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
        if (isset($change->doc->contrats)) {
            $contrats = array();
            foreach($change->doc->contrats as $k => $c) {
                $contrats[] = $c;
            }
            $change->doc->contrats = $contrats;
        }
        if (isset($change->doc->totaux)) {
            $produits = array();
            foreach($change->doc->totaux->produits as $k => $p) {
                $produits[] = $p;
            }
            $change->doc->totaux->produits = $produits;
        }
        if (isset($change->doc->mouvements)) {
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
    }
    if ($change->doc->type == "CSVDRM") {
        $erreurs = array();
        foreach($change->doc->erreurs as $id => $e) {
            $e->id = $id;
            $erreurs[] = $e;
        }
        $change->doc->erreurs = $erreurs;
    }
    if ($change->doc->type == "Etablissement") {
        if (isset($change->doc->liaisons_operateurs)) {
            $liaisons = array();
            foreach ($change->doc->liaisons_operateurs as $k => $o) {
                $liaisons[] = $o;
            }
            $change->doc->liaisons_operateurs = $liaisons;
        }
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
    if ($change->doc->type == "Compte") {
        unset($change->doc->infos);
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
        $drmmvt = array("doc" => array("type" => "DRMMVT", "campagne" => $change->doc->campagne,
                        "periode" => $change->doc->periode,  "version" => $change->doc->version,
                        "declarant" => $change->doc->declarant, "identifiant" => $change->doc->identifiant,
                        "mode_de_saisie" => $change->doc->mode_de_saisie, "valide" => $change->doc->valide,
                        "mouvements" => null, "drmid" => $change->doc->_id));
        if (isset($change->doc->region)) {
            $drmmvt["region"]  = $change->doc->region;
        }
        if (isset($change->doc->type_creation)) {
            $drmmvt["type_creation"] = $change->doc->type_creation;
        }
        if (isset($change->doc->teledeclare)) {
            $drmmvt["teledeclare"]  = $change->doc->teledeclare;
        }
        if (isset($change->doc->numero_archive)) {
            $drmmvt["numero_archive"]  = $change->doc->numero_archive;
        }
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
                if (!isset($mvt->date)) {
                    $periode = substr_replace('-', '', $change->doc->periode);
                    $mvt->date = substr($periode, 0, 4).'-'.substr($periode, 4, 2).'-15';
                }
                $drmmvt["doc"]["mouvements"] = $mvt;
                emit($change->id."-".$id, $drmmvt, "DRMMVT", $change->id);
            }
        }
        $mvt = array();
        if (isset($change->doc->region)) {
            $mvt['region'] = $change->doc->region;
            $mvt['region_destinataire'] = $change->doc->region;
        }
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
                                        if (isset($detail->produit_libelle)) {
                                            $mvt['produit_libelle'] = $detail->produit_libelle;
                                        }
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
                                            $mvt['id'] = 'DRM-'.$change->doc->identifiant.'-'.$change->doc->periode.'-'.md5($produit_hash.' '.$kd.' '.$s);
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
        if (isset($change->doc->demandes)) {
            $demandes = array();
            foreach($change->doc->demandes as $key => $d) {
                $d->produit_hash = $key;
                $demandes[] = $d;
            }
            $change->doc->demandes = $demandes;
        }
    }
    if ($change->doc->type == "Parcellaire" || $change->doc->type == "ParcellaireAffectation" || $change->doc->type == "ParcellaireIntentionAffectation"
        || $change->doc->type == "ParcellaireIrrigue" || $change->doc->type == "ParcellaireIrrigable") {
        $declaration = array();
        if (isset($change->doc->declaration->certification)){
            foreach($change->doc->declaration->certification as $kg => $genre) {
                if (preg_match('/^genre/', $kg)) foreach($genre as $ka => $appellation) {
                    if (preg_match('/^appellation/', $ka)) foreach($appellation as $km => $mention) {
                        if (preg_match('/^mention/', $km)) foreach($mention as $kl => $lieu) {
                            if (preg_match('/^lieu/', $kl)) foreach($lieux as $kcoul => $couleurs) {
                                if (preg_match('/^couleur/', $kcoul)) foreach($couleurs as $kcep => $cepage) {
                                    if (preg_match('/^cepage/', $kcep)) {
                                        $produit_hash = '/declaration/certification/'.$kg.'/'.$ka.'/'.$km.'/'.$kl.'/'.$kcoul.'/'.$kcep;
                                        if (!isset($cep->details)) {
                                            continue;
                                        }
                                        foreach($cep->details as $kd => $detail) {
                                            $d->produit_hash = $produit_hash;
                                            $d->detail_hash = $kd;
                                            $declaration[] = $d;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }else {
            foreach($change->doc->declaration as $key => $d) {
                foreach ($d->detail as $dkey => $adetail) {
                    $adetail->produit_hash = $key;
                    $adetail->detail_key = $dkey;
                    $adetail->produit_libelle = $d->libelle;
                    $declaration[] = $adetail;
                }
            }
        }
        $change->doc->declaration = $declaration;
        if (isset($change->doc->acheteurs)){
            $acheteurs = array();
            foreach($change->doc->acheteurs as $type => $as) {
                foreach ($as as $cvi => $a) {
                    $acheteurs[] = $a;
                }
            }
            $change->doc->acheteurs = $acheteurs;
        }

    }
    if ($change->doc->type == "DRev") {
        unset($change->doc->documents);
        $declaration = array();
        if (isset($change->doc->declaration->certification)){
            foreach($change->doc->declaration->certification as $kg => $genre) {
                if (preg_match('/^genre/', $kg)) foreach($genre as $ka => $appellation) {
                    if (preg_match('/^appellation/', $ka)) foreach($appellation as $km => $mention) {
                        if (preg_match('/^mention/', $km)) foreach($mention as $kl => $lieu) {
                            if (preg_match('/^lieu/', $kl)) foreach($lieu as $kcoul => $couleur) {
                                if (preg_match('/^couleur/', $kcoul)) {
                                    if (isset($couleur->vci)) {
                                        $vci = array();
                                        foreach($couleur->vci as $k => $vci) {
                                            $vci[] = $vci;
                                        }
                                        $couleur->vci = $vci;
                                    }
                                    $couleur->produit_hash = '/declaration/certification/'.$kg.'/'.$ka.'/'.$km.'/'.$kl.'/'.$kcoul;
                                    $declaration[] = $couleur;
                                }
                            }
                        }
                    }
                }
            }
        }else {
            foreach($change->doc->declaration as $pkey => $details) {
                foreach($details as $dkey => $d) {
                    $d->produit_hash = $pkey;
                    $d->detail_hash = $pkey;
                    $declaration[] = $d;
                }
            }
        }
        $change->doc->declaration = $declaration;
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
    if ($change->doc->type == "Abonnement") {
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

    if ($change->doc->type == "Tournee") {
        $agents = array();
        foreach($change->doc->agents as $k => $a) {
            $agents[] = $a;
        }
        $change->doc->agents = $agents;
        $degustateurs = array();
        foreach($change->doc->degustateurs as $type => $list) {
            foreach($list as $compteid => $d) {
                $d->compte_id = $compteid;
                $d->degustateur_type = $type;
                $degustateurs[] = $d;
            }
        }
        $change->doc->degustateurs = $degustateurs;
        $degustations = array();
        foreach($change->doc->degustations as $k => $d) {
            $degustations[] = array('id' => $k, "doc" => $d);
        }
        $change->doc->degustations = $degustations;
        $rendezvous = array();
        foreach($change->doc->rendezvous as $k => $r) {
            $r->rendezvous_key = $k;
            $rendezvous[] = $r;
        }
        $change->doc->rendezvous = $rendezvous;
    }

    if ($change->doc->type == "Degustation") {
        $lots = array();
        foreach($change->doc->lots as $k => $l) {
            $l->key = $k;
            $lots[] = $l;
        }
        $change->doc->lots = $lots;
    }

    if ($change->doc->type == "Tirage") {
        unset($change->doc->documents);
    }

    if ($change->doc->type == 'Constats') {
        $constats = array();
        foreach($change->doc->constats as $k => $c) {
            $c->constat_key = $k;
            $constats[] = $c;
        }
        $change->doc->constats = $constats;
    }

    emit($change->id, $change, strtoupper($change->doc->type));
}
