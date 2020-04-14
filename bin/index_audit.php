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
$url = $couchdb_url_db.'/_changes';
if (isset($last_seq) && $last_seq) {
    $url .= '&since='.$last_seq;
}
//Récupère les derniers changements
$changes = fopen($url, 'r');
if ($verbose) echo "$url\n";

$cpt = 0;
$errors = 0;
//Pour chaque changement, on récupére le document couchdb
while($changes && ($l = fgets($changes))) {
    if(!str_replace("\n", "", $l)) {
        if ($verbose) echo "Empty response\n";
        continue;
    }
    $l = preg_replace('/,$/', '', $l);
    if (preg_match('/^(."results":.|.|"last_seq":[0-9]*.)$/', $l)) {
        continue;
    }

    //Decode le json fourni par couchdb
    $change = json_decode($l);
    if (isset($change->id) && preg_match('/^_/', $change->id)) {
        continue;
    }
    if (isset($change->id) && preg_match('/^(CONFIGURATION|CURRENT|COMPTABILITE)/', $change->id)) {
        continue;
    }
    if (!$change || !$change->id) {
        echo "ERROR : pb json : $l\n";
        continue;
    }
    $test_deleted = (isset($change->deleted) && $change->deleted);
    $json = query($change, $test_deleted);
    $cpt++;
    if ($test_deleted && isset($json->hits) && $json->hits && isset($json->hits->total) && $json->hits->total) {
        echo "PB: ".$change->id." should be deleted\n";
        $errors++;
    }elseif(!$test_deleted && (!isset($json->hits) || !$json->hits || !isset($json->hits->total) || !$json->hits->total)) {
        echo "PB: ".$change->id." should be indexed\n";
        $errors++;
    }elseif($verbose){
        echo "OK: ".$change->id;
        echo ($test_deleted) ? ' DELETED' : '';
        echo "\n";
    }
}
echo "Errors : ".$errors."/".$cpt."\n";

function query($change, $test_deleted) {
    global $elastic_url_db;
    $ch = curl_init();
    if ($test_deleted) {
        curl_setopt($ch, CURLOPT_URL, $elastic_url_db."/_search?q=source:".urlencode($change->id));
    }else{
        curl_setopt($ch, CURLOPT_URL, $elastic_url_db."/_search?q=id:".urlencode($change->id));
    }
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    $result = curl_exec($ch);
    $json = json_decode($result);
    curl_close($ch);
    return $json;
}
