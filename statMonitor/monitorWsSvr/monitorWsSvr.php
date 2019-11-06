<?php
use Workerman\Worker;
use \Workerman\Connection\AsyncTcpConnection;
require_once __DIR__ . '/Workerman/Autoloader.php';
require_once __DIR__ . '/Workerman/vendor/autoload.php';
require_once "system/load_config.php";
require_once 'system/log/log.php';
require_once 'system/Util.php';

$load_config = new load_config();
$config = $load_config->fc_load_config("system/conf/config.ini");

//数据库连接
$db = 1;


//ws业务服务进程组///////////////////////////////////////

$ws_worker = new Worker("websocket://0.0.0.0:23400");

// 启动4个进程对外提供服务
$ws_worker->count = $config['worker_count'];


$ws_worker->onWorkerStart = function($worker)
{
    // 将db实例存储在全局变量中(也可以存储在某类的静态成员中)
    global $db;
    global $config;

    $dbHost = $config['db_ip'];
    $dbPort = $config['db_port'];
    $dbUser = $config['db_user'];
    $dbPass = $config['db_psw'];
    $dbName = $config['db_name'];

    $m = new MongoClient("mongodb://" . $dbHost . ":" . $dbPort);
    $db = $m->$dbName;
    if($db == null){
        log_message("ERROR", "db connect failed");
    }
};
//
$ws_worker->onMessage = function($connection, $data)
{
//    log_message("DEBUG", $data);

    $reqData = json_decode($data, true);
    msgProc($connection, $reqData);
};


// 运行worker
Worker::runAll();



//
function msgProc($connection, $reqData){


    # 根据不同命令字转入对应处理函数
    if($reqData['cmd'] == 'getCurStatPoint'){
        cmdGetCurStatPointProc($connection, $reqData);
    }else{
        log_message('ERROR', "bad cmd=[" . $reqData['cmd'] . "]");
    }

    //
    $connection->close();
}


function cmdGetCurStatPointProc($connection, $req){

    // 将db实例存储在全局变量中(也可以存储在某类的静态成员中)
    global $db;
    global $config;

    while(true) {
        $collection = $db->cur_svr_stat_point; // 选择集合
        $cursor = $collection->find();
        $statArr = array();
        foreach ($cursor as $document) {
            array_push($statArr, $document);
        }

        $rspData['protVer'] = 0.1;
        $rspData['cmd'] = 'getCurStatPointRsp';
        $rspData['errCode'] = "success";
        $rspData['statArr'] = $statArr;
        $rspStr = json_encode($rspData);
        if($connection->send($rspStr) == false){
            break;
        }

        sleep(5);
    }
}