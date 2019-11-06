
//这些事提供给外部使用的数据结构////////////////////////////////////////////////

class GameMotorEvent extends egret.Event {
    public static GAME_MOTOR: string = 'GAME_MOTOR_EVENT';
    public gmData: Object;

    public constructor(bubbles:boolean=false, cancelable:boolean=false)
    {
        super(GameMotorEvent.GAME_MOTOR,bubbles,cancelable);
    }
}

//以下是内部控制用的数据结构/////////////////////////////////////////////


//当前协议版本
let gProtocolVer = 0.1;


class NetworkMotor extends egret.EventDispatcher{
    ///////////////////////////////////////////////////
    //
    // private gameServerHost = "122.152.221.92";
    // private gameServerPort = 23469;
    private gameServerUrl = "";

    private gameServerSocket: egret.WebSocket;

    public constructor(listener: any){
        super();
        this.gameServerSocket = new egret.WebSocket();
        //回调的监听器
        this.gameEventListenerObj = listener;

        {
            this.gameServerUrl = "ws://wxgate01.5maogame.com:23400";
        }
    }


    //外部接口////////////////////////////////////////////////////////////////////////////////////////////////////

    public closeMotor(){
        this.closeConnet();
    }

    //游戏服务器事件相关回调对象
    private gameEventListenerObj: any = undefined;
    //异步接口
    public cmdGetCurStatPoint(){
        this.getCurStatPoint();
    }


    //内部函数//////////////////////////////////////////////////////////////////////////////
    
    //客户端请求包缓存队列。所有请求包都一律先入队列，等待轮询发送。
    private clientCmd: any;

    private getCurStatPoint() {
        //组装命令
        let req = {
            "protVer": gProtocolVer,
            "cmd": "getCurStatPoint"
        };
        //
        this.clientCmd = req;

        //
        this.tryConnect();
    }
    
    

    private tryConnect(serverUrl: string=""){

        egret.log("connect to stat monitor server");
        this.gameServerSocket.addEventListener( egret.ProgressEvent.SOCKET_DATA, this.onRecvMsgFromGameSvr, this );
        this.gameServerSocket.addEventListener( egret.Event.CONNECT, this.onConnectedToGameSvr, this );
        this.gameServerSocket.addEventListener( egret.Event.CLOSE, this.onCloseFromGameSvr, this );
        this.gameServerSocket.addEventListener( egret.IOErrorEvent.IO_ERROR, this.onIOErrorFromGameSvr, this );
        // this.gameServerSocket.connect(this.gameServerHost, this.gameServerPort);
        if(serverUrl == ""){
            this.gameServerSocket.connectByUrl(this.gameServerUrl);
        }else{
            this.gameServerSocket.connectByUrl(serverUrl);
        }
        
    }
        
    private onConnectedToGameSvr(e: egret.Event) {
        egret.log("connected to GameServer")

        //
        let req = this.clientCmd;
        let reqPack = JSON.stringify(req);
        egret.log("发送数据：" + reqPack);
        this.gameServerSocket.writeUTF(reqPack);
    }
    private onCloseFromGameSvr(e: egret.Event) {
        egret.log("closed from GameServer")
        this.closeConnet();
    }
    private onIOErrorFromGameSvr(e: egret.Event) {
        egret.log("IOErr from GameServer")
        
        if(this.gameEventListenerObj){
            this.gameEventListenerObj.callbackNetworkError();
        }

        this.closeConnet();
    }
    private closeConnet(){
        if(this.gameServerSocket){
            this.gameServerSocket.removeEventListener( egret.ProgressEvent.SOCKET_DATA, this.onRecvMsgFromGameSvr, this );
            this.gameServerSocket.removeEventListener( egret.Event.CONNECT, this.onConnectedToGameSvr, this );
            this.gameServerSocket.removeEventListener( egret.Event.CLOSE, this.onCloseFromGameSvr, this );
            this.gameServerSocket.removeEventListener( egret.IOErrorEvent.IO_ERROR, this.onIOErrorFromGameSvr, this );
            this.gameServerSocket.close();
        }
        this.gameServerSocket = undefined;

        this.clientCmd = undefined;//清空请求
        this.gameEventListenerObj = undefined;
    }

    //
    private onRecvMsgFromGameSvr(e: egret.Event) {

        var str = this.gameServerSocket.readUTF();    
        // egret.log("收到数据：" + str);

        let msg = JSON.parse(str);
        let cmd: string = msg['cmd'];

        if(cmd == "getCurStatPointRsp"){
            this.onGetCurStatPointRsp(msg);
        }else{
            egret.log("bad cmd from gameserver." + cmd)
        }
    }
    

    private onGetCurStatPointRsp(msg: Object) {
        egret.log("onQueryLevelPassedStatisticsRsp.")
        let rsp = {};
        rsp['errCode'] = msg['errCode'];
        rsp['statArr'] = msg['statArr'];
        
        let gmEvent = new GameMotorEvent();
        // gmEvent.gmDataType = GM_DATA_TYPE_ENTERROOM;
        gmEvent.gmData = rsp;

        //
        this.addEventListener(GameMotorEvent.GAME_MOTOR, this.gameEventListenerObj.callBackGetCurStatPointRsp, this.gameEventListenerObj);
        this.dispatchEvent(gmEvent);
        this.removeEventListener(GameMotorEvent.GAME_MOTOR, this.gameEventListenerObj.callBackGetCurStatPointRsp, this.gameEventListenerObj);
    }
    

}


