

//facebook instant game
let FBPlayerId = "";
let FBPlayerName = "";
let FBPlayerAvatarUrl = "";

//语言
enum GameLanguage{
    en,
    ch
}
let gGameLanguage = GameLanguage.en;

let gMain: Main;
class Main extends egret.DisplayObjectContainer {
    public constructor() {
        super();
        this.addEventListener(egret.Event.ADDED_TO_STAGE, this.onAddToStage, this);

        gMain = this;
    }

    private onAddToStage(event: egret.Event) {
        GameManager.stage = this.stage;

        egret.lifecycle.addLifecycleListener((context) => {
            // custom lifecycle plugin

            context.onUpdate = () => {

            }
        })

        egret.lifecycle.onPause = () => {
            egret.ticker.pause();
            egret.log("egret.lifecycle.onPause");
        }

        egret.lifecycle.onResume = () => {
            egret.ticker.resume();
            egret.log("egret.lifecycle.onResume");
            
        }

        this.runGame().catch(e => {
            console.log(e);
        })



    }

    private async runGame() {
        //解决图片跨域
        // if(egret.Capabilities.runtimeType == egret.RuntimeType.WXGAME){
            egret.ImageLoader.crossOrigin = "anonymous";
        // }
        
        egret.TextField.default_fontFamily = "Microsoft YaHei";// "SimHei";

        {
            //其他测试情况
            await this.loadResource()
            
            

            SceneManager.init(this.stage);
        }
    }


    ////////////////////////////////////////////
    private networkMotor: NetworkMotor = undefined;
    //网络操作取消函数
    private cancelNetworkReq(){
        this.networkMotor.closeMotor();
        this.networkMotor = undefined;
    }
    //记录当前是否有需要监听网络回调

    public callbackNetworkError(){
        // this.notifyPanel.showNotify("Network Error... :(", true);
    }
    


    private async loadResource() {
        try {
            if(egret.Capabilities.runtimeType == egret.RuntimeType.WXGAME){
                await RES.setMaxLoadingThread(1);//防加载卡死
            }

            const loadingView = new LoadingUI();
            this.stage.addChild(loadingView);
            await RES.loadConfig("resource/default.res.json", "resource/");
            await RES.loadGroup("preload", 0, loadingView);
            this.stage.removeChild(loadingView);
        }
        catch (e) {
            console.error(e);
        }
    }
}