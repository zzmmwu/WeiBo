// TypeScript file

//
enum SceneID {
    WELCOME = 1
};

let RecomPlayerNeedIncludeOpenId = undefined;

class SceneManager {

    public static alertLayer: egret.Sprite = new egret.Sprite;
    public static gameLayer: egret.Sprite = new egret.Sprite;

    // private static bgMusicSound: egret.Sound;
    // private static bgMusicChn: egret.SoundChannel;

    //所有游戏的Scene都存储在这个键值对数组中。{SceneID=>GameScene}
    public static _sceneArr: any[] = [];

    public constructor() {
    }

    public static welcome: WelcomeScene;

    public static init(stage: egret.Stage) {
        stage.addChild(this.alertLayer);
        stage.addChild(this.gameLayer);

        

        //初始化欢迎面板
        this._sceneArr[SceneID.WELCOME] = new WelcomeScene();;

        this.gameLayer.addChild(this._sceneArr[SceneID.WELCOME]);
    }

    public static fixScreen() {
        if (egret.Capabilities.isMobile) {
            GameManager.stage.scaleMode = egret.StageScaleMode.NO_BORDER;
        } else {
            GameManager.stage.scaleMode = egret.StageScaleMode.SHOW_ALL;
        }


    }


    public static switchScene(fromSceneId: SceneID, toSceneId: SceneID, data: any = undefined, returnToMe: boolean=true) {
        
        
        if (!this._sceneArr[toSceneId]) {
            switch (toSceneId) {
                case SceneID.WELCOME:
                    this._sceneArr[toSceneId] = new WelcomeScene;
                    break;
                default:
                    console.log("bad toSceneId=" + toSceneId);
                    return;
            }
        }

    }

    public static callbackNetworkError(){
        egret.log("report on network error");
    }


}