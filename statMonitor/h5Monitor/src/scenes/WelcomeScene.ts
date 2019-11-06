// TypeScript file

// //显示的卡片类型
// enum CardType {
//     normal = 1,
//     pre,
//     follow
// }





class WelcomeScene extends GameScene {
    // public static game : GamePanel;



    public constructor() {
        super();

        this.width = GameManager.stage.stageWidth;
        this.height = GameManager.stage.stageHeight;

        //
        this.playRect = new egret.Rectangle;
        this.playRect.width = GameManager.getViewRect().width;
        this.playRect.height = GameManager.getViewRect().height;
        this.playRect.x = GameManager.getViewRect().x;
        this.playRect.y = GameManager.getViewRect().y;

        this.init();
    }

    //顶栏和底栏中间的显示区域
    private playRect: egret.Rectangle;

    private bg: BitMapContainer;
    private gameTitle: egret.Bitmap;
    private blackMask: BitMapContainer;

    //
    private gameStarButton: TxtButton;
    private gameHelpMeButton: TxtButton;



    //命令反馈
    private followTxtBg: BitMapContainer;
    private followTxt: egret.TextField;




    private init() {

        //加载滚动背景图片。
        this.bg = new BitMapContainer(RES.getRes("bg_gray_png"), this.playRect.width, 8000);
        this.bg.touchEnabled = false;
        this.bg.x = 0;
        this.bg.y = 0;
        this.addChild(this.bg);

        var scrollView:egret.ScrollView = new egret.ScrollView();
        scrollView.setContent(this.bg);
        scrollView.width = this.playRect.width;
        scrollView.height = this.playRect.height;
        scrollView.x = this.playRect.x;
        scrollView.y = this.playRect.y;
        this.addChild(scrollView);

        this.queryLevelPassedStatistics();
    }

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

    private queryLevelPassedStatistics(){
        // this.notifyPanel.showModalLoading("query player data", this.cancelNetworkReq, this);

        this.networkMotor = new NetworkMotor(this);
        this.networkMotor.cmdGetCurStatPoint();
        
        return;
    }
    public callBackGetCurStatPointRsp(e: GameMotorEvent) {
        //
        this.bg.removeChildren();


        //
        let msg = <any>e.gmData;

        //排序
        let statArr: any[] = msg['statArr'];
        statArr.sort(function(a: any, b: any) : number{
            return a["svr_name"].localeCompare(b["svr_name"]);
        })
        


        //记录Y坐标
        let curYCursor = 20;

        for(let i=0; i<statArr.length; ++i){
            //total
            let totalLabel = new egret.TextField;
            totalLabel.text = "------" + statArr[i]["svr_name"] + "--------------------------------------";
            totalLabel.width = 500;
            totalLabel.height = 10;
            totalLabel.size = 10;
            totalLabel.textColor = 0x000000;
            totalLabel.x = 10 ;
            totalLabel.y = curYCursor;
            this.bg.addChild(totalLabel);
            //
            curYCursor += totalLabel.height + 5;

            for(let j=0; j<statArr[i]["data"].length; ++j){
                //name
                let nameLabel = new egret.TextField;
                nameLabel.text = statArr[i]["data"][j]["name"];
                nameLabel.width = 80;
                nameLabel.height = 10;
                nameLabel.size = 10;
                nameLabel.textColor = 0x000000;
                nameLabel.x = 10 + j*(nameLabel.width+10) ;
                nameLabel.y = curYCursor;
                this.bg.addChild(nameLabel);

                //name
                let dataLabel = new egret.TextField;
                dataLabel.text = statArr[i]["data"][j]["data"];
                dataLabel.width = 80;
                dataLabel.height = 10;
                dataLabel.size = 10;
                dataLabel.textColor = 0x000000;
                dataLabel.x = 10 + j*(dataLabel.width+10) ;
                dataLabel.y = curYCursor + nameLabel.height+5;
                this.bg.addChild(dataLabel);

                //warning
                this.setWarning(statArr[i]["svr_name"], statArr[i]["data"][j]["name"], statArr[i]["data"][j]["data"], dataLabel)
            }

            curYCursor += 30;
        }


        //
        this.bg.bitMap.height = curYCursor + 50;
        this.bg.height = curYCursor + 50;
    }

    
    private setWarning(svrName: string, dataName: string, data : number, label: egret.TextField){
        let warningLevel = 0;

        if(svrName.lastIndexOf("frontSvr")>=0){
            if(dataName.lastIndexOf("reqChanLen")>=100){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("reqChanLen")>=1000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("pullSvr")>=0){
            if(dataName.lastIndexOf("reqChanLen")>=100){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("reqChanLen")>=10000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("followSvr")>=0){
            if(dataName.lastIndexOf("cacheUserCount")>=100000){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("cacheUserCount")>=1000000){
                warningLevel = 2;
            }
        }
        if(svrName.lastIndexOf("followSvr")>=0){
            if(dataName.lastIndexOf("cacheFollowCount")>=10000000){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("cacheFollowCount")>=20000000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("followedSvr")>=0){
            if(dataName.lastIndexOf("cacheUserCount")>=100000){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("cacheUserCount")>=1000000){
                warningLevel = 2;
            }
        }
        if(svrName.lastIndexOf("followedSvr")>=0){
            if(dataName.lastIndexOf("cacheFollowerCount")>=10000000){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("cacheFollowerCount")>=20000000){
                warningLevel = 2;
            }
        }
        if(svrName.lastIndexOf("followedSvr")>=0){
            if(dataName.lastIndexOf("onlineUserCount")>=100100){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("onlineUserCount")>=200000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("usrMsgIdSvr")>=0){
            if(dataName.lastIndexOf("cacheUserCount")>=100000){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("cacheUserCount")>=1000000){
                warningLevel = 2;
            }
        }
        if(svrName.lastIndexOf("usrMsgIdSvr")>=0){
            if(dataName.lastIndexOf("cacheMsgIdCount")>=1000000){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("cacheMsgIdCount")>=2000000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("contentSvr")>=0){
            if(dataName.lastIndexOf("cacheMsgCount")>=1000000){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("cacheMsgCount")>=2000000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("postSvr")>=0){
            if(dataName.lastIndexOf("reqChanLen")>=100){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("reqChanLen")>=10000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("pushSvr")>=0){
            if(dataName.lastIndexOf("reqChanLen")>=100){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("reqChanLen")>=10000){
                warningLevel = 2;
            }
        }
        if(svrName.lastIndexOf("pushSvr")>=0){
            if(dataName.lastIndexOf("onlineUserCount")>=100100){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("onlineUserCount")>=200000){
                warningLevel = 2;
            }
        }

        //
        if(svrName.lastIndexOf("frontNotifySvr")>=0){
            if(dataName.lastIndexOf("onlineReqChanLen")>=100){
                warningLevel = 1;
            }else if(dataName.lastIndexOf("onlineReqChanLen")>=10000){
                warningLevel = 2;
            }
        }

        if(warningLevel == 0){
            label.textColor = 0x0466d5;
        }else if(warningLevel == 1){
            label.background = true;
            label.backgroundColor = 0xf15500;
            label.textColor = 0xffffff;
        }else if(warningLevel == 2){
            label.background = true;
            label.backgroundColor = 0x720000;
            label.textColor = 0xffffff;
        }
        
    }
}


