
class NotifyPanel extends BitMapContainer{
    private notifyText: egret.TextField;
    private cancelModalButton: TxtButton;
    private cancelModalButtonListenerFunc: Function;
    private cancelModalButtonThisObj: any;

    constructor(){
        super(RES.getRes("black_mask_png"), GameManager.stage.stageWidth, GameManager.stage.stageHeight);

        this.touchEnabled = true;

        this.notifyText = new egret.TextField;
        this.notifyText.text = "";
        this.notifyText.width = GameManager.getViewRect().width*2/3;
        this.notifyText.height = 300;
        this.notifyText.x = this.width/2 - this.notifyText.width/2;
        this.notifyText.y = this.height*2/5 - this.notifyText.height/2;
        this.notifyText.textColor = 0xffffff;
        this.notifyText.textAlign = egret.HorizontalAlign.CENTER;
        this.notifyText.verticalAlign = egret.VerticalAlign.MIDDLE;
        this.notifyText.size = 40;
        this.notifyText.lineSpacing = 20;
        this.notifyText.wordWrap = true;
        this.addChild(this.notifyText);

        this.cancelModalButton = new TxtButton("Cancel", "red_button_png", 0xffffff, 25, this.cancelModalButtonTap, this, undefined, 100, 50);
        this.cancelModalButton.x = this.width/2 - this.cancelModalButton.width/2;
        this.cancelModalButton.y = this.height - 150 - this.cancelModalButton.height;
        this.cancelModalButton.visible = false;
        this.addChild(this.cancelModalButton);

        this.visible = false;
        this.bitMap.alpha = 0.7;
    }

    private callBackWhenNotifyHide: Function;
    private callBackThisObjWhenNotifyHide: any;
    public showNotify(txt: string, hideAuto: boolean=true, callBackWhenNotifyHide: Function=undefined, thisObj: any=undefined){
        this.callBackWhenNotifyHide = callBackWhenNotifyHide;
        this.callBackThisObjWhenNotifyHide = thisObj;

        this.visible = true;
        this.cancelModalButton.visible = false;

        if(this.parent){
            this.parent.setChildIndex(this, 9999);
        }

        this.notifyText.text = txt;

        if(hideAuto){
            egret.Tween.get(this).to({}, 1500).call(function(){
                this.hideNotify();
            }, this);
        }else{
            this.addEventListener(egret.TouchEvent.TOUCH_TAP, this.hideNotify, this);
        }
    }
    private hideNotify(){
        this.removeEventListener(egret.TouchEvent.TOUCH_TAP, this.hideNotify, this);
        this.visible = false;

        if(this.callBackWhenNotifyHide && this.callBackThisObjWhenNotifyHide){
            this.callBackWhenNotifyHide.call(this.callBackThisObjWhenNotifyHide);
        }
    }

    public showModalLoading(txt: string, cancelFunc: Function, thisObject: any){
        this.visible = true;
        this.cancelModalButton.visible = false;
        if(cancelFunc && thisObject){
            this.cancelModalButton.visible = true;
        }

        if(this.parent){
            this.parent.setChildIndex(this, 9999);
        }

        this.notifyText.text = "......" + txt + "......";
        this.removeEventListener(egret.TouchEvent.TOUCH_TAP, this.hideNotify, this);

        this.cancelModalButtonListenerFunc = cancelFunc;
        this.cancelModalButtonThisObj = thisObject;
    }
    public hideModalLoading(){

        this.visible = false;
    }
    private cancelModalButtonTap(){
        this.hideModalLoading();

        if(this.cancelModalButtonListenerFunc && this.cancelModalButtonThisObj){
            this.cancelModalButtonListenerFunc.call(this.cancelModalButtonThisObj);;
        }
    }
}