
class ConfirmPanel extends BitMapContainer{
    private notifyText: egret.TextField;
    private confirmButton: TxtButton;
    private cancelButton: TxtButton;
    private confirmButtonListenerFunc: Function;
    private confirmButtonThisObj: any;
    private confirmFuncParam: any;

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
        this.addChild(this.notifyText);

        this.confirmButton = new TxtButton("确定", "red_button_png", 0xffffff, 20, this.confirmButtonTap, this, undefined, 120, 60);
        this.confirmButton.x = this.width/2 - 50 - this.confirmButton.width;
        this.confirmButton.y = this.height*3/5;
        this.addChild(this.confirmButton);

        this.cancelButton = new TxtButton("取消", "blue_button_png", 0xffffff, 20, this.cancelButtonTap, this, undefined, 120, 60);
        this.cancelButton.x = this.width/2 + 50;
        this.cancelButton.y = this.confirmButton.y;
        this.addChild(this.cancelButton);

        this.visible = false;
        this.bitMap.alpha = 0.7;

        this.addEventListener(egret.TouchEvent.TOUCH_TAP, this.cancelButtonTap, this);
    }

    public showConfirmPanel(txt: string, confirmFunction: Function, thisObj: any, confirmFunctionParam: any){
        this.notifyText.text = txt;
        this.confirmButtonListenerFunc = confirmFunction;
        this.confirmButtonThisObj = thisObj;
        this.confirmFuncParam = confirmFunctionParam;

        this.visible = true;
        if(this.parent){
            this.parent.setChildIndex(this, 9999);
        }
    }

    private confirmButtonTap(){
        this.visible = false;

        this.confirmButtonListenerFunc.call(this.confirmButtonThisObj, this.confirmFuncParam);
    }
    private cancelButtonTap(){
        this.visible = false;
    }

}