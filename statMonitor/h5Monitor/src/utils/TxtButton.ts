
class TxtButton extends egret.DisplayObjectContainer {
    public bg: egret.Bitmap;
    private bgResName: string;
    public nameTxt: egret.TextField;

    private listener: Function;
    private thisObject: any;

    public dataTrans: any;

    public constructor(txt: string, bgResName: string, txtColor: number, txtSize: number, listener: Function, thisObject: any, data: any=undefined, width: number=0, height: number=0) {
        super();

        //
        this.dataTrans = data;

        //
        this.bgResName = bgResName;
        this.bg = new egret.Bitmap(RES.getRes(this.bgResName));
        this.addChild(this.bg);
        //
        if(width > 0){
            this.bg.width = width;
        }
        if(height > 0){
            this.bg.height = height;
        }
        //
        this.width = this.bg.width;
        this.height = this.bg.height;

        //name
        this.nameTxt = new egret.TextField;
        this.nameTxt.text = txt;
        this.nameTxt.width = this.bg.width;
        this.nameTxt.height = this.bg.height;
        this.nameTxt.textColor = txtColor;
        this.nameTxt.lineSpacing = 5;
        this.nameTxt.size = txtSize;
        this.nameTxt.textAlign = egret.HorizontalAlign.CENTER;
        this.nameTxt.verticalAlign = egret.VerticalAlign.MIDDLE;
        this.nameTxt.x = 0;
        this.nameTxt.y = 0;
        this.addChild(this.nameTxt);

        //
        this.touchEnabled = true;
        this.listener = listener;
        this.thisObject = thisObject;
        this.addEventListener(egret.TouchEvent.TOUCH_TAP, this.buttonTap, this);
    }

    private buttonTap(e: egret.TouchEvent){
        //播放音效
        // SoundManager.playButtonClick();

        this.listener.call(this.thisObject, e);
    }

    public setButtonEnable(){
        // this.bg.$setBitmapData(RES.getRes(this.bgResName));
        this.addEventListener(egret.TouchEvent.TOUCH_TAP, this.buttonTap, this);
    }
    public setButtonDisable(){
        // this.bg.$setBitmapData(RES.getRes("button_grey_png"));
        this.removeEventListener(egret.TouchEvent.TOUCH_TAP, this.buttonTap, this);
    }

}