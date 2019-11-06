
class TxtHoldButton extends egret.DisplayObjectContainer {
    private bg: egret.Bitmap;
    private bgResName: string;
    public nameTxt: egret.TextField;

    private listener: Function;
    private touchEndListener: Function;
    private thisObject: any;

    public dataTrans: any;

    private holdTapSpeed: number;

    public constructor(txt: string, bgResName: string, txtColor: number, txtSize: number, listener: Function, touchEndListener: Function, thisObject: any, data: any=undefined, width: number=0, height: number=0, holdTapSpeed: number=10) {
        super();

        //
        this.dataTrans = data;
        //
        this.holdTapSpeed = holdTapSpeed;

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
        this.touchEndListener = touchEndListener;
        this.thisObject = thisObject;
        this.addEventListener(egret.TouchEvent.TOUCH_TAP, this.listener, this.thisObject);
        this.addEventListener(egret.TouchEvent.TOUCH_BEGIN, this.touchBegin, this);
        this.addEventListener(egret.TouchEvent.TOUCH_CANCEL, this.touchEnd, this);
        this.addEventListener(egret.TouchEvent.TOUCH_RELEASE_OUTSIDE, this.touchEnd, this);
        this.addEventListener(egret.TouchEvent.TOUCH_END, this.touchEnd, this);
    }

    public setButtonEnable(){
        // this.bg.$bitmapData = RES.getRes(this.bgResName);
        this.addEventListener(egret.TouchEvent.TOUCH_TAP, this.listener, this.thisObject);
    }
    public setButtonDisable(){
        // this.bg.$bitmapData = RES.getRes("button_grey_png");
        this.removeEventListener(egret.TouchEvent.TOUCH_TAP, this.listener, this.thisObject);
    }

    private touchStartTime: number;
    private touchHoldStartTimer: egret.Timer;//按住多长时间开始
    private touchHoldClickTimer: egret.Timer;//按住反复点击
    private touchBegin(){
        this.touchStartTime = egret.getTimer();
        this.touchHoldStartTimer = new egret.Timer(500, 1);
        this.touchHoldStartTimer.addEventListener(egret.TimerEvent.TIMER, this.holdStart, this);
        this.touchHoldStartTimer.start();
    }
    private touchEnd(){
        if(this.touchHoldStartTimer){
            this.touchHoldStartTimer.stop();
            this.touchHoldStartTimer = undefined;
        }
        if(this.touchHoldClickTimer){
            this.touchHoldClickTimer.stop();
            this.touchHoldClickTimer = undefined;
        }

        if(this.touchEndListener){
            this.touchEndListener.call(this.thisObject);
        }
    }
    private holdStart(){
        this.touchHoldClickTimer = new egret.Timer(1000/this.holdTapSpeed, 0);
        this.touchHoldClickTimer.addEventListener(egret.TimerEvent.TIMER, this.listener, this.thisObject);
        this.touchHoldClickTimer.start();
    }

}