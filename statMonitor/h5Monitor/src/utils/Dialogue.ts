



class Dialogue extends egret.DisplayObjectContainer {
    private blackMask: BitMapContainer;
    private panel: BitMapContainer;

    //panelX,panelY为负表示放中间
    public constructor(panel: BitMapContainer, maskWidth: number, maskHeight: number, panelX: number = -1, panelY: number = -1) {
        super();

        //
        this.blackMask = new BitMapContainer(RES.getRes("black_mask_png"), maskWidth, maskHeight);
        this.blackMask.touchEnabled = true;
        this.blackMask.alpha = 0.6;
        this.blackMask.addEventListener(egret.TouchEvent.TOUCH_TAP, this.retreat, this);
        this.addChild(this.blackMask);

        //
        this.width = this.blackMask.width;
        this.height = this.blackMask.height;

        //
        this.panel = panel;
        if(panelX < 0){
            this.panel.x = GameManager.getViewRect().x - this.panel.width/2;
        }else{
            this.panel.x = panelX;
        }
        if(panelY < 0){
            this.panel.y = GameManager.getViewRect().y - this.panel.height/2;
        }else{
            this.panel.y = panelY;
        }
        this.addChild(this.panel);

        //
        this.panel.touchEnabled = true;
        
    }

    public retreat(){
        this.parent.removeChild(this);
    }

}