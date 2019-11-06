
//将BitMap包装成一个container，以便可以作为父节点使用
class BitMapContainer extends egret.DisplayObjectContainer {
    public bitMap: egret.Bitmap;

    public constructor(imgData: egret.BitmapData | egret.Texture, width: number=0, height: number=0) {
        super();

        //图片
        this.bitMap = new egret.Bitmap();
        this.bitMap.$setBitmapData(imgData);
        if(width > 0){
            this.bitMap.width = width;
        }
        if(height > 0){
            this.bitMap.height = height;
        }
        this.width = this.bitMap.width;
        this.height = this.bitMap.height;
        this.addChild(this.bitMap);
    }

    public setTouchEnable(enable: boolean){
        this.touchEnabled = enable;
        this.bitMap.touchEnabled = enable;
    }

    public removeChildren(){
        super.removeChildren();
        this.addChild(this.bitMap);
    }
}