// TypeScript file
class GameManager {
    public static stage:egret.Stage;

    //获取到当前实际显示的矩形区域，只针对noBorder模式
    public static getViewRect(): egret.Rectangle{
        let rect = new egret.Rectangle;
        

        //判断noBorder缩放是以那边为基准
        let screenWidth = egret.Capabilities.boundingClientWidth;
        let screenHeight = egret.Capabilities.boundingClientHeight;
        if(screenWidth/screenHeight < this.stage.stageWidth/this.stage.stageHeight){
            //以高为基准，裁剪宽度
            rect.height = this.stage.stageHeight;
            rect.width = rect.height * (screenWidth/screenHeight);

            rect.x = (this.stage.stageWidth - rect.width)/2;
            rect.y = 0;
        }else{
            //以宽为基准，裁剪高度
            rect.width = this.stage.stageWidth;
            rect.height = rect.width * (screenHeight/screenWidth);

            rect.x = 0;
            rect.y = (this.stage.stageHeight - rect.height)/2;
        }

        return rect;
    };
    
    
}