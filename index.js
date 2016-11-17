/**
 * Created by Administrator on 2016/11/17.
 */
var path=require('path');
global.$config={
    log:{path:path.join(__dirname,'./logs/')}
}
let util=require('co-util');
class Co{
    constructor(opts){
        //console.log(test111)
        //global.dir=opts;
        //this._log=new Log({logdir:opts.logdir});
    }

}
let co=new Co(global.$config)
util.log('ssssssssssssssss');
//co.log("ssss")
module.exports=Co;