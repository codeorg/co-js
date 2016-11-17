/**
 * Created by Administrator on 2016/11/17.
 */

global.$config={
    log:{path:require('path').join(__dirname,'./logs/')},
    errs:{
        "0":"%s",
        "1":"系统异常",
        "2":"%s",
        "3":"Redis出错",
        "4":"数据库出错",
        "5":"Leveldb出错",
        "6":"http post出错",
        "7":"http get出错",
        "8":"二维码生成出错",
        "20":"加密出错",
        "21":"验证码出错",
        "22":"验证码重新分配",
        "100":"------------------------[System]------------------------",
        "101":"router:路由格式有误,出错url:%s",
    }
}
if(!global.$config) {
    throw new Error('全局配置不存在，请选择设置global.$config');
    return;
}

const Http=require('./bin/http');
const Mysql=require('./bin/mysql');
class Co{
    constructor(opts){
        this.Http=Http;
        this.Mysql=Mysql;
    }

}
let co=new Co(global.$config)

let util=require('co-util');
util.log('ssssssssssssssss');
//co.log("ssss")
module.exports=co;