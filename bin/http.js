/**
 * Created by Codeorg.com on 2016/11/3.
 */
"use strict";
var util=require('co-util');
var http=require('http');
var https=require('https');
 class Http{
    constructor(url,contentType){
        this.contentType=contentType||'application/json';
        url.replace(/^(http[s]*):\/\/(.*?)(:(\d+))?(\/[\w\W]*)*$/,(a,b,c,d,e,f)=>{
            this.http=b.toLowerCase()=='https'?https:http;
            this.host=c;
            this.port=e;
            this.path=f||'/';
        });
    }
    post(data){
        return new Promise( (resolve, reject)=> {
            var postData = JSON.stringify(data);
            let options = {
                hostname: this.host,
                port: this.port,
                path: this.path,
                method: 'POST',
                headers: {
                    'Content-Type': this.contentType,
                    'Content-Length': Buffer.byteLength(postData)
                }
            };
            let req = this.http.request(options, (res) => {
                res.setEncoding('utf8');
                let str='';
                res.on('data', (chunk) => {
                    str+=chunk;
                });
                res.on('end', () => {
                    try {
                        let obj=JSON.parse(str);
                        resolve(util.body(obj));
                    }catch(e){
                        resolve(util.err(6));
                    }
                });
            });
            req.on('error', (e) => {
                resolve(util.err(6));
            });
            req.write(postData);
            req.end();
        })

    }


}

module.exports=Http;




