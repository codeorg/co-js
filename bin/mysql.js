/**
 * Created by Codeorg.com on 2016/9/23.
 */
"use strict";
let mysql = require('mysql');
let util=require('co-util');

class Mysql {
    /**
     * 构造函数,var mysql=new Mysql(option);
     * @param {Object} option选项，json对象如:{port:3306,host:"127.0.0.1",user: 'root', password: '1',database: 'f5'}
     * @api public
     */
    constructor(option) {
        //console.log("option",option.collections)
        this.conn=option.conn;
        this.collections=option.collections;
        if(!this.conn){
            this.pool = mysql.createPool({
                port: option.port,
                host: option.host,
                user: option.user,
                password: option.password,
                database: option.database
            });
        }
        let methods = {
            find: this.find,
            findOne: this.findOne,
            page: this.page,
            findPage: this.findPage,
            insert: this.insert,
            update: this.update,
            remove: this.remove,
            exist: this.exist,
            scaler: this.scaler,
            max: this.max,
            min: this.min,
            count: this.count,
            sum: this.sum
        };
        let collections = option.collections;
        var command = (function (self, collection, fn) {
            return async function () {
                var args = [];
                args.push(collection);
                for (var i in arguments) {
                    args.push(arguments[i]);
                }
                return await fn.apply(self, args);
            }
        });
        let cmd = (collection) => {
            var objCmd = {};
            for (var key in methods) {
                objCmd[key] = new command(this, collection, methods[key])
            }
            return objCmd;
        }
        for (var i in collections) {
            this[collections[i]] = cmd(collections[i]);
        }
    }

    /**
     * 执行sql语句，存储过程
     * @param {String} 1.sql字符串，2.存储过程：call fn(IN,OUT)
     * @param {Function} cb回调函数
     * @api public
     */
    exec(sql) {
        console.log("sql",sql);
        return new Promise((resolve, reject)=> {
                if(!this.conn){
                    this.pool.query(sql, function (err, result) {
                        if (err) {
                            util.log('sql:' + sql ,err);
                            reject(util.err(4));
                        } else {
                            resolve(result)
                        }
                    });
                }else{
                    //有事务
                    this.conn.query(sql, function(err, result) {
                        if (err) {
                            util.log('sql:' + sql , err);
                            this.conn.rollback(function () {
                                this.conn.release();
                                reject(util.err(4));
                            });
                        }else{
                            resolve(result)
                        }
                    });
                }

            }
        )
    }

    /**
     * find，查询数据
     * @param {String} table表名
     * @param {Object|Array} con条件,接受json和array,当为json对象时，如：{id:1,username:'夜孤城'}等同于 id=1 and username='夜孤城'
     *                       当为array时，如：[{id:1,username:'夜孤城'},{age:20}]等同于(id=1 and username='夜孤城') or age=20
     *                       当需要用到>=|>|<=|<|!=时，只需要在值前面加入，默认为=,如：{age:'>=20'}等同于age>=20
     * @param {Object} option选项对象　{limit:'10,2',order:'id desc'} 限额，排序...
     * @return {Array} 返回为[]数组
     * @api public
     */
    async find(table, con, option) {
        let sql = 'select * from ' + mysql.escapeId(table) + this.getWhere(con)
        //console.log(con);
        //省略option
        if (option) {
            if (!!option.order)sql += ' order by ' + option.order;
            if (!!option.limit)sql += ' limit ' + option.limit;
        }
        return await this.exec(sql);
    }

    /**
     * findOne，查询一条数据
     * @param {String} table表名
     * @param {Object} con条件，json对象如:{id:1,username:'夜孤城'}
     * @param {Function} cb回调函数　fn(err,row) row为{}对象，非数组
     * @api public
     */
    async findOne(table, con) {
        let rows = await this.find(table, con, {limit: '1'});
        if (!rows || rows.length === 0) return null;
        return rows[0];
    }

    /**
     * findPage，分页
     * @param {String} table表名
     * @param {Object} con条件，json对象如:{id:1,username:'夜孤城'}
     * @param {Object} option选项对象　{limit:'10,2',order:'id desc'} 限额，排序...
     * @param {Array} 返回数组
     * @api public
     */
    async findPage(table, con, option) {
        option = option || {};
        option.order = option.order || "";
        option.pageNo=util.toInt(option.pageNo);
        option.pageSize=util.toInt(option.pageSize);
        option.pageNo = option.pageNo || 1;
        option.pageSize = option.pageSize || 10;
        option.limit = (option.pageNo - 1) * option.pageSize + ',' + option.pageSize;
        return await this.find(table, con, option);
    }
    /**
     * page，分页，带总数
     * @param {String} table表名
     * @param {Object} con条件，json对象如:{id:1,username:'夜孤城'}
     * @param {Object} option选项对象　{limit:'10,2',order:'id desc'} 限额，排序...
     * @param {Object} 返回{count:int,rows:[]}
     * @api public
     */
    async page(table, con, option) {
        let count=await this.count(table, con);
        let rows=await this.findPage(table, con, option);
        return {count:count,rows:rows};
    }


    /**
     * update，更新数据update操作
     * @param {String} table表名
     * @param {Object} con条件，json对象如:{id:1,username:'夜孤城'}
     * @param {Object} up欲修改字段对象，json对象如:{money:100000}
     *                 当需要使用字段计算[+|-|*|/]如{age:"+1"}等同于age=age+1
     * @param {Function} cb回调函数　fn(err,res),res为{status:true/false}
     * @api public
     */
    async update(table, con, up) {
        let sql = 'update '+mysql.escapeId(table)+' set ' +this.getUp(up);
        //let params = [table];
        //sql = mysql.format(sql, params);
        sql += this.getWhere(con);
        let res = await this.exec(sql);
        if (!res || !res.hasOwnProperty('affectedRows') || res.affectedRows === 0) return false;
        return true;
    }

    /**
     * insert，添加数据insert操作
     * @param {String} table表名
     * @param {Object} ins欲添加数据，json对象如:{id:1,username:'夜孤城'}
     * @param {Function} cb回调函数　fn(err,res),res为{status:true/false,id:新添加id}
     * @api public
     */
    async insert(table, ins) {
        let sql = 'insert into ?? set ?';
        let params = [table, ins];
        sql = mysql.format(sql, params);
        let res = await this.exec(sql);
        if (!res || !res.hasOwnProperty('affectedRows') || res.affectedRows === 0) return false;
        return {status: true, id: res.insertId};
    }

    /**
     * remove，删除数据delete操作
     * @param {String} table表名
     * @param {Object} con条件，json对象如:{id:1,username:'夜孤城'}
     * @param {Function} cb回调函数　fn(err,res),res为{status:true/false}
     * @api public
     */
    async remove(table, con) {
        let sql = 'delete from ' + mysql.escapeId(table) + this.getWhere(con);
        let res = await this.exec(sql);
        if (!res || !res.hasOwnProperty('affectedRows') || res.affectedRows === 0) return false;
        return true;
    }

    /**
     * exist，判断是否存在记录
     * @param {String} table表名
     * @param {Object} con条件，json对象如:{id:1,username:'夜孤城'}
     * @param {Function} cb回调函数　fn(err,res) res为{status:true/false}
     * @api public
     */
    async exist(table, con) {
        let sql = 'select * from ' + mysql.escapeId(table);
        sql += this.getWhere(con);
        sql += ' limit 1';
        let res = await this.exec(sql);
        if (!res || res.length === 0)  return false;
        return true;
    }

    /**
     * singleScaler，计数
     * @param {String} table表名
     * @param {String} option类型，max,min,sum,count...
     * @param {String} filed统计字段
     * @param {Object} con条件，json对象如:{id:1,username:'夜孤城'}
     * @param {Function} cb回调函数　fn(err,res),res为json对象{value:101}
     * @api private
     */
    async singleScaler(table, option, filed, con) {
        if (filed !== 0)filed = mysql.escapeId(filed);
        let fileds=option + '(' + filed + ') as `value`';
        let obj=await this.scaler(table, fileds, con);
        // var sql = 'select ' + option + '(' + filed + ') as `value` from ' + mysql.escapeId(table);
        // sql += this.getWhere(con);
        // let rows = await this.exec(sql);
        // if (!rows || rows.length === 0) return 0;
        return obj.value;
    }

    async scaler(table, fileds, con) {
        var sql = 'select ' +fileds+' from ' + mysql.escapeId(table);
        sql += this.getWhere(con);
        let rows = await this.exec(sql);
        if (!rows || rows.length === 0) return 0;
        return rows[0];
    }


    async max(table, filed, con, cb) {
        return this.singleScaler(table, 'max', filed, con, cb);
    }

    async min(table, filed, con, cb) {
        return this.singleScaler(table, 'min', filed, con, cb);
    }

    async count(table, con, cb) {
        return this.singleScaler(table, 'count', 0, con, cb);
    }

    async sum(table, filed, con, cb) {
        return this.singleScaler(table, 'sum', filed, con, cb);
    }

    //获取条件，返回 where id=1 and username='sdfds'
    getWhere(con) {
        //console.log("con", con)
        con = util.formatObj(con);
        if (!con) return '';
        return ' where ' + this.parseCon(con);
    }

    //_or:[{status:1},{status:2}] 等同 status=1 or status=2
    //_or:[[{status:1},{status:2}],[{ccc:1},{ccc:2}]] (status=1 or status=2) and (ccc=1 or ccc=2)
    parseCon(con) {
        let obj={};
        util.extend(obj,con);
        if (!obj||typeof obj !== 'object') return '';

        if(obj._or){
            let arr=obj._or;
            if(arr.length==0)  {
                delete obj._or;
                return this.objToString(obj);
            }

            if (arr.length === 1) {
                if(typeof arr[0]==="object") return this.objToString(arr[0]);
            }
            let arrOr = [],arrAnd = [],str="";
            for (var i = 0, len = arr.length; i < len; i++) {
                if(Array.isArray(arr[i])){
                    let arrChild=[];
                    for (var j = 0, len2 = arr[i].length; j < len2; j++) {
                        arrChild.push(this.objToString(arr[i][j]));
                    }
                    if(arrChild.length>0) arrAnd.push("("+arrChild.join(" or ")+ ")");
                }else {
                    arrOr.push(this.objToString(arr[i]));
                }
            }

            if(arrOr.length>0){
                str=arrOr.join(" or ");
                str="("+str+")";
            }else{
                str=arrAnd.join(" and ");
            }
            delete obj._or;
            let otherCon=this.objToString(obj);
            return !otherCon? str:str+ " and " +otherCon;

        }else {
            return this.objToString(obj);
        }

        // if (Array.isArray(obj)) {
        //     if (obj.length === 1) return this.objToString(obj[0]);
        //     for (var i = 0, len = obj.length; i < len; i++) {
        //         values.push('(' + this.objToString(obj[i]) + ')');
        //     }
        // } else if (typeof obj === 'obj') {
        //     return this.objToString(obj);
        // }

    };

    objToString(object) {
        var values = [];
        for (var key in object) {
            var value = object[key];
            if (typeof value === 'function') {
                continue;
            }
            //"[1000,10000]" 大于等于
            let match=/^([\[\(])([\d\.]*),([\d\.]*)([\]\)])$/.exec(value);
            if(match&&match.length==5){
                let s1=match[1]=="["?">=":">";
                if(match[2]){
                    values.push(mysql.escapeId(key) + ' '+s1+' ' + mysql.escape(match[2], true));
                }
                let s2=match[4]=="]"?"<=":"<";
                if(match[3]){
                    values.push(mysql.escapeId(key) + ' '+s2+' ' + mysql.escape(match[3], true));
                }
                continue;
            }
            //username:"like '%aaa%'"　搜索
            let match2=/^like '([\w\W]+)'$/i.exec(value);
            if(match2&&match2.length==2){
                values.push(mysql.escapeId(key) + ' like ' + mysql.escape(match2[1], true));
                continue;
            }
            //username:"!='dasd'"　不等于
            let match3=/^!='([\w\W]+)'$/i.exec(value);
            if(match3&&match3.length==2){
                values.push(mysql.escapeId(key) + ' != ' + mysql.escape(match3[1], true));
                continue;
            }

            //正常等号
            values.push(mysql.escapeId(key) + ' = ' + mysql.escape(value, true));
        }
        return values.join(' and ');
    }

    // formatKey(str){
    //     let pat=/([0-9a-z_]+)([>|>=|<|<=|!=|=|\%]*)$/gi;
    //     let m=pat.exec(str);
    //     if(!m||m.length<3) return {sign:"=",key:str};
    //     console.log(m)
    //     if(m[2]=="%")return {sign:"like",key:m[1]};
    //
    //     return {sign:m[2],key:m[1]};
    // }

    getUp(object) {
        var values = [];
        for (var key in object) {
            var value = object[key];
            if (typeof value === 'function') {
                continue;
            }
            let objV=this.formatUpValue(value);
            if(!objV.sign){
                values.push(mysql.escapeId(key) + ' = ' + mysql.escape(value, true));
            }else{
                values.push(mysql.escapeId(key) + ' = ' +mysql.escapeId(key)+objV.sign+ mysql.escape(objV.value, true));
            }

        }
        return values.join(',');
    }

    // formatUpKey(str){
    //     let pat=/([0-9a-z_]+)([\+|\-|\*|\/]*)$/gi;
    //     let m=pat.exec(str);
    //     if(!m||m.length<3) return {sign:"",key:str};
    //     return {key:m[1],sign:m[2]};
    // }


//{aa:"(+)ssss"}
    formatUpValue(str){
        let pat=/^\(([\+|\-|\*|\/|\%]*)\)([\d\.]+)/gi;
        let m=pat.exec(str);
        if(!m||m.length<3) return {sign:"",value:str};
        return {sign:m[1],value:m[2]};
    }

    //------------------------------------存储过程------------------------------------
    /**
     * 执行存储过程
     * @param {String} 1.存储过程：call fn(IN,OUT)
     * @param {Object} 返回值{err:0,rows:[],msg:""}
     * @api public
     */
    async exesp(sql) {
        let res = await this.exec(sql);
        let obj = {err: 0};
        if (!res || res.length == 0) return util.err(4);
        if (!res[0] || res[0].length == 0) return util.err(4);
        if (res[0].length == 1) {
            let doc = res[0][0];
            if (!doc.err) {
                obj.rows = [];
                obj.rows.push(doc);
                return obj;
            }
            return util.err(doc.err)
        }
        obj.rows = res[0];
        return obj;
    }


    //------------------------------------事务------------------------------------
    /**
     * 获取一个事务
     * @return {Object} 返回一个dbt，在同一个conn里进行多次操作
     * @api public
     */
    tx() {
        return new Promise((resolve, reject)=> {
            this.pool.getConnection((err, connection)=> {
                if(err) {
                    util.log(err);
                    return reject(util.err(4));
                }
                connection.beginTransaction((err)=>{
                    if(err) {
                        util.log(err);
                        return reject(util.err(4));
                    }
                    var dbt = new Mysql({conn: connection,collections:this.collections});
                    resolve(dbt);
                })
            })
        })
    }
    /**
     * 事务确认提交
     * @return {Boolen} 返回true|flase
     * @api public
     */
    commit() {
        return new Promise((resolve, reject)=> {
            this.conn.commit((err) => {
                if (err) {
                    this.conn.rollback(()=>{
                        this.conn.release();
                        util.log(err);
                        resolve(false);
                    });
                } else {
                    this.conn.release();
                    resolve(true);
                }
            })
        });
    }
    /**
     * 事务回滚
     * @api public
     */
    rollback () {
        return new Promise((resolve, reject)=> {
            this.conn.rollback(()=> {
                this.conn.release();
                resolve(true);
            });
        })
    }
}
module.exports=Mysql;
