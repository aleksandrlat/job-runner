var fork = require('child_process').fork;
var config = require('./../config');
/** @type {Logger} */
var logger = require('../../../lib/logger')('Job');

/**
 * @callback ForkCallback
 * @param {Error} err
 * @param {...*} results
 */

/**
 * @param {string} jobName
 * @param {ForkCallback} result
 * @returns {Function}
 */
function OnExit(jobName, result){
    return function(code, signal){
        var err;

        if (code === null) {
            err = 'Job "'+ jobName +'" was killed with signal: "'+ signal +'"';
        } else if (code > 0) {
            err = 'Job "'+ jobName +'" exited with code: "'+ code +'"';
        }

        if (err) {
            logger.error(err);
            result(new Error(err), null);
        } else {
            logger.info('"'+ jobName +'" finished succefully');
        }
    };
}

/**
 * @param {string} jobName
 * @param {ForkCallback} result
 * @returns {Function}
 */
function OnError(jobName, result){
    return function(err){
        logger.error('"'+ jobName +'" error: '+ err.message);
        result(err, null);
    };
}

/**
 * @param {string} jobName
 * @returns {Function}
 */
function OnData(jobName){
    return function(data){
        var buffer = new Buffer(data),
            log = buffer.toString();

        logger.info('"'+ jobName +'": '+ log.trim());
    };
}

/**
 * @param {string} jobName
 * @returns {Function}
 */
function OnErrorData(jobName){
    return function(data){
        var buffer = new Buffer(data),
            log = buffer.toString();

        logger.error('"'+ jobName +'": '+ log.trim());
    };
}

/**
 * @param {ForkCallback} result
 * @returns {Function}
 */
function OnMessage(result){
    return function(message){
        result.apply(null, message);
    };
}

/**
 * Empty function
 */
function DoNothing(){}

/**
 * @param {string} jobName
 * @param {...*} [args] - any additional arguments for job
 * @param {ForkCallback} [result] - if last argument is function it will be processed as result callback
 * @returns {Function}
 */
function GetForkRun(jobName, args, result){
    if (!config[jobName]) {
        throw new Error('Job "'+ jobName +'" does not exists');
    }

    args = Array.prototype.slice.call(arguments, 1);

    if (typeof(args[args.length - 1]) == 'function') {
        result = args.pop();
    } else {
        result = DoNothing;
    }

    return function(){
        var child = fork(__dirname +'/jobwrapper', [jobName], {silent: true}),
            exit = OnExit(jobName, result);

        child.on('exit', exit);
        // child.on('close', exit);
        child.on('error', OnError(jobName, result));
        child.on('message', OnMessage(result));

        child.stdout.on('data', OnData(jobName));
        child.stderr.on('data', OnErrorData(jobName));

        child.send(args);
    };
}

module.exports = GetForkRun;