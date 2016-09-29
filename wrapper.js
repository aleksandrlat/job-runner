var config = require('./../config');
var jobName = process.argv[2];

var logger = require('../../../lib/logger')('jobwrapper - "'+ jobName +'"');

process.on('uncaughtException', result);

if (!config[jobName]) {
    throw new Error('Job "'+ jobName +'" does not exists');
}

var job = require('../'+ jobName);

if (typeof(job.Run) != 'function') {
    throw new TypeError('Job "'+ jobName +'" does not have method Run');
}

function result(err, result){
    if (err) {
        err = {
            name: err.name,
            message: err.message,
            stack: err.stack
        };
    }

    arguments[0] = err;

    process.send(Array.prototype.slice.call(arguments));
    process.disconnect();
}

process.on('message', function(args) {
    args.push(result);
    job.Run.apply(null, args);
});