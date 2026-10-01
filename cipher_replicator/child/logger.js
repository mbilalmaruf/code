const _ = require('lodash');
const path = require('path');
const util = require('util');
const contextService = require('request-context');

    function showDate() {
        let date = new Date();
        let str = date.toLocaleDateString('en-US', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
        })
        return str;
    }

    function getFileName(caller) {
        const STACK_FUNC_NAME = new RegExp(/at\s+((\S+)\s)?\((\S+):(\d+):(\d+)\)/);
        let err = new Error();
        Error.captureStackTrace(err);
        let stacks = err.stack.split('\n').slice(1);
        let fName = _.get(stacks, '2', '');
        let lNo = fName.split(path.sep);
        let filename, module;
        try {
            filename = lNo.pop();
            module = lNo.pop();
        } catch (ex) {
            // NOOP
        }
        let finalTrace = String(filename)
            .replace(')', ' ')
            .replace('(', ' ')
            .replace('\n', ' ')
            .trim();
        return `${''} ${finalTrace} `;
    }

    function formatArgs(args) {
        return util.format
            .apply(util.format, Array.prototype.slice.call(args))
            .indexOf('--- CSERV ') > -1 ?
            util.format.apply(util.format, Array.prototype.slice.call(args)) :
            util.format.apply(
                util.format,
                Array.prototype.slice.call(['-', ...args])
            );
    }
    let origLog = console.log;
    console.log = function() {
        let x = getFileName(this);
        let context = contextService.get('request:uuid');
        let strDate = `${showDate()} ${context || process.pid} ` + x;
        let logString = strDate + formatArgs(arguments);
        origLog(logString);
    };