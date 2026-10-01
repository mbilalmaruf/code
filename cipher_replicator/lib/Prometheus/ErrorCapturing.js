const { Counter, register, Registry } = require("prom-client");
const os = require("os");
const moment = require("moment-timezone");

class ErrorCapturing {
  static _instanceCache = null;
  
  static instance() {
    if (!this._instanceCache) {
      console.log("--------------Creating Error Capturing Instance--------------------");
      this._instanceCache = new this();
    }
    return this._instanceCache;
  }

  errorCodes = {};
  registry = new Registry();
  errorDescriptor = new Counter({
    name: "T6_capture_response_errors_total",
    help: "Total number of response errors in my app",
    registers: [this.registry],
    labelNames: [
      "error_code",
      "error_message",
      "error_time",
      "error_stack",
      "bankCode",
      "processor",
      "UTCBatchNo",
      "UTCRefNo",
      "messageId",
      "source",
    ],
  });

  errorCounter = new Counter({
    name: "error_rate",
    help: "Total number of errors",
    registers: [this.registry],
    labelNames: ["error_code", "error_description"],
  });

  /**
   * Initializes the ErrorCapturing module.
   * This function registers error capturing metrics, fetches error codes from the database,
   * and sets up the error codes for the module.
   * If error codes are not found in the database, it falls back to using default error codes.
   * @returns {Promise<void>} A promise that resolves once the initialization is complete.
   */
  async init() {
    console.log("--------------Registering Error Capturing Metrics--------------------");
    register.registerMetric(this.errorDescriptor);
    register.registerMetric(this.errorCounter);
    this.errorCodes = global.errorCodesForPrometheus ;
    console.log("--------------Error Capturing Metrics Registered--------------------");
  }

  /**
   * Records an error and captures relevant information.
   * @param {Error} error - The error object.
   * @param {string} type - The type of error.
   */
  recordError(error, type) {
    console.log("-------------Recording Error-------------", error, type);
    console.log("errorCode -> ", this.errorCodes[type])
    this.errorCounter.inc({
      error_code: type,
      error_description: this.errorCodes[type],
    });
    this.errorDescriptor.inc({
      error_code: type,
      error_message: this.errorCodes[type],
      error_time: moment().tz("Asia/Dubai").format("YYYY-MM-DD HH:mm:ss"),
      error_stack: error.stack || "N/A",
      bankCode: global.config.bankCode,
      processor: os.hostname(),
      UTCBatchNo: error.UTCBatchNo || "N/A",
      UTCRefNo: error.UTCRefNo || "N/A",
      messageId: error.messageId || "N/A",
      source: error.source || "N/A",
    });
  }
}

module.exports = ErrorCapturing;