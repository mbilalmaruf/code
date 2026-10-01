'use strict';

function ExceptionType(ISATHR, ISAUTH, ISFAIL, ISINIT, ISRECN, ISRECV) {

  if (ISINIT === true && ISRECV === true && ISATHR === true && ISAUTH === true && ISRECN === true) {
    return 'SUCCESS';

  }
  else if (ISINIT === true && ISRECV === true && ISATHR === true && ISAUTH === true && ISFAIL === false) {
    return 'PG';
  }
  else if (ISINIT === true && ISRECV === true && ISATHR === true && ISAUTH === false && ISFAIL === true) {
    return 'DEG';

  }
  else if (ISINIT === true && ISRECV === true && ISATHR === true) {
    return 'DEG-PP';

  }
  else if (ISINIT === true && ISRECV === true) {
    return 'DEG-PP';
  }

  return 'SP-DEG';

}

module.exports = ExceptionType;
