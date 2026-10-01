'use strict';

function TranStatus(ISATHR, ISAUTH, ISFAIL, ISINIT, ISRECN, ISRECV) {

  if (ISINIT === true && ISRECV === true && ISATHR === true && ISAUTH === true && ISRECN === true) {
    return 'Reconcile';

  }
  else if (ISINIT === true && ISRECV === true && ISATHR === false && ISAUTH === true && ISFAIL === false) {
    return 'Authorized';
  }
  else if (ISFAIL === true) {
    return 'Failed';
  }
  else if (ISINIT === true && ISRECV === true && ISATHR === true && ISAUTH === true && ISRECN === false) {
    return 'Auth Received';
  }
  else if (ISINIT === true && ISRECV === true) {
    return 'Received';
  }
  return 'Initiated';

}

module.exports = TranStatus;
