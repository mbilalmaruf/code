'use strict';

function findStatus(ISRFND, ISFAIL, EPayStatusAuthRecv, EPayStatusRecv) {
  let status = '';
  if (ISRFND) {
    status = 'REFUNDED';
  }
  else if (ISFAIL) {
    status = 'FAILED';
  }
  else if (EPayStatusAuthRecv !== '') {
    status = EPayStatusAuthRecv;
  }
  else if (EPayStatusRecv !== '') {
    status = EPayStatusRecv;
  }
  else {
    status = 'PENDING';
  }
  return status;
}

module.exports = findStatus;
