'use strict';

const uuid = require('uuid');
const fs = require('fs');
const path = require('path');
const dates = require('./dates');
const mkdirp = require('mkdirp');

module.exports = {
  create: create,
  readFromFile: readFromFile,
  deleteFile: deleteFile,
  isExist : isExist
};

function create(data, ext) {
  const base = path.join(__dirname, '../..');
  const folder = path.join('document', dates.ddMMyyyy);
  const fileName = uuid() + '.' + ext;
  const base64Data = data.replace(/^data:image\/[a-zA-Z]+;base64,/, '');
  return dirExists(path.join(base, folder))
    .then(() => {
      const filePath = path.join(base, folder, fileName);
      return createFile(base64Data, filePath);
    })
    .then(() => path.join(folder, fileName));
}

function dirExists(path) {
  return new Promise((resolve, reject) => {
    mkdirp(path, function(err) {
      if (err) {
        return reject(err);
      }
      return resolve(true);
    });
  });
}

function createFile(base64Data, path) {
  return new Promise((resolve, reject) => {
    fs.writeFile(path, base64Data, { encoding: 'base64' }, function(err) {
      if (err) {
        return reject(err);
      }
      return resolve(true);
    });
  });

}

function readFromFile(file) {
  return new Promise((resolve, reject) => {
    fs.readFile(file, 'utf8', function(err, res) {
      if (err) {
        reject(err);
      }
      resolve(res);
    });
  });
}

function deleteFile(file) {
  return new Promise((resolve, reject) => {
    fs.unlink(file, function(error) {
      if (error) {
        reject(error);
      }
      resolve(true);
    });
  });
}

function isExist(path) {
  return new Promise((resolve, reject) => {
    fs.exists(path, function (exists) {
      if (exists) {
        resolve(true);
      }
       reject(false);
    });
  })
}