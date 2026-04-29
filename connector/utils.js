import fs from "fs";

const randomString = () => crypto.randomBytes(16).toString('hex');
const generateHexString = (length) => {
  const characters = '0123456789abcdef';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * characters.length));
  }
  return result;
}
const join = (arr) => {
  let res = '';
  for (let i = 0; i < arr.length; i++) {
    res += arr[i] + ',';
  }

  return res.slice(0, -1);
};

const fileHandler = new function (path, data) {
  this.readJSON = (path) => new Promise((resolve, reject) =>{
    console.log("...opening " + path);
    fs.readFile(path, 'utf8', function(err, contents){
      console.log("...parsing " + path);
      if (err) {
        console.log(err);
      } else {
        try {
          contents=JSON.parse(contents);
        } catch (e) {
          console.log (e);
          return;
        }
        resolve(contents);
      }
    });
  });

  this.appendJSON = (path, data) => new Promise((resolve, reject) =>{
    fs.open(path, 'r+', function(err, fd) {
      if (err) {
        console.log(path);
        throw 'could not open file: ' + err;
      }
      fs.fstat(fd, function(err, stats) {
        if (err) throw 'error getting stat for a file: ' + err;
        //console.log(stats.size);
        // write the contents of the buffer, from position 0 to the end, to the file descriptor returned in opening our file
        //console.log('stats.size-1 = ', stats.size-1);
        if(stats.size-1 > 1) data = ', ' + data;
        fs.write(fd, data, stats.size-1, data.length, function(err) {
          if (err) throw 'error writing file: ' + err;
          fs.close(fd, function() {
            console.log('wrote the file successfully');
            resolve ('done');
          });
        });
      });


    });
  });

  this.saveTxtAsFile = (path, data) => new Promise((resolve, reject) => {
    fs.writeFile(path, data, (err, contents) => {
      console.log("...saving " + path);
      if (err) {
        console.log(err);
        reject(err);
      } else {
        resolve('file saved');
      }
    });
  })


};

function getUnixTimestampInSeconds(year, month, day, hour = 0, minute = 0, second = 0) {
  const date = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  return Math.floor(date.getTime() / 1000);
}

function roundToMinutes(date, roundPeriod) {
  roundPeriod = roundPeriod || 10;
  date = date || new Date();
  // Get the current minutes and seconds
  const minutes = date.getMinutes();

  // Calculate the rounded minutes (floored to the nearest roundPeriod)
  const roundedMinutes = Math.floor(minutes / roundPeriod) * roundPeriod;

  // Create a new date object with the rounded time
  const roundedDate = new Date(date);
  roundedDate.setMinutes(roundedMinutes, 0, 0); // Set minutes, seconds, and milliseconds

  return +roundedDate;
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


export {
  randomString,
  generateHexString,
  join,
  fileHandler,
  roundToMinutes,
  getUnixTimestampInSeconds,
  sleep
}