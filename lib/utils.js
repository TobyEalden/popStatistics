module.exports = (function() {
  "use strict";

  const _ = require("lodash");
  const Promise = require("bluebird");
  const assert = require("assert");
  const fs = require("fs");
  const path = require("path");

  const _constants = {
    male: "male",
    female: "female",
    firstAgeBand: "0",
    penultimateAgeBand: "89",
    ultimateAgeBand: "90+",
    allAgesBand: "All Ages"
  };

  // TODO - shouldn't this be parameterised/configurable?
  _constants.ageBands = ["0-4", 
                        "5-9", 
                        "10-14", 
                        "15-19", 
                        "20-24", 
                        "25-29", 
                        "30-34", 
                        "35-39", 
                        "40-44", 
                        "45-49", 
                        "50-54", 
                        "55-59", 
                        "60-64", 
                        "65-69", 
                        "70-74", 
                        "75-79", 
                        "80-84", 
                        "85-89", 
                        _constants.ultimateAgeBand 
                      ];  

  //
  // For debug only.
  //
  const _dump = function(name, arr) {
    const dumpFile = path.resolve("./" + name + ".json");
    _.forEach(arr, (item) => {
      fs.appendFileSync(dumpFile, JSON.stringify(item) + "\n");
    });
  };

  const _writeNewLineDelimited = function(outPath, data, append) {
    const streamOptions = {
      defaultEncoding: "utf8"
    };
    return new Promise((resolve, reject) => {
      if (!data || data.length === 0) {
        resolve();
      } else {
        streamOptions.flags = append ? "a" : "w";
        const stream = fs.createWriteStream(outPath, streamOptions);
        _.forEach(data, (obj) => {
          stream.write(JSON.stringify(obj) + "\n");
        });
        stream.on("finish", resolve);
        stream.on("error", reject);
        stream.end();
      }
    });
  };

  const _cloneData = function(inObj) {
    return _.pick(inObj, "area_id", "area_name", "gender", "age_band", "persons", "year");
  };

  const _createData = function(areaId, areaName, gender, age_band, persons, year) {
    let clone;
    if (typeof areaId === "object") {
      clone = _cloneData(areaId);
    } else {
      clone = {
        area_id: areaId,
        areaName: areaName,
        gender: gender,
        age_band: age_band,
        persons: persons,
        year: year
      };
    }
    return clone;
  };

  const _incrementString = function(str) {
    return (parseInt(str) + 1).toString();
  };

  const _decrementString = function(str) {
    return (parseInt(str) - 1).toString();
  };

  const _convertRates = function(rates) {
    return _.map(rates, (rate) => {
      const rateComponents = rate.age_band.split("-");       
      return _.extend({}, rate, {
        __lowerAge: parseInt(rateComponents[0]),
        __upperAge: parseInt(rateComponents[1] || 999999),
      });
    });
  };

  const _filterByAgeBand = function(item) {
    const itemAge = parseInt(item.age_band);
    return itemAge >= this.__lowerAge && itemAge <= this.__upperAge && (!this.gender || this.gender === item.gender); 
  };

  return {
    incrementString: _incrementString,
    decrementString: _decrementString,
    convertRates: _convertRates,
    filterByAgeBand: _filterByAgeBand,
    constants: _constants,
    dump: _dump,
    writeNewLineDelimited: _writeNewLineDelimited,
    createData: _createData
  };
}());