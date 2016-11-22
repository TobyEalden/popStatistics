module.exports = (function() {
  "use strict";

  const Promise = require("bluebird");

  const chunkInitialise = function(tdxApi, chunkNumber, districtDatasetId) {
    // Finds the district id that this databot instance should run.
    // This is done by using the chunkNumber assigned to the databot to 
    // index into the list of unique district ids.
    //
    // Get the list of unique districts.
    return tdxApi.getDistinctAsync(districtDatasetId, "area_id")
      .then((districts) => {
        if (chunkNumber > districts.data.length) {
          return Promise.reject(new Error("chunkNumber out of range, district count is: " + districts.data.length));
        }
        return districts.data[chunkNumber];
      })
      .catch(err => {
        return Promise.reject(new Error("chunkInitialise - failure getting district data: " + err.message));
      });
  };

  return chunkInitialise;
}());