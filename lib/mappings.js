/*
 * Created by G on 11/07/2016.
 */

module.exports = (function() {
  "use strict";

  const output = require("nqm-databot-utils").output;

  const _getData = function (tdxApi, config, cb) {

    output.debug("fetching mappingId data for %s", config.mappingId);

    const filter = { parent_type: config.parent_type, child_type: config.child_type };
    filter[config.fId] = { "$in": [].concat(config.dataId) };

    tdxApi.getDistinct(config.mappingId, config.vId, filter, null, { "limit": 0 }, function (err, response) {
      if (err) {
        output.abort("Failed to get mappingId data - %s", err.message);
      } else {
        output.debug("mappingId data: %d", response.data.length);
        cb(response.data);
      }
    });
  };

  const parentToChildId = function (tdxApi, config, cb) {
    config.fId = "parent_id";
    config.vId = "child_id";
    _getData(tdxApi, config, cb);
  };

  const childToParentId = function (tdxApi, config, cb) {
    config.fId = "child_id";
    config.vId = "parent_id";
    _getData(tdxApi, config, cb);
  };

  return {
    parentToChildId: parentToChildId,
    childToParentId: childToParentId
  };
}());

