/*
 * Created by G on 16/11/2016.
 */


module.exports = (function() {
	"use strict";
	
	const _ = require("lodash");
	const assert = require("assert");
	const output = require("nqm-databot-utils").output;
	
	const getData = function (tdxApi, dataId, areaIds, year) {
		output.debug("fetching the data for %s from %s", areaIds.toString(), dataId);
		
		const filter = {  area_id: { $in: areaIds }, year: year};
		
		// Get tdx data
		return tdxApi.getDatasetDataAsync(dataId, filter, null, {limit:0})
				.then((response) => {
					output.debug("got normalise data: %d documents", response.data.length);
					return response.data;
				})
				.catch(err => {
					output.abort("Failed to get normalise data - %s", err.message);
				});
	};
	
	// Get the r-error
	const getRError = function (obj) {
		obj.rerror = (obj.personNormalised - obj.personBased) ? (obj.personProjected - obj.personNormalised) / (obj.personNormalised - obj.personBased) : null;
	};
	
	// Get the absolute error
	const getAbsoluteError = function (obj) {
		obj.absoluteError = obj.personProjected - obj.personNormalised;
	};
	
	const rError = function(tdxApi, input, statisticData, year) {
		return getData(tdxApi, input.prjDataId, input.mappedAreaIds, year)
				.then((prjData) => {
					_.forEach(statisticData, (obj) => {
						let dataPrj = _.filter(prjData, function (o) {
							return o.area_id == obj.area_id && o.age_band == obj.age_band && o.gender == obj.gender;
						});
						assert(dataPrj);
		
						// Get projected data
						obj.personProjected = dataPrj[0].persons;
					});
		
					return getData(tdxApi, input.normYearDataId, input.mappedAreaIds, year);
				})
				.then((normData) => {
					_.forEach(statisticData, (obj) => {
						let dataNorm = _.filter(normData, function (o) {
							return o.area_id == obj.area_id && o.age_band == obj.age_band && o.gender == obj.gender;
						});
						assert(dataNorm);

						// Get mid-year data
						obj.personNormalised = dataNorm[0].persons;
						
						getRError(obj);
						getAbsoluteError(obj);
					
					});
			
					return statisticData;
				});
	};
	
	return rError;
}());
