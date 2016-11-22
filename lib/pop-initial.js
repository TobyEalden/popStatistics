/*
 * Created by G on 16/11/2016.
 */


module.exports = (function() {
	"use strict";
	
	const _ = require("lodash");
	
	const dataInitial = function (basedData, statisticData, currentYear) {
		_.forEach(basedData, function (objData) {
			let obj = _.cloneDeep(objData);
			
			obj.year = currentYear + '';
			obj.personBased = objData.persons;
			
			delete obj.persons;
			
			statisticData.push(obj);
		});
	};
	
	return dataInitial;
}());
