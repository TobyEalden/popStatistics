const Promise = require("bluebird");
const path = require("path");
const assert = require("assert");
const _ = require("lodash");
const mappings = require("./lib/mappings");
const utils = require("./lib/utils");
const chunkInitialise = require("./lib/chunk-initialise");
const appendFile = true;
let _currentTask = 0;
const _totalTasks = 11;

const popStatistic = require("./lib/pop-statistic");

const updateProgress = (input, output) => {
	// Multiple yearly task count by number of years.
	const allTasks = (_totalTasks+1)*input.numberOfYears;
	_currentTask++;
	output.progress(Math.min(100,(_currentTask/allTasks)*100));
};

const nextYearStatistic = (input, output, context, areaId, year, bData) => {
	if (!bData || bData.length === 0) {
		output.abort("received no documents for year previous to %s [%s]", year, areaId);
	} else {
		output.debug("received %d documents, starting statistics", bData.length);
		
		const basedData = bData;
		const statisticData = [];
		
		// initial the statistic data
		updateProgress(input, output);
		popStatistic.popInitial(basedData, statisticData, year);

		updateProgress(input, output);
		return popStatistic.popRError(context.tdxApi, input, statisticData, year)
			.then((statsData) => {
				updateProgress(input, output);

				// Return all data for file output.
				return [statsData, basedData];
			});
	}
};

const yearStatistic = (input, output, context, areaId, year) => {
	const parentToChildConfig = { mappingId: input.mappingResourceId,
		parent_type: "LAD15CD",
		child_type: "LSOA11CD",
		dataId: areaId
	};
	
	return new Promise((resolve) => {
			mappings.parentToChildId(context.tdxApi, parentToChildConfig, (mappedAreaIds) => {
			output.debug("got %d mapping ids for %s", mappedAreaIds.length, areaId);
			input.mappedAreaIds = mappedAreaIds;
	
			const filter = { area_id: { $in: mappedAreaIds }, year: year};
	
			output.debug("fetching ONS data from %s for %s in %s", input.basedDataId, areaId, year);
			context.tdxApi.getDatasetData(input.basedDataId, filter, { _id: 0 }, { "limit": 0 }, function (err, response) {
				resolve(nextYearStatistic(input, output, context, areaId, input.startYear, response.data));
			});
		});
	});
};

function databot(input, output, context) {
	
	if (!input.basedYear || !input.startYear || !input.numberOfYears || !input.mappingResourceId || !input.basedDataId || !input.prjDataId || !input.normYearDataId) {
		output.abort("invalid arguments - please supply basedYear, startYear, numberOfYears, mappingResourceId, basedDataId, prjDataId, normYearDataId");
	}
	
	const chunkNumber = (input.__controller ? input.__controller.chunkNumber : context.chunkNumber) || 0;
	
	let areaIdPromise;
	if (input.districtResourceId) {
		// Running in distributed mode - find the areaId by using the chunkNumber to index into the district list.
		areaIdPromise = chunkInitialise(context.tdxApi, chunkNumber, input.districtResourceId);
	} else {
		// Expect an areaId input.
		if (!input.areaId) {
			output.abort("invalid arguments - please specify an areaId or districtResourceId");
		}
		areaIdPromise = Promise.resolve(input.areaId);
	}
	
	areaIdPromise.then((areaId) => {
		const startYear = parseInt(input.startYear);
		const jsonOutputPath = output.getFileStorePath(_.kebabCase(context.instanceName + " pop stats " + chunkNumber) + ".json");
		
	const doNextStatistic = (currentYearData, basedData) => {
		const yearToStatistic = startYear + currentYearData;

		let statisticPromise;
		if (!basedData) {
			statisticPromise = yearStatistic(input, output, context, areaId, input.basedYear);
		} else {
			statisticPromise = nextYearStatistic(input, output, context, areaId, yearToStatistic, basedData);
		}
		
		let statisticData, midYearData;
		return statisticPromise
			.spread((statsData, bData) => {
				statisticData = statsData;
				midYearData = bData;
		
				// Write statistics data.
				updateProgress(input, output);
		
				return utils.writeNewLineDelimited(jsonOutputPath, statisticData, appendFile);
			})
			.then(() => {
					output.debug("statistic data write finished");
		
				statisticsData = [];
				
				if (currentYearData + 1 < input.numberOfYears) {
					// Execute on next tick to avoid stack recursion.
					return new Promise((resolve) => {
							process.nextTick(() => {
								resolve(doNextStatistic(currentYearData+1, midYearData));
							});
					});
				} else {
					areaIdObjs[_index].prj_status = 'OK';
					
					output.result({ projectionOutput: jsonOutputPath});
				}
			})
			.catch(err => {
					output.abort("statistic data failure: " + err.message);
			});
	};
	
	doNextStatistic(0);
});
}

var input = require("nqm-databot-utils").input;
input.pipe(databot);
