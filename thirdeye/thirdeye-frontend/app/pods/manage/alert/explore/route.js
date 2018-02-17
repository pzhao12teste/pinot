/**
 * Handles the 'explore' route for manage alert
 * @module manage/alert/edit/explore
 * @exports manage/alert/edit/explore
 */
import RSVP from "rsvp";
import fetch from 'fetch';
import moment from 'moment';
import Route from '@ember/routing/route';
import { checkStatus, postProps, parseProps, buildDateEod, toIso } from 'thirdeye-frontend/utils/utils';
import { isPresent, isEmpty, isNone, isBlank } from "@ember/utils";
import { enhanceAnomalies, toIdGroups, setUpTimeRangeOptions, getTopDimensions } from 'thirdeye-frontend/utils/manage-alert-utils';

/**
 * Shorthand for setting date defaults
 */
const dateFormat = 'YYYY-MM-DD';

/**
 * Basic alert page defaults
 */
const defaultSeverity = 30;
const paginationDefault = 10;
const durationDefault = '1m';
const metricDataColor = 'blue';
const durationMap = { m:'month', d:'day', w:'week' };
const startDateDefault = buildDateEod(1, 'month');
const endDateDefault = buildDateEod(1, 'day');

/**
 * Response type options for anomalies
 */
const anomalyResponseObj = [
  { name: 'Not reviewed yet',
    value: 'NO_FEEDBACK',
    status: 'Not Resolved'
  },
  { name: 'True anomaly',
    value: 'ANOMALY',
    status: 'Confirmed Anomaly'
  },
  { name: 'False alarm',
    value: 'NOT_ANOMALY',
    status: 'False Alarm'
  },
  { name: 'Confirmed - New Trend',
    value: 'ANOMALY_NEW_TREND',
    status: 'New Trend'
  }
];

/**
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {RSVP promise}
 */
const fetchCombinedAnomalies = (anomalyIds) => {
  if (anomalyIds.length) {
    const idGroups = toIdGroups(anomalyIds);
    const anomalyPromiseHash = idGroups.map((group, index) => {
      let idStringParams = `anomalyIds=${encodeURIComponent(idGroups[index].toString())}`;
      let getAnomalies = fetch(`/anomalies/search/anomalyIds/0/0/${index + 1}?${idStringParams}`).then(checkStatus);
      return RSVP.resolve(getAnomalies);
    });
    return RSVP.all(anomalyPromiseHash);
  } else {
    return RSVP.resolve([]);
  }
};

/**
 * Derives start/end timestamps based on queryparams and user-selected time range with certain fall-backs/defaults
 * @param {String} bucketUnit - is requested range from an hourly or minutely metric?
 * @param {String} duration - the model's processed query parameter for duration ('1m', '2w', etc)
 * @param {String} start - the model's processed query parameter for startDate
 * @param {String} end - the model's processed query parameter for endDate
 * @returns {Object}
 */
const processRangeParams = (bucketUnit, duration, start, end) => {
  // To avoid loading too much data, override our time span defaults based on whether the metric is 'minutely'
  const isMetricMinutely = bucketUnit.toLowerCase().includes('minute');
  const defaultQueryUnit = isMetricMinutely ? 'week' : 'month';
  const defaultQuerySize = isMetricMinutely ? 2 : 1;

  // We also allow a 'duration' query param to set the time range. For example, duration=15d (last 15 days)
  const qsRegexMatch = duration.match(new RegExp(/^(\d)+([d|m|w])$/i));
  const durationMatch = duration && qsRegexMatch ? qsRegexMatch : [];

  // If the duration string is recognized, we use it. Otherwise, we fall back on the defaults above
  const querySize = durationMatch && durationMatch.length ? durationMatch[1] : defaultQuerySize;
  const queryUnit = durationMatch && durationMatch.length ? durationMap[durationMatch[2].toLowerCase()] : defaultQueryUnit;

  // If duration = 'custom', we know the user is requesting specific start/end times.
  // In this case, we will use those instead of our parsed duration & defaults
  const isCustomDate = duration === 'custom';
  const baseStart = isCustomDate ? moment(parseInt(start, 10)) : buildDateEod(querySize, queryUnit);
  const baseEnd = isCustomDate ? moment(parseInt(end, 10)) : endDateDefault;

  // These resulting timestamps are used for our graph and anomaly queries
  const startStamp = baseStart.valueOf();
  const endStamp = baseEnd.valueOf();

  return { startStamp, endStamp, baseStart, baseEnd };
};

/**
 * Builds the graph metric URL from config settings
 * TODO: Pull this into utils if used by create-alert
 * @param {Object} cfg - settings for current metric graph
 * @param {String} maxTime - an 'bookend' for this metric's graphable data
 * @return {String} URL for graph metric API
 */
const buildGraphConfig = (config, maxTime) => {
  const dimension = config.exploreDimensions ? config.exploreDimensions.split(',')[0] : 'All';
  const currentEnd = moment(maxTime).isValid()
    ? moment(maxTime).valueOf()
    : buildDateEod(1, 'day').valueOf();
  const formattedFilters = JSON.stringify(parseProps(config.filters));
  // Load less data if granularity is 'minutes'
  const isMinutely = config.bucketUnit.toLowerCase().includes('minute');
  const duration = isMinutely ? { unit: 2, size: 'week' } : { unit: 1, size: 'month' };
  const currentStart = moment(currentEnd).subtract(duration.unit, duration.size).valueOf();
  const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
  const baselineEnd = moment(currentEnd).subtract(1, 'week');
  // Prepare call for metric graph data
  const metricDataUrl =  `/timeseries/compare/${config.id}/${currentStart}/${currentEnd}/` +
    `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=` +
    `${config.bucketSize + '_' + config.bucketUnit}&filters=${encodeURIComponent(formattedFilters)}`;
  // Prepare call for dimension graph data
  const topDimensionsUrl = `/rootcause/query?framework=relatedDimensions&anomalyStart=${currentStart}` +
    `&anomalyEnd=${currentEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}` +
    `&analysisStart=${currentStart}&analysisEnd=${currentEnd}&urns=thirdeye:metric:${config.id}` +
    `&filters=${encodeURIComponent(formattedFilters)}`;

  return { metricDataUrl, topDimensionsUrl };
};

/**
 * Setup for query param behavior
 */
const queryParamsConfig = {
  refreshModel: true,
  replace: true
};

export default Route.extend({
  queryParams: {
    duration: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig
  },

  beforeModel(transition) {
    const { duration, startDate, jobId } = transition.queryParams;

    // Default to 1 month of anomalies to show if no dates present in query params
    if (!duration || !startDate) {
      this.transitionTo({ queryParams: {
        duration: durationDefault,
        startDate: startDateDefault,
        endDate: endDateDefault,
        jobId
      }});
    }
  },

  model(params, transition) {
    const { id, alertData, jobId, functionName } = this.modelFor('manage.alert');
    if (!id) { return; }

    const {
      duration = durationDefault,
      startDate = startDateDefault,
      endDate = endDateDefault
    } = transition.queryParams;

    // Prepare endpoints for eval, mttd, projected metrics calls
    const dateParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const evalUrl = `/detection-job/eval/filter/${id}?${dateParams}`;
    const mttdUrl = `/detection-job/eval/mttd/${id}?severity=${defaultSeverity/100}`;
    const performancePromiseHash = {
      current: fetch(`${evalUrl}&isProjected=FALSE`).then(checkStatus),
      projected: fetch(`${evalUrl}&isProjected=TRUE`).then(checkStatus),
      mttd: fetch(mttdUrl).then(checkStatus)
    };

    return RSVP.hash(performancePromiseHash)
      .then((alertEvalMetrics) => {
        Object.assign(alertEvalMetrics.current, { mttd: alertEvalMetrics.mttd});
        return {
          id,
          jobId,
          functionName,
          alertData,
          duration,
          startDate,
          endDate,
          dateParams,
          alertEvalMetrics
        };
      })
      // Catch is not mandatory here due to our error action, but left it to add more context.
      .catch((error) => {
        return RSVP.reject({ error, location: `${this.routeName}:model`, calls: performancePromiseHash });
      });
  },

  afterModel(model) {
    this._super(model);

    const {
      id: alertId,
      alertData,
      jobId,
      startDate,
      endDate,
      duration,
      alertEvalMetrics
    } = model;

    // Pull alert properties into context
    const {
      metric: metricName,
      collection: dataset,
      exploreDimensions,
      filters,
      bucketSize,
      bucketUnit
    } = alertData;

    // Derive start/end time ranges based on querystring input with fallback on default '1 month'
    const {
      startStamp,
      endStamp,
      baseStart,
      baseEnd
    } = processRangeParams(bucketUnit, duration, startDate, endDate);

    // Set initial value for metricId for early transition cases
    const config = {
      filters,
      startStamp,
      endStamp,
      bucketSize,
      bucketUnit,
      baseEnd,
      baseStart,
      exploreDimensions
    };

    // Load endpoints for projected metrics. TODO: consolidate into CP if duplicating this logic
    const qsParams = `start=${baseStart.utc().format(dateFormat)}&end=${baseEnd.utc().format(dateFormat)}&useNotified=true`;
    const dateParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const anomalyDataUrl = `/anomalies/search/anomalyIds/${startStamp}/${endStamp}/1?anomalyIds=`;
    const metricsUrl = `/data/autocomplete/metric?name=${dataset}::${metricName}`;
    const anomaliesUrl = `/dashboard/anomaly-function/${alertId}/anomalies?${qsParams}`;

    const anomalyPromiseHash = {
      projectedMttd: 0, // In overview mode, no projected MTTD value is needed
      metricsByName: fetch(metricsUrl).then(checkStatus),
      anomalyIds: fetch(anomaliesUrl).then(checkStatus)
    };

    return RSVP.hash(anomalyPromiseHash)
      .then((data) => {
        const totalAnomalies = data.anomalyIds.length;
        Object.assign(model.alertEvalMetrics.projected, { mttd: data.projectedMttd });
        Object.assign(config, { id: data.metricsByName.length ? data.metricsByName.pop().id : '' });
        Object.assign(model, {
          anomalyIds: data.anomalyIds,
          exploreDimensions,
          totalAnomalies,
          anomalyDataUrl,
          config
        });
        fetch(`/data/maxDataTime/metricId/${config.id}`).then(checkStatus);
      })
      // Note: In the event of custom date selection, the end date might be less than maxTime
      .then((maxTime) => {
        const { metricDataUrl, topDimensionsUrl } = buildGraphConfig(config, maxTime);
        Object.assign(model, { metricDataUrl, topDimensionsUrl });
      })
      // Catch is not mandatory here due to our error action, but left it to add more context
      .catch((err) => {
        return RSVP.reject({ err, location: `${this.routeName}:afterModel`, calls: anomalyPromiseHash });
      });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      id,
      jobId,
      functionName,
      alertData,
      anomalyIds,
      email,
      filters,
      duration,
      config,
      loadError,
      metricDataUrl,
      anomalyDataUrl,
      topDimensionsUrl,
      exploreDimensions,
      totalAnomalies,
      alertEvalMetrics,
      allConfigGroups,
      allAppNames,
      rawAnomalyData
    } = model;

    // Initial value setup for displayed option lists
    let subD = {};
    let anomalyData = [];
    const notCreateError = jobId !== -1;
    const resolutionOptions = ['All Resolutions'];
    const dimensionOptions = ['All Dimensions'];
    const wowOptions = ['Wow', 'Wo2W', 'Wo3W', 'Wo4W'];
    const baselineOptions = [{ name: 'Predicted', isActive: true }];
    const responseOptions = anomalyResponseObj.map(response => response.name);
    const timeRangeOptions = setUpTimeRangeOptions(['1m', '2w', '1w'], duration);
    const alertDimension = exploreDimensions ? exploreDimensions.split(',')[0] : '';
    const newWowList = wowOptions.map((item) => {
      return { name: item, isActive: false };
    });

    // Prime the controller
    controller.setProperties({
      loadError,
      jobId,
      functionName,
      alertId: id,
      defaultSeverity,
      isMetricDataInvalid: false,
      anomalyDataUrl,
      baselineOptions,
      responseOptions,
      timeRangeOptions,
      alertData,
      anomalyResponseObj,
      anomaliesLoaded: false,
      alertEvalMetrics,
      activeRangeStart: config.startStamp,
      activeRangeEnd: config.endStamp,
      isMetricDataLoading: true,
      isReplayPending: isPresent(model.jobId) && model.jobId !== -1
    });

    // Kick off controller defaults and replay status check
    controller.initialize();

    // Fetch all anomalies we have Ids for. Enhance the data and populate power-select filter options.
    if (notCreateError) {
      fetchCombinedAnomalies(anomalyIds)
        .then((rawAnomalyData) => {
          anomalyData = enhanceAnomalies(rawAnomalyData);
          resolutionOptions.push(...new Set(anomalyData.map(record => record.anomalyFeedback)));
          dimensionOptions.push(...new Set(anomalyData.map(anomaly => anomaly.dimensionString)));
          controller.setProperties({
            anomaliesLoaded: true,
            anomalyData,
            resolutionOptions,
            dimensionOptions
          });
          return fetch(metricDataUrl).then(checkStatus);
        })
        // Fetch and load graph metric data
        .then((metricData) => {
          subD = metricData.subDimensionContributionMap;
          Object.assign(metricData, { color: metricDataColor });
          controller.setProperties({
            alertDimension,
            topDimensions: [],
            metricData,
            isMetricDataLoading: exploreDimensions ? true : false
          });
          return this.fetchCombinedAnomalyChangeData(anomalyData);
        })
        // Load and display rest of options once data is loaded ('2week', 'Last Week')
        .then((wowData) => {
          anomalyData.forEach((anomaly) => {
            anomaly.wowData = wowData[anomaly.anomalyId] || {};
          });
          controller.setProperties({
            anomalyData,
            baselineOptions: [baselineOptions[0], ...newWowList]
          });
          // If alert has dimensions set, load them into graph
          if (exploreDimensions) {
            return fetch(topDimensionsUrl).then(checkStatus).then((allDimensions) => {
              const newtopDimensions = getTopDimensions(subD, allDimensions, alertDimension);
              controller.setProperties({
                topDimensions: newtopDimensions,
                isMetricDataLoading: false
              });
            });
          }
        })
        .catch((errors) => {
          controller.setProperties({
            isMetricDataInvalid: true,
            isMetricDataLoading: false,
            graphMessageText: 'Error loading metric data'
          });
        });
    }
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
  },

  /**
   * Fetches change rate data for each available anomaly id
   * @method fetchCombinedAnomalyChangeData
   * @returns {RSVP promise}
   */
  fetchCombinedAnomalyChangeData(anomalyData) {
    let promises = {};

    anomalyData.forEach((anomaly) => {
      let id = anomaly.anomalyId;
      promises[id] = fetch(`/anomalies/${id}`).then(checkStatus);
    });

    return RSVP.hash(promises);
  },

  actions: {
    /**
    * Refresh route's model.
    */
    refreshModel() {
      this.refresh();
    },

    /**
     * Handle any errors occurring in model/afterModel in parent route
     * https://www.emberjs.com/api/ember/2.16/classes/Route/events/error?anchor=error
     * https://guides.emberjs.com/v2.18.0/routing/loading-and-error-substates/#toc_the-code-error-code-event
     */
    error(error, transition) {
      return true;
    }
  }
});
