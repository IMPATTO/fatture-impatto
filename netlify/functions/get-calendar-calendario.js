const getCalendar = require('./get-calendar');

exports.handler = async (event) => getCalendar.handler(event);
