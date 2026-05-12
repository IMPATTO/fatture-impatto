const createFatturaFic = require('./create-fattura-fic');

exports.handler = async (event) => createFatturaFic.handler(event);
