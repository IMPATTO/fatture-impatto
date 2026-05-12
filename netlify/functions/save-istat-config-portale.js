const { handleSaveIstatConfig } = require('./_lib/istat-config');

exports.handler = async (event) => handleSaveIstatConfig(event);
