import fs from 'node:fs';
import path from 'node:path';

import { getLegacyCompatibilityEntries } from '../config/site-links.mjs';
import { siteRegistry } from '../sites/site-registry.mjs';

const repoRoot = process.cwd();
const issues = [];

function read(relativePath) {
  const absolutePath = path.join(repoRoot, relativePath);
  return fs.existsSync(absolutePath) ? fs.readFileSync(absolutePath, 'utf8') : '';
}

const helperRelativePath = 'js/legacy-host-redirect.js';
const helperSource = read(helperRelativePath);
const portaleFunctions = new Set(siteRegistry.portale?.functions || []);

if (!helperSource) {
  issues.push(`missing generated helper: ${helperRelativePath}`);
}

if (!portaleFunctions.has('log-legacy-redirect')) {
  issues.push('portale does not include log-legacy-redirect in its functions');
}

for (const entry of getLegacyCompatibilityEntries()) {
  const normalizedRoute = String(entry.route || '').replace(/^\/+/, '');
  const pageRelativePath = normalizedRoute || 'index.html';
  const pageSource = read(pageRelativePath);

  if (!pageSource) {
    issues.push(`missing legacy page source: ${pageRelativePath}`);
    continue;
  }

  if (entry.strategy === 'host-redirect') {
    const assets = new Set(siteRegistry[entry.siteKey]?.assets || []);
    if (!assets.has(helperRelativePath)) {
      issues.push(`${entry.siteKey} does not include ${helperRelativePath} in its assets`);
    }

    if (!pageSource.includes('legacy-host-redirect.js')) {
      issues.push(`${pageRelativePath} does not include the legacy host redirect helper`);
    }

    if (helperSource) {
      const expectedRoute = normalizedRoute ? `/${normalizedRoute}` : '/';
      if (!helperSource.includes(expectedRoute)) {
        issues.push(`generated helper does not contain expected legacy route ${expectedRoute}`);
      }
      if (!helperSource.includes(entry.canonicalUrl)) {
        issues.push(`generated helper does not contain canonical target ${entry.canonicalUrl}`);
      }
      if (!helperSource.includes('.netlify/functions/log-legacy-redirect')) {
        issues.push('generated helper does not include the legacy redirect logging endpoint');
      }
    }
  }

  if (entry.strategy === 'tombstone') {
    const relativeCanonicalHref = `/${String(entry.canonicalRoute || '').replace(/^\/+/, '')}`;
    if (!pageSource.includes('noindex, nofollow')) {
      issues.push(`${pageRelativePath} is missing the noindex robots directive`);
    }
    if (!pageSource.includes(entry.canonicalUrl) && !pageSource.includes(relativeCanonicalHref)) {
      issues.push(`${pageRelativePath} is missing canonical target ${entry.canonicalUrl}`);
    }
  }
}

if (issues.length) {
  console.error('Legacy compatibility check failed.\n');
  for (const issue of issues) {
    console.error(`- ${issue}`);
  }
  process.exit(1);
}

console.log('Legacy compatibility routes and tombstones are aligned.');
