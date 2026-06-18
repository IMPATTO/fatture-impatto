import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  canonicalSiteUrls,
  getDeprecatedProjectLinkUrls,
  getDeprecatedProjectLinksSections,
  getProjectLinkUrls,
  getProjectLinksSections,
  legacySitePages,
  legacySiteUrls,
} from '../config/site-links.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');

function writeFile(relativePath, content) {
  const targetPath = path.join(repoRoot, relativePath);
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });
  fs.writeFileSync(targetPath, content, 'utf8');
}

function buildProjectLinksMarkdown() {
  const lines = [
    '# Project Links',
    '',
    `Ultimo aggiornamento: ${new Date().toISOString().slice(0, 10)}`,
    '',
  ];

  for (const section of getProjectLinksSections()) {
    lines.push(`## ${section.label}`);
    lines.push('');
    for (const url of section.urls) {
      lines.push(`- ${url}`);
    }
    lines.push('');
  }

  const deprecatedSections = getDeprecatedProjectLinksSections();
  if (deprecatedSections.length) {
    lines.push('## Deprecated');
    lines.push('');
    for (const section of deprecatedSections) {
      lines.push(`### ${section.label}`);
      lines.push('');
      for (const url of section.urls) {
        lines.push(`- ${url}`);
      }
      lines.push('');
    }
  }

  return `${lines.join('\n').trimEnd()}\n`;
}

function buildBrowserHelper() {
  const payload = {
    canonicalSiteUrls,
    legacySiteUrls,
    legacySitePages,
  };

  return `;(function () {
  const payload = ${JSON.stringify(payload, null, 2)};

  function getCanonicalSiteUrl(siteKey) {
    return payload.canonicalSiteUrls[siteKey] || payload.canonicalSiteUrls.portale || '';
  }

  function getLegacySiteUrl(siteKey) {
    return payload.legacySiteUrls[siteKey] || '';
  }

  function getHostname(url) {
    try {
      return new URL(String(url || '')).hostname.toLowerCase();
    } catch (_error) {
      return '';
    }
  }

  function getCanonicalSiteHost(siteKey) {
    return getHostname(getCanonicalSiteUrl(siteKey));
  }

  function getLegacySiteHost(siteKey) {
    return getHostname(getLegacySiteUrl(siteKey));
  }

  function buildCheckinUrl(publicCheckinKey, siteKey = 'portale') {
    if (!publicCheckinKey) return '';
    const params = new URLSearchParams({
      apt: String(publicCheckinKey),
      mode: 'checkin',
    });
    return \`\${getCanonicalSiteUrl(siteKey)}/?\${params.toString()}\`;
  }

  function buildOpenPortalUrl(publicCheckinKey, siteKey = 'portale') {
    if (!publicCheckinKey) return '';
    const params = new URLSearchParams({
      apt: String(publicCheckinKey),
      mode: 'open',
    });
    return \`\${getCanonicalSiteUrl(siteKey)}/portale.html?\${params.toString()}\`;
  }

  function buildPortalTokenUrl(token, publicCheckinKey = '', siteKey = 'portale') {
    if (!token) return '';
    const params = new URLSearchParams({ token: String(token) });
    if (publicCheckinKey) params.set('apt', String(publicCheckinKey));
    return \`\${getCanonicalSiteUrl(siteKey)}/portale.html?\${params.toString()}\`;
  }

  function buildSitePageUrl(siteKey, route = '') {
    const normalizedRoute = String(route || '').replace(/^\\/+/, '');
    return \`\${getCanonicalSiteUrl(siteKey)}/\${normalizedRoute}\`;
  }

  function applySiteLinks(root = document) {
    const scope = root && typeof root.querySelectorAll === 'function' ? root : document;
    scope.querySelectorAll('[data-site-link]').forEach((node) => {
      const token = String(node.getAttribute('data-site-link') || '').trim();
      if (!token) return;
      const separatorIndex = token.indexOf(':');
      if (separatorIndex <= 0) return;
      const siteKey = token.slice(0, separatorIndex);
      const route = token.slice(separatorIndex + 1);
      if (!siteKey || !route) return;
      node.setAttribute('href', buildSitePageUrl(siteKey, route));
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => applySiteLinks(), { once: true });
  } else {
    applySiteLinks();
  }

  window.siteLinks = {
    canonicalSiteUrls: payload.canonicalSiteUrls,
    legacySiteUrls: payload.legacySiteUrls,
    applySiteLinks,
    getCanonicalSiteUrl,
    getCanonicalSiteHost,
    getLegacySiteUrl,
    getLegacySiteHost,
    buildCheckinUrl,
    buildOpenPortalUrl,
    buildPortalTokenUrl,
    buildSitePageUrl,
  };
})();\n`;
}

writeFile('config/project-links.json', `${JSON.stringify(getProjectLinkUrls(), null, 2)}\n`);
writeFile('config/project-links-deprecated.json', `${JSON.stringify(getDeprecatedProjectLinkUrls(), null, 2)}\n`);
writeFile('docs/project-links.md', buildProjectLinksMarkdown());
writeFile('js/site-links.js', buildBrowserHelper());

console.log('Synced project links into config/project-links.json, config/project-links-deprecated.json, docs/project-links.md and js/site-links.js');
