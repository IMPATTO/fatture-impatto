import fs from 'node:fs';
import path from 'node:path';

import {
  getVillaMargheritaUrls,
  VILLA_MARGHERITA_BASE_URL,
  VILLA_MARGHERITA_PAGE_PATHS,
} from '../config/villa-margherita-site.mjs';

const repoRoot = process.cwd();
const siteRoot = path.join(repoRoot, 'villa-margherita');
const issues = [];

function fileForPath(pagePath) {
  if (pagePath === '/') return path.join(siteRoot, 'index.html');
  return path.join(siteRoot, pagePath.replace(/^\/+/, ''));
}

function readUtf8(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

for (const pagePath of VILLA_MARGHERITA_PAGE_PATHS) {
  const filePath = fileForPath(pagePath);
  if (!fs.existsSync(filePath)) {
    issues.push(`missing file for page path ${pagePath}: ${path.relative(repoRoot, filePath)}`);
    continue;
  }

  const html = readUtf8(filePath);
  const expectedUrl = new URL(pagePath, `${VILLA_MARGHERITA_BASE_URL}/`).toString();
  const canonicalPattern = new RegExp(`<link\\s+rel=["']canonical["']\\s+href=["']${escapeRegExp(expectedUrl)}["']`, 'i');
  const ogUrlPattern = new RegExp(`<meta\\s+property=["']og:url["']\\s+content=["']${escapeRegExp(expectedUrl)}["']`, 'i');

  if (!canonicalPattern.test(html)) {
    issues.push(`canonical mismatch in ${path.relative(repoRoot, filePath)} (expected ${expectedUrl})`);
  }
  if (!ogUrlPattern.test(html)) {
    issues.push(`og:url mismatch in ${path.relative(repoRoot, filePath)} (expected ${expectedUrl})`);
  }
}

const sitemapPath = path.join(siteRoot, 'sitemap.xml');
if (!fs.existsSync(sitemapPath)) {
  issues.push('missing villa-margherita/sitemap.xml');
} else {
  const sitemap = readUtf8(sitemapPath);
  for (const expectedUrl of getVillaMargheritaUrls()) {
    if (!sitemap.includes(`<loc>${expectedUrl}</loc>`)) {
      issues.push(`sitemap missing URL ${expectedUrl}`);
    }
  }
}

const robotsPath = path.join(siteRoot, 'robots.txt');
if (!fs.existsSync(robotsPath)) {
  issues.push('missing villa-margherita/robots.txt');
} else {
  const robots = readUtf8(robotsPath);
  const expectedSitemapLine = `Sitemap: ${VILLA_MARGHERITA_BASE_URL}/sitemap.xml`;
  if (!robots.includes(expectedSitemapLine)) {
    issues.push(`robots.txt missing sitemap line "${expectedSitemapLine}"`);
  }
}

if (issues.length) {
  console.error('Villa Margherita site check failed.\n');
  for (const issue of issues) {
    console.error(`- ${issue}`);
  }
  process.exit(1);
}

console.log('Villa Margherita site metadata is aligned.');
